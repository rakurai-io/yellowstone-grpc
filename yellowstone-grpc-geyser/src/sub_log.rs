//! Detailed per-client subscription logging, built only with the `sub-log` feature.
//!
//! Every accepted or rejected `SubscribeRequest` is logged under the `sub_log` target with the
//! client's peer address and each filter in full: which accounts/owners, which data filters,
//! commitment and data slices.

use {
    std::{fmt::Write as _, net::SocketAddr},
    tonic::metadata::MetadataMap,
    yellowstone_grpc_proto::geyser::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_lamports::Cmp as AccountsFilterLamports,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        CommitmentLevel as CommitmentLevelProto, SubscribeRequest, SubscribeRequestFilterAccounts,
    },
};

const TARGET: &str = "sub_log";

/// Where the client connects from: the TCP peer, plus any proxy-provided headers.
#[derive(Debug, Clone)]
pub struct ClientPeer(String);

impl ClientPeer {
    pub fn new(remote_addr: Option<SocketAddr>, metadata: &MetadataMap) -> Self {
        let mut peer = remote_addr.map_or_else(|| "unknown".to_owned(), |addr| addr.to_string());
        for header in ["x-forwarded-for", "x-real-ip"] {
            if let Some(value) = metadata.get(header).and_then(|v| v.to_str().ok()) {
                let _ = write!(peer, " {header}={value}");
            }
        }
        Self(peer)
    }
}

pub fn log_connected(subscriber_id: &Option<String>, id: usize, peer: &ClientPeer) {
    log::info!(target: TARGET, "client #{subscriber_id:?}/{id} peer={}: connected", peer.0);
}

pub fn log_subscribe_request(
    subscriber_id: &str,
    id: usize,
    peer: &ClientPeer,
    request: &SubscribeRequest,
    rejected: Option<&str>,
) {
    let prefix = format!("client #{subscriber_id:?}/{id} peer={}", peer.0);
    let commitment = request
        .commitment
        .map(|c| {
            CommitmentLevelProto::try_from(c)
                .map(|c| c.as_str_name().to_owned())
                .unwrap_or_else(|_| format!("invalid({c})"))
        })
        .unwrap_or_else(|| "default(PROCESSED)".to_owned());
    let data_slice = request
        .accounts_data_slice
        .iter()
        .map(|s| format!("{}..{}", s.offset, s.offset.saturating_add(s.length)))
        .collect::<Vec<_>>();
    let status = match rejected {
        Some(error) => format!("REJECTED ({error})"),
        None => "accepted".to_owned(),
    };
    log::info!(
        target: TARGET,
        "{prefix}: subscribe {status}: commitment={commitment} from_slot={:?} accounts_data_slice={data_slice:?} \
         filters: accounts={} slots={} blocks_meta={} transactions={} transactions_status={} blocks={} entry={} block_footer={}",
        request.from_slot,
        request.accounts.len(),
        request.slots.len(),
        request.blocks_meta.len(),
        request.transactions.len(),
        request.transactions_status.len(),
        request.blocks.len(),
        request.entry.len(),
        request.block_footer.len(),
    );

    for (name, filter) in &request.accounts {
        log::info!(target: TARGET, "{prefix}:   accounts[{name:?}]: {}", format_accounts(filter));
    }
    for (name, filter) in &request.slots {
        log::info!(
            target: TARGET,
            "{prefix}:   slots[{name:?}]: filter_by_commitment={:?} interslot_updates={:?}",
            filter.filter_by_commitment,
            filter.interslot_updates
        );
    }
    for name in request.blocks_meta.keys() {
        log::info!(target: TARGET, "{prefix}:   blocks_meta[{name:?}]");
    }
    for (name, filter) in &request.transactions {
        log::info!(target: TARGET, "{prefix}:   transactions[{name:?}]: {filter:?}");
    }
    for (name, filter) in &request.transactions_status {
        log::info!(target: TARGET, "{prefix}:   transactions_status[{name:?}]: {filter:?}");
    }
    for (name, filter) in &request.blocks {
        log::info!(target: TARGET, "{prefix}:   blocks[{name:?}]: {filter:?}");
    }
    for name in request.entry.keys() {
        log::info!(target: TARGET, "{prefix}:   entry[{name:?}]");
    }
    for name in request.block_footer.keys() {
        log::info!(target: TARGET, "{prefix}:   block_footer[{name:?}]");
    }
}

fn format_accounts(filter: &SubscribeRequestFilterAccounts) -> String {
    let filters = filter
        .filters
        .iter()
        .map(|f| match &f.filter {
            Some(AccountsFilterDataOneof::Memcmp(memcmp)) => {
                let data = match &memcmp.data {
                    Some(AccountsFilterMemcmpOneof::Bytes(bytes)) => {
                        format!("base58={}", bs58::encode(bytes).into_string())
                    }
                    Some(AccountsFilterMemcmpOneof::Base58(value)) => format!("base58={value}"),
                    Some(AccountsFilterMemcmpOneof::Base64(value)) => format!("base64={value}"),
                    None => "none".to_owned(),
                };
                format!("memcmp(offset={}, {data})", memcmp.offset)
            }
            Some(AccountsFilterDataOneof::Datasize(size)) => format!("datasize={size}"),
            Some(AccountsFilterDataOneof::TokenAccountState(state)) => {
                format!("token_account_state={state}")
            }
            Some(AccountsFilterDataOneof::Lamports(lamports)) => match &lamports.cmp {
                Some(AccountsFilterLamports::Eq(v)) => format!("lamports=={v}"),
                Some(AccountsFilterLamports::Ne(v)) => format!("lamports!={v}"),
                Some(AccountsFilterLamports::Lt(v)) => format!("lamports<{v}"),
                Some(AccountsFilterLamports::Gt(v)) => format!("lamports>{v}"),
                None => "lamports(none)".to_owned(),
            },
            None => "none".to_owned(),
        })
        .collect::<Vec<_>>();
    let cuckoo = filter.cuckoo_accounts_filter.as_ref().map(|c| {
        format!(
            "buckets={} entries_per_bucket={} fingerprint_bits={} bytes={}",
            c.bucket_count,
            c.entries_per_bucket,
            c.fingerprint_bits,
            c.data.len()
        )
    });
    format!(
        "account({})={:?} owner({})={:?} filters={filters:?} nonempty_txn_signature={:?} cuckoo_accounts_filter={cuckoo:?}",
        filter.account.len(),
        filter.account,
        filter.owner.len(),
        filter.owner,
        filter.nonempty_txn_signature,
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        yellowstone_grpc_proto::geyser::{
            SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
            SubscribeRequestFilterAccountsFilterMemcmp,
        },
    };

    #[test]
    fn format_accounts_shows_every_account_owner_and_filter() {
        let filter = SubscribeRequestFilterAccounts {
            account: vec!["Acc1".to_owned(), "Acc2".to_owned()],
            owner: vec!["Own1".to_owned()],
            filters: vec![
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(AccountsFilterDataOneof::Memcmp(
                        SubscribeRequestFilterAccountsFilterMemcmp {
                            offset: 32,
                            data: Some(AccountsFilterMemcmpOneof::Bytes(vec![1, 2, 3])),
                        },
                    )),
                },
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(AccountsFilterDataOneof::Datasize(165)),
                },
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(AccountsFilterDataOneof::Lamports(
                        SubscribeRequestFilterAccountsFilterLamports {
                            cmp: Some(AccountsFilterLamports::Gt(10)),
                        },
                    )),
                },
            ],
            nonempty_txn_signature: None,
            cuckoo_accounts_filter: None,
        };
        assert_eq!(
            format_accounts(&filter),
            r#"account(2)=["Acc1", "Acc2"] owner(1)=["Own1"] filters=["memcmp(offset=32, base58=Ldp)", "datasize=165", "lamports>10"] nonempty_txn_signature=None cuckoo_accounts_filter=None"#
        );
    }
}
