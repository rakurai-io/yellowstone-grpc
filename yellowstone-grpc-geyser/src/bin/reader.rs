use {
    flexi_logger::{FileSpec, Logger},
    log::{error, info, warn},
    solana_pubkey::Pubkey,
    std::env,
    yellowstone_grpc_proto::{
        geyser::{subscribe_update::UpdateOneof, SubscribeUpdate},
        prost::Message as ProstMessage,
    },
    zstd::stream::decode_all,
};

fn init_logger(log_prefix: &str) {
    Logger::try_with_env_or_str("info")
        .unwrap()
        .log_to_file(
            FileSpec::default()
                .directory(".")
                .basename(log_prefix)
                .suffix("log"),
        )
        .duplicate_to_stdout(flexi_logger::Duplicate::None)
        .rotate(
            flexi_logger::Criterion::Size(5 * 1024 * 1024),
            flexi_logger::Naming::Numbers,
            flexi_logger::Cleanup::KeepLogFiles(1),
        )
        .start()
        .unwrap();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <log_prefix> <ZMQ_SOCKET>", args[0]);
        eprintln!(
            "Example: {} account PushPull_ipc:///tmp/account.sock",
            args[0]
        );
        std::process::exit(1);
    }

    let log_prefix = &args[1];
    let full_path = &args[2];

    init_logger(&format!("reader-{}", log_prefix));
    info!("Starting reader for socket: {}", full_path);

    let (prefix, zmq_path) = full_path
        .split_once('_')
        .ok_or("Invalid format. Use: PushPull_<zmq_path> or PubSub_<zmq_path>")?;

    info!("Connecting to {} socket: {}", prefix, zmq_path);

    let context = zmq::Context::new();
    let socket = match prefix {
        "PushPull" => {
            let s = context.socket(zmq::PULL)?;
            s.connect(zmq_path)?;
            s
        }
        "PubSub" => {
            let s = context.socket(zmq::SUB)?;
            s.connect(zmq_path)?;
            s.set_subscribe(b"")?;
            s
        }
        _ => return Err(format!("Unknown prefix: {}", prefix).into()),
    };

    info!("Connected to socket at {}", zmq_path);
    info!("Reading now: {}", chrono::Local::now());
    loop {
        match socket.recv_msg(0) {
            Ok(msg) => match decode_all(&msg[..]) {
                Ok(decompressed) => match SubscribeUpdate::decode(&*decompressed) {
                    Ok(update) => {
                        log_message(update);
                    }
                    Err(e) => {
                        error!("Failed to decode Protobuf SubscribeUpdate: {:?}", e)
                    }
                },
                Err(e) => error!("Failed to decompress ZSTD: {:?}", e),
            },
            Err(e) => error!("ZMQ recv error: {:?}", e),
        }
    }
}

fn log_message(msg: SubscribeUpdate) {
    match msg.update_oneof {
        Some(UpdateOneof::Account(update)) => {
            let slot = update.slot;
            match update.account {
                Some(acc) => match Pubkey::try_from(acc.pubkey) {
                    Ok(pubkey) => info!("Account update at slot {}: pubkey={}", slot, pubkey),
                    Err(e) => warn!(
                        "Failed to parse pubkey in Account update at slot {}: {:?}",
                        slot, e
                    ),
                },
                None => warn!("Account update at slot {} missing account field", slot),
            }
        }
        Some(UpdateOneof::Transaction(update)) => {
            info!("Transaction update received at slot {}", update.slot);
        }
        Some(UpdateOneof::Slot(update)) => {
            info!(
                "Slot update at slot {} (status: {:?})",
                update.slot, update.status
            );
        }
        Some(UpdateOneof::Entry(update)) => {
            info!(
                "Entry update at slot {} (index={}, hashes={}, txs={})",
                update.slot, update.index, update.num_hashes, update.executed_transaction_count
            );
        }
        Some(UpdateOneof::Block(update)) => {
            info!(
                "Block update at slot {} (rewards: {:?}, txs={}, accounts={}, entries={})",
                update.slot,
                update.rewards,
                update.transactions.len(),
                update.accounts.len(),
                update.entries_count
            );
        }
        Some(UpdateOneof::BlockMeta(update)) => {
            info!(
                "BlockMeta update at slot {} (rewards: {:?}, block_time: {:?})",
                update.slot, update.rewards, update.block_time
            );
        }
        None => warn!("Received SubscribeUpdate with no update_oneof"),
        Some(other) => warn!("Received unknown update type: {:?}", other),
    }
}
