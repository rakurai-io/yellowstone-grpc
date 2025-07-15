use {
    crate::grpc::BroadcastedMessage,
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    log::{error, info, trace, warn},
    std::{
        collections::HashMap,
        io::Cursor,
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::{sync::broadcast::Receiver, task},
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
        },
        plugin::{
            filter::{limits::FilterLimits, message::FilteredUpdate, name::FilterNames, Filter},
            message::MessageType,
        },
        prost::Message as ProstMessage,
    },
    zmq,
    zstd::encode_all,
};

pub struct ZmqSocketManager {
    sockets: HashMap<MessageType, Arc<Mutex<zmq::Socket>>>,
}

impl ZmqSocketManager {
    pub async fn start(
        message_sockets: &HashMap<String, String>,
        broadcast_rx: Receiver<BroadcastedMessage>,
    ) -> Result<(), GeyserPluginError> {
        let context = zmq::Context::new();
        let mut sockets = HashMap::new();

        for (key, full_path) in message_sockets {
            let msg_type = MessageType::from_str(key).ok_or_else(|| {
                GeyserPluginError::Custom(format!("Invalid message type: {}", key).into())
            })?;

            let (prefix, zmq_path) = full_path.split_once('_').ok_or_else(|| {
                GeyserPluginError::Custom(
                    format!("Expected format <Prefix>_<zmq_path>: {}", full_path).into(),
                )
            })?;

            if zmq_path.starts_with("ipc://") {
                if let Some(ipc_path) = zmq_path.strip_prefix("ipc://") {
                    if Path::new(ipc_path).exists() {
                        if let Err(e) = std::fs::remove_file(ipc_path) {
                            warn!("Failed to remove old IPC file {}: {:?}", ipc_path, e);
                        }
                    }
                }
            }

            let socket = match prefix {
                "PushPull" => {
                    let s = context.socket(zmq::PUSH).map_err(|e| {
                        GeyserPluginError::Custom(format!("Socket creation failed: {}", e).into())
                    })?;
                    s.bind(zmq_path).map_err(|e| {
                        GeyserPluginError::Custom(format!("Bind failed: {}", e).into())
                    })?;
                    s.set_sndhwm(100_000).unwrap();
                    s.set_rcvhwm(100_000).unwrap();
                    info!("Bound PUSH socket for {:?} to {}", msg_type, zmq_path);
                    s
                }
                "PubSub" => {
                    let s = context.socket(zmq::PUB).map_err(|e| {
                        GeyserPluginError::Custom(format!("Socket creation failed: {}", e).into())
                    })?;
                    s.bind(zmq_path).map_err(|e| {
                        GeyserPluginError::Custom(format!("Bind failed: {}", e).into())
                    })?;
                    s.set_sndhwm(100_000).unwrap();
                    s.set_rcvhwm(100_000).unwrap();
                    info!("Bound PUB socket for {:?} to {}", msg_type, zmq_path);
                    s
                }
                _ => {
                    return Err(GeyserPluginError::Custom(
                        format!("Unknown socket type prefix: {}", prefix).into(),
                    ));
                }
            };

            sockets.insert(msg_type, Arc::new(Mutex::new(socket)));
        }
        let zmq_manager = Arc::new(Mutex::new(Self { sockets }));
        task::spawn(Self::run_message_loop(zmq_manager, broadcast_rx));

        Ok(())
    }

    async fn run_message_loop(
        zmq_manager: Arc<Mutex<Self>>,
        mut broadcast_rx: Receiver<BroadcastedMessage>,
    ) {
        let filter = Self::create_all_filters();
        loop {
            match broadcast_rx.recv().await {
                Ok((_commitment, arc_messages)) => {
                    let messages = Arc::clone(&arc_messages);
                    let manager = zmq_manager.clone();
                    let filter = filter.clone();

                    task::spawn_blocking(move || {
                        if let Ok(mut mgr) = manager.lock() {
                            for (_msgid, message) in messages.iter() {
                                for update in
                                    filter.get_updates(message, Some(filter.get_commitment_level()))
                                {
                                    mgr.send_filtered_update(message, &update);
                                }
                            }
                        }
                    });
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    warn!("ZMQ manager: broadcast channel closed");
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    warn!("ZMQ manager: broadcast channel lagged by {}", count);
                    continue;
                }
            }
        }
    }

    fn create_all_filters() -> Filter {
        // let filter = Filter::default();
        let mut account_filters = HashMap::new();
        account_filters.insert(
            "account_subscribe".to_string(),
            SubscribeRequestFilterAccounts::default(),
        );
        let mut transaction_filters = HashMap::new();
        transaction_filters.insert(
            "transaction_subscribe".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        let mut slot_filters = HashMap::new();
        slot_filters.insert(
            "slot_subscribe".to_string(),
            SubscribeRequestFilterSlots::default(),
        );
        let mut entry_filters = HashMap::new();
        entry_filters.insert(
            "entry_subscribe".to_string(),
            SubscribeRequestFilterEntry::default(),
        );
        let mut blockmeta_filters = HashMap::new();
        blockmeta_filters.insert(
            "blockmeta_subscribe".to_string(),
            SubscribeRequestFilterBlocksMeta::default(),
        );
        let mut block_filters = HashMap::new();
        block_filters.insert(
            "block_subscribe".to_string(),
            SubscribeRequestFilterBlocks::default(),
        );

        let config = SubscribeRequest {
            accounts: account_filters,
            transactions: transaction_filters,
            entry: entry_filters,
            slots: slot_filters,
            blocks: block_filters,
            blocks_meta: blockmeta_filters,
            ..Default::default()
        };
        let limit = FilterLimits::default();
        let mut names = FilterNames::new(64, 1024, Duration::from_secs(1));
        Filter::new(&config, &limit, &mut names).unwrap()
    }

    pub fn send_filtered_update(
        &mut self,
        raw_msg: &yellowstone_grpc_proto::plugin::message::Message,
        update: &FilteredUpdate,
    ) {
        let msg_type = raw_msg.get_type_enum();
        let update_proto = update.as_subscribe_update();
        // Encode Protobuf
        let mut uncompressed = Vec::with_capacity(ProstMessage::encoded_len(&update_proto));
        if let Err(e) = ProstMessage::encode(&update_proto, &mut uncompressed) {
            error!("Failed to encode SubscribeUpdate: {:?}", e);
            return;
        }

        // Compress with Zstd
        let compressed_payload = match encode_all(Cursor::new(uncompressed), 1) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to compress SubscribeUpdate with Zstd: {:?}", e);
                return;
            }
        };

        trace!(
            "ZMQ send slot={} type={:?} uncompressed={}B compressed={}B",
            raw_msg.get_slot(),
            msg_type,
            update_proto.encoded_len(),
            compressed_payload.len()
        );
        if let Some(socket) = self.sockets.get(&msg_type) {
            match socket.lock() {
                Ok(sock) => {
                    if let Err(e) = sock.send(&compressed_payload, zmq::DONTWAIT) {
                        error!("Failed to send via ZMQ: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to lock ZMQ socket: {:?}", e);
                }
            }
        } else {
            warn!("No ZMQ socket registered for type: {:?}", msg_type);
        }
    }
}
