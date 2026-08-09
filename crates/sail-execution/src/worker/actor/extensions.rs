use crate::shuffle::ShuffleBackendKind;
use crate::stream::local::{LocalStreamManager, LocalStreamManagerOptions};
use crate::stream::storage::StorageStreamManager;
use crate::worker::WorkerOptions;

pub struct WorkerExtensions {
    pub local_streams: LocalStreamManager,
    pub storage_streams: Option<StorageStreamManager>,
}

impl WorkerExtensions {
    pub fn new(options: &WorkerOptions) -> Self {
        let storage_streams = match &options.shuffle_backend {
            ShuffleBackendKind::Flight => None,
            ShuffleBackendKind::Storage {
                path,
                max_file_size,
                compression,
            } => Some(StorageStreamManager::new(
                path.clone(),
                options.session_id.clone(),
                *max_file_size,
                *compression,
            )),
        };
        Self {
            local_streams: LocalStreamManager::new(LocalStreamManagerOptions::from(options)),
            storage_streams,
        }
    }
}
