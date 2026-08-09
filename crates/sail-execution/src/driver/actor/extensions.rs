use crate::driver::DriverOptions;
use crate::shuffle::ShuffleBackendKind;
use crate::stream::local::{LocalStreamManager, LocalStreamManagerOptions};
use crate::stream::storage::StorageStreamManager;

pub struct DriverExtensions {
    pub local_streams: LocalStreamManager,
    pub storage_streams: Option<StorageStreamManager>,
}

impl DriverExtensions {
    pub fn new(options: &DriverOptions) -> Self {
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
