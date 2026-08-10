use sail_common::actor::ActorHandle;
use sail_common::utils::retry::RetryStrategy;

use crate::driver::{DriverActor, DriverClientSet};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::shuffle::ShuffleBackendKind;
use crate::stream::celeborn::CelebornStreamManager;
use crate::stream::local::{LocalStreamManager, LocalStreamManagerOptions};
use crate::stream::storage::StorageStreamManager;
use crate::worker::WorkerActor;
use crate::worker::peer_tracker::PeerTracker;

pub struct TaskRunnerExtensions {
    pub local_streams: LocalStreamManager,
    pub storage_streams: Option<StorageStreamManager>,
    pub celeborn_streams: Option<CelebornStreamManager>,
}

impl TaskRunnerExtensions {
    pub fn new(
        local_stream_options: LocalStreamManagerOptions,
        shuffle_backend: &ShuffleBackendKind,
        session_id: String,
        celeborn_streams: Option<CelebornStreamManager>,
    ) -> Self {
        let storage_streams = match shuffle_backend {
            ShuffleBackendKind::Flight => None,
            ShuffleBackendKind::Storage {
                path,
                max_file_size,
                compression,
            } => Some(StorageStreamManager::new(
                path.clone(),
                session_id,
                *max_file_size,
                *compression,
            )),
            ShuffleBackendKind::Celeborn { .. } => None,
        };
        Self {
            local_streams: LocalStreamManager::new(local_stream_options),
            storage_streams,
            celeborn_streams,
        }
    }

    pub fn storage_streams(&self) -> ExecutionResult<&StorageStreamManager> {
        self.storage_streams.as_ref().ok_or_else(|| {
            ExecutionError::InternalError(
                "storage stream requested without a storage shuffle backend".to_string(),
            )
        })
    }
}

pub enum TaskRunnerPlacement {
    Driver {
        driver: ActorHandle<DriverActor>,
    },
    Worker {
        worker_id: WorkerId,
        /// A monotonically increasing sequence number for ordered messages.
        sequence: u64,
        driver: DriverClientSet,
        worker: ActorHandle<WorkerActor>,
        peers: PeerTracker,
        retry_strategy: RetryStrategy,
    },
}

pub struct TaskRunnerComponents {
    pub extensions: TaskRunnerExtensions,
    pub placement: TaskRunnerPlacement,
}
