mod core;
mod handler;
mod message;
mod options;

use std::collections::{HashMap, HashSet};

pub use message::LifecycleManagerMessage;
pub use options::LifecycleManagerOptions;
use tokio::sync::oneshot;

use crate::common::{
    ApplicationMetrics, PartitionLocation, SlotReservation, WorkerIdentity, WorkerSlotLocations,
};
use crate::error::{CelebornError, CelebornResult};
use crate::master::MasterClient;
use crate::worker::WorkerClientPool;

pub(super) enum ApplicationRegistration {
    Pending,
    Succeeded,
    Failed { reason: String },
}

impl ApplicationRegistration {
    pub(super) fn error(&self) -> Option<CelebornError> {
        match self {
            Self::Pending => Some(CelebornError::Application(
                "registration is pending".to_string(),
            )),
            Self::Succeeded => None,
            Self::Failed { reason } => Some(CelebornError::Application(format!(
                "registration failed: {reason}"
            ))),
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
struct ShuffleKey {
    job_id: u64,
    stage: u64,
}

pub struct LifecycleManagerActor {
    options: LifecycleManagerOptions,
    client: MasterClient,
    worker_clients: WorkerClientPool,
    excluded_workers: HashMap<WorkerIdentity, PartitionLocation>,
    registered_shuffles: HashMap<i32, HashMap<WorkerIdentity, WorkerSlotLocations>>,
    reservations: HashMap<i32, SlotReservation>,
    pending_slot_requests: HashMap<i32, Vec<oneshot::Sender<CelebornResult<SlotReservation>>>>,
    pending_revives: HashMap<(i32, i32), Vec<oneshot::Sender<CelebornResult<PartitionLocation>>>>,
    mapper_attempts: HashMap<i32, Vec<i32>>,
    committing_shuffles: HashSet<i32>,
    committed_shuffles: HashSet<i32>,
    shuffle_ids: HashMap<ShuffleKey, i32>,
    next_shuffle_id: i32,
    application_registration: ApplicationRegistration,
    application_metrics: ApplicationMetrics,
    heartbeat_metrics: Option<ApplicationMetrics>,
}
