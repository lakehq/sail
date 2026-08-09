mod core;
mod handler;
mod message;
mod options;

use std::collections::{HashMap, HashSet};

pub use message::LifecycleManagerMessage;
pub use options::LifecycleManagerOptions;

use crate::error::CelebornError;
use crate::master::{MasterClient, WorkerSlotLocations};

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

pub struct LifecycleManagerActor {
    options: LifecycleManagerOptions,
    client: MasterClient,
    registered_shuffles: HashMap<i32, HashMap<String, WorkerSlotLocations>>,
    mapper_attempts: HashMap<i32, Vec<i32>>,
    committing_shuffles: HashSet<i32>,
    committed_shuffles: HashSet<i32>,
    application_registration: ApplicationRegistration,
}
