mod core;
mod handler;

use std::collections::{HashMap, HashSet};

use crate::error::CelebornError;
use crate::lifecycle::options::LifecycleManagerOptions;
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
    pub(super) options: LifecycleManagerOptions,
    pub(super) client: MasterClient,
    pub(super) registered_shuffles: HashMap<i32, HashMap<String, WorkerSlotLocations>>,
    pub(super) mapper_attempts: HashMap<i32, Vec<i32>>,
    pub(super) committing_shuffles: HashSet<i32>,
    pub(super) committed_shuffles: HashSet<i32>,
    pub(super) application_registration: ApplicationRegistration,
}
