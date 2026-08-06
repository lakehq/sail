mod core;
mod handler;

use std::collections::HashSet;

use crate::error::CelebornError;
use crate::lifecycle::options::LifecycleManagerOptions;
use crate::master::MasterClient;

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
    pub(super) registered_shuffles: HashSet<i32>,
    pub(super) application_registration: ApplicationRegistration,
}
