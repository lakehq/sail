use datafusion::prelude::SessionContext;
use sail_execution::DriverId;
use tokio::sync::oneshot;

use crate::error::SessionResult;

pub struct ServerSession {
    pub state: ServerSessionState,
}

pub enum ServerSessionState {
    Creating {
        driver_id: Option<DriverId>,
        waiters: Vec<oneshot::Sender<SessionResult<SessionContext>>>,
    },
    Running {
        context: SessionContext,
        driver_id: Option<DriverId>,
    },
    Deleted,
    Failed,
}

impl ServerSessionState {
    pub fn status(&self) -> &'static str {
        match self {
            ServerSessionState::Creating { .. } => "CREATING",
            ServerSessionState::Running { .. } => "RUNNING",
            ServerSessionState::Deleted => "DELETED",
            ServerSessionState::Failed => "FAILED",
        }
    }
}
