use datafusion::prelude::SessionContext;
use sail_execution::DriverId;

pub struct ServerSession {
    pub state: ServerSessionState,
}

pub enum ServerSessionState {
    Creating {
        driver_id: Option<DriverId>,
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
