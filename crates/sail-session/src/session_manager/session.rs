use datafusion::prelude::SessionContext;
use sail_execution::DriverId;

pub struct ServerSession {
    pub user_id: String,
    pub state: ServerSessionState,
}

pub enum ServerSessionState {
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
            ServerSessionState::Running { .. } => "RUNNING",
            ServerSessionState::Deleted => "DELETED",
            ServerSessionState::Failed => "FAILED",
        }
    }
}
