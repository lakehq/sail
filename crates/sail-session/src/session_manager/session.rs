use datafusion::prelude::SessionContext;
use sail_execution::DriverId;
use tokio::sync::oneshot;

use crate::error::SessionResult;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub user_id: String,
    pub client_session_id: String,
}

impl std::fmt::Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.user_id, self.client_session_id)
    }
}

pub struct ServerSession {
    pub session_key: SessionKey,
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
