mod actor;
mod event;
mod options;
mod session;

use std::fmt;

use datafusion::prelude::SessionContext;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
pub(crate) use crate::session_manager::actor::SessionManagerActor;
pub(crate) use crate::session_manager::event::SessionManagerEvent;
pub use crate::session_manager::options::SessionManagerOptions;

pub struct SessionManager {
    handle: ActorHandle<SessionManagerActor>,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub fn try_new(options: SessionManagerOptions) -> SessionResult<Self> {
        let system = options.system.clone();
        let handle = system.lock()?.spawn::<SessionManagerActor>(options);
        Ok(Self { handle })
    }

    pub async fn get_or_create_session_context(
        &self,
        session_id: String,
        user_id: String,
    ) -> SessionResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::GetOrCreateSession {
            session_id,
            user_id,
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to get session: {e}")))?
    }

    pub async fn delete_session(&self, session_id: String) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::DeleteSession {
            session_id,
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to delete session: {e}")))?
    }
}
