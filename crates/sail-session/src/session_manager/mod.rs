mod actor;
mod event;
mod options;

use std::fmt;
use std::hash::Hash;

use datafusion::prelude::SessionContext;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::SessionManagerEvent;
pub use crate::session_manager::options::SessionManagerOptions;

/// A marker trait for session keys.
pub trait SessionKey: fmt::Display + Clone + Eq + Hash + Send + 'static {}

pub struct SessionManager<K: SessionKey> {
    handle: ActorHandle<SessionManagerActor<K>>,
}

impl<K: SessionKey> fmt::Debug for SessionManager<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl<K: SessionKey> SessionManager<K> {
    pub fn try_new(options: SessionManagerOptions<K>) -> SessionResult<Self> {
        let system = options.system.clone();
        let handle = system.lock()?.spawn::<SessionManagerActor<K>>(options);
        Ok(Self { handle })
    }

    pub async fn get_or_create_session_context(&self, key: K) -> SessionResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::GetOrCreateSession { key, result: tx };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to get session: {e}")))?
    }

    pub async fn delete_session(&self, key: K) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::DeleteSession { key, result: tx };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to delete session: {e}")))?
    }
}
