use async_trait::async_trait;
use datafusion_common::{exec_datafusion_err, Result};
use sail_catalog_system::querier::{SessionQuerier, SessionRow};
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::SessionError;
use crate::session_manager::{SessionManagerActor, SessionManagerEvent};

pub struct SessionManagerSessionQuerier {
    handle: ActorHandle<SessionManagerActor>,
}

impl SessionManagerSessionQuerier {
    pub fn new(handle: ActorHandle<SessionManagerActor>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl SessionQuerier for SessionManagerSessionQuerier {
    async fn sessions(&self) -> Result<Vec<SessionRow>> {
        let (tx, rx) = oneshot::channel();
        self.handle
            .send(SessionManagerEvent::QuerySessions { result: tx })
            .await
            .map_err(|_| exec_datafusion_err!("failed to query sessions from system manager"))?;
        rx.await
            .map_err(|_| exec_datafusion_err!("failed to receive sessions from system manager"))?
            .map_err(|e| match e {
                SessionError::DataFusionError(x) => x,
                _ => exec_datafusion_err!("failed to query sessions from system manager: {}", e),
            })
    }
}
