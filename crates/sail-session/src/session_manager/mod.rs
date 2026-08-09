mod actor;
mod session;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use sail_common::actor::{ActorHandle, ActorSystem};
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_execution::driver::{DriverGateway, DriverGatewayOptions};
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::{
    ServerSessionInfo, ServerSessionJobRunnerFactory, SessionFactory, SessionJobRunnerFactory,
};
pub(crate) use crate::session_manager::actor::{
    SessionHistory, SessionManagerActor, SessionManagerMessage,
};
pub use crate::session_manager::actor::{SessionManagerComponents, SessionManagerOptions};

pub type ServerSessionFactoryFn =
    fn(Arc<AppConfig>, RuntimeHandle) -> Box<dyn SessionFactory<ServerSessionInfo>>;

#[derive(Clone)]
pub struct SessionManager {
    handle: ActorHandle<SessionManagerActor>,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub fn try_new(
        options: SessionManagerOptions,
        components: SessionManagerComponents,
        system: &mut ActorSystem,
    ) -> SessionResult<Self> {
        let handle = system.spawn::<SessionManagerActor>((options, components));
        Ok(Self { handle })
    }

    pub async fn get_or_create_session_context(
        &self,
        session_id: String,
        user_id: String,
    ) -> SessionResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::GetOrCreateSession {
            session_id,
            user_id,
            result: tx,
        };
        self.handle.send(message).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to get session: {e}")))?
    }

    pub async fn delete_session(&self, session_id: String) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::DeleteSession {
            session_id,
            result: tx,
        };
        self.handle.send(message).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to delete session: {e}")))?
    }

    /// Shut down the session manager and all resources it owns.
    pub async fn shutdown(&self) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.handle
            .send(SessionManagerMessage::Shutdown { result: tx })
            .await?;
        rx.await.map_err(|e| {
            SessionError::internal(format!("failed to shut down session manager: {e}"))
        })?;
        Ok(())
    }
}

pub async fn create_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    session_factory_fn: ServerSessionFactoryFn,
    session_timeout: Duration,
    system: &mut ActorSystem,
) -> SessionResult<SessionManager> {
    let session_factory = session_factory_fn(config.clone(), runtime.clone());
    let job_runner_factory = Box::new(ServerSessionJobRunnerFactory::new(
        config.clone(),
        runtime.clone(),
    )) as Box<dyn SessionJobRunnerFactory>;
    let driver_gateway = if matches!(&config.mode, ExecutionMode::Local) {
        None
    } else {
        Some(
            DriverGateway::try_new(DriverGatewayOptions::new(&config))
                .await
                .map_err(|e| {
                    SessionError::internal(format!("failed to create driver gateway: {e}"))
                })?,
        )
    };
    let options = SessionManagerOptions::new(runtime)
        .with_session_timeout(session_timeout)
        .with_options(
            config
                .raw()
                .map_err(|e| SessionError::internal(e.to_string()))?,
        );
    let components = SessionManagerComponents {
        session_factory,
        job_runner_factory,
        driver_gateway,
    };
    SessionManager::try_new(options, components, system)
}
