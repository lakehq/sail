// SPDX-License-Identifier: Apache-2.0

mod actor;
mod session;

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use sail_common::actor::{ActorHandle, ActorSystem};
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common::server::ServerBuilder;
use sail_common_datafusion::session::job::ServerShutdown;
use sail_execution::driver::{DriverGateway, DriverGatewayOptions};
use sail_telemetry::telemetry::global_system_event_reporter;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::{
    ServerSessionInfo, ServerSessionJobRunnerFactory, SessionFactory, SessionJobRunnerFactory,
};
pub(crate) use crate::session_manager::actor::{SessionManagerActor, SessionManagerMessage};
pub use crate::session_manager::actor::{SessionManagerComponents, SessionManagerOptions};

pub type ServerSessionFactoryFn =
    fn(Arc<AppConfig>, RuntimeHandle) -> Box<dyn SessionFactory<ServerSessionInfo>>;

#[derive(Clone)]
pub struct SessionManager {
    handle: ActorHandle<SessionManagerActor>,
    shutdown: Arc<ServerShutdown>,
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
        let shutdown = Arc::new(ServerShutdown::default());
        let handle = system.spawn::<SessionManagerActor>((options, components, shutdown.clone()));
        Ok(Self { handle, shutdown })
    }

    pub async fn get_or_create_session_context(
        &self,
        client_session_id: String,
        user_id: String,
    ) -> SessionResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::GetOrCreateSession {
            client_session_id,
            user_id,
            result: tx,
        };
        self.handle.send(message).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to get session: {e}")))?
    }

    pub async fn delete_session(
        &self,
        client_session_id: String,
        user_id: String,
    ) -> SessionResult<String> {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::DeleteSession {
            client_session_id,
            user_id,
            result: tx,
        };
        self.handle.send(message).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to delete session: {e}")))?
    }

    /// Cancel work inside a query RPC, including execution needed to resolve its plan.
    pub async fn run_query<T>(
        &self,
        query: impl Future<Output = Result<T, tonic::Status>>,
    ) -> Result<T, tonic::Status> {
        tokio::select! {
            biased;
            () = self.shutdown.cancelled() => Err(tonic::Status::cancelled("query cancelled because the server is shutting down")),
            result = query => result,
        }
    }

    /// Shut down sessions as the server starts draining active requests.
    pub async fn serve(
        self,
        builder: ServerBuilder<'_>,
        listener: TcpListener,
        signal: impl Future<Output = ()>,
    ) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        let serve = async move {
            let signal = async move {
                signal.await;
                let _ = tx.send(());
            };
            builder
                .serve(listener, signal)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        };
        let shutdown = async {
            // The sender also drops if the server exits before receiving the signal.
            let _ = rx.await;
            self.shutdown().await
        };
        let (result, shutdown) = tokio::join!(serve, shutdown);
        shutdown?;
        Ok(result?)
    }

    /// Shut down the session manager and all resources it owns.
    pub async fn shutdown(&self) -> SessionResult<()> {
        self.shutdown.cancel();
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
        event_reporter: global_system_event_reporter().ok_or_else(|| {
            SessionError::internal("telemetry is not initialized for system store")
        })?,
    };
    SessionManager::try_new(options, components, system)
}
