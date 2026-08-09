use std::sync::Arc;

use indexmap::IndexMap;
use log::{info, warn};
use sail_common::actor::{Actor, ActorAction, ActorContext, ActorHandle};
use sail_execution::driver::{DriverHandle, DriverRegistryAccessor};
use sail_execution::error::{ExecutionError, ExecutionResult};
use sail_execution::{DriverId, IdGenerator};

use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::{
    SessionManagerComponents, SessionManagerMessage, SessionManagerOptions,
};

struct SessionDriverRegistry {
    handle: ActorHandle<SessionManagerActor>,
}

#[tonic::async_trait]
impl DriverRegistryAccessor for SessionDriverRegistry {
    async fn get(&self, driver_id: DriverId) -> ExecutionResult<DriverHandle> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.handle
            .send(SessionManagerMessage::GetDriver {
                driver_id,
                result: tx,
            })
            .await
            .map_err(ExecutionError::from)?;
        rx.await.map_err(ExecutionError::from)?
    }
}

#[tonic::async_trait]
impl Actor for SessionManagerActor {
    type Message = SessionManagerMessage;
    type Options = (SessionManagerOptions, SessionManagerComponents);

    fn name() -> &'static str {
        "SessionManagerActor"
    }

    fn new(options: Self::Options) -> Self {
        let (options, components) = options;
        let SessionManagerComponents {
            session_factory,
            job_runner_factory,
            driver_gateway,
        } = components;
        Self {
            options,
            session_factory,
            job_runner_factory,
            sessions: IndexMap::new(),
            drivers: Default::default(),
            driver_gateway,
            driver_id_generator: IdGenerator::new(),
            shutdown_notifier: None,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let Some(driver_gateway) = &mut self.driver_gateway else {
            return;
        };
        driver_gateway.start(Arc::new(SessionDriverRegistry {
            handle: ctx.handle().clone(),
        }));
        info!("driver server is ready on port {}", driver_gateway.port());
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            SessionManagerMessage::GetOrCreateSession {
                session_id,
                user_id,
                result,
            } => self.handle_get_or_create_session(ctx, session_id, user_id, result),
            SessionManagerMessage::ProbeIdleSession {
                session_id,
                instant,
            } => self.handle_probe_idle_session(ctx, session_id, instant),
            SessionManagerMessage::DeleteSession { session_id, result } => {
                self.handle_delete_session(ctx, session_id, result)
            }
            SessionManagerMessage::SetSessionHistory {
                session_id,
                history,
            } => self.handle_set_session_history(ctx, session_id, history),
            SessionManagerMessage::SetSessionFailure { session_id } => {
                self.handle_set_session_failure(ctx, session_id)
            }
            SessionManagerMessage::ObserveState { observer } => {
                self.handle_observe_state(ctx, observer)
            }
            SessionManagerMessage::GetDriver { driver_id, result } => {
                self.handle_get_driver(driver_id, result)
            }
            SessionManagerMessage::Shutdown { result } => {
                self.shutdown_notifier = Some(result);
                ActorAction::Stop
            }
        }
    }

    async fn stop(mut self, ctx: &mut ActorContext<Self>) {
        // Keep the gateway available while drivers stop. Graceful gateway shutdown waits for
        // active task stream connections, which are owned by the drivers.
        let drivers = self.drivers.drain().collect::<Vec<_>>();
        for (driver_id, driver) in drivers {
            if let Err(e) = driver.shutdown_and_wait().await {
                warn!("failed to shut down driver {driver_id}: {e}");
            }
        }
        ctx.children_mut().join().await;
        if let Some(mut driver_gateway) = self.driver_gateway {
            driver_gateway.stop().await;
            info!("driver server has stopped");
        }
        if let Some(result) = self.shutdown_notifier {
            let _ = result.send(());
        }
    }
}
