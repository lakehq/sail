use std::sync::Arc;

use indexmap::IndexMap;
use log::info;
use sail_execution::DriverId;
use sail_execution::driver::{DriverHandle, DriverRegistryAccessor};
use sail_execution::error::{ExecutionError, ExecutionResult};
use sail_server::actor::{Actor, ActorAction, ActorContext, ActorHandle};

use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::SessionManagerEvent;
use crate::session_manager::options::SessionManagerOptions;

struct SessionDriverRegistry {
    handle: ActorHandle<SessionManagerActor>,
}

#[tonic::async_trait]
impl DriverRegistryAccessor for SessionDriverRegistry {
    async fn get(&self, driver_id: DriverId) -> ExecutionResult<DriverHandle> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.handle
            .send(SessionManagerEvent::GetDriver {
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
    type Message = SessionManagerEvent;
    type Options = SessionManagerOptions;

    fn name() -> &'static str {
        "SessionManagerActor"
    }

    fn new(mut options: Self::Options) -> Self {
        let factory = (options.factory)();
        let job_runner_factory = (options.job_runner_factory)();
        let gateway = options.take_driver_gateway();
        Self {
            options,
            factory,
            job_runner_factory,
            sessions: IndexMap::new(),
            drivers: Default::default(),
            gateway,
            next_driver_id: 1,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let Some(gateway) = &mut self.gateway else {
            return;
        };
        gateway.start(Arc::new(SessionDriverRegistry {
            handle: ctx.handle().clone(),
        }));
        info!("driver server is ready on port {}", gateway.port());
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            SessionManagerEvent::GetOrCreateSession {
                session_id,
                user_id,
                result,
            } => self.handle_get_or_create_session(ctx, session_id, user_id, result),
            SessionManagerEvent::ProbeIdleSession {
                session_id,
                instant,
            } => self.handle_probe_idle_session(ctx, session_id, instant),
            SessionManagerEvent::DeleteSession { session_id, result } => {
                self.handle_delete_session(ctx, session_id, result)
            }
            SessionManagerEvent::SetSessionHistory {
                session_id,
                history,
            } => self.handle_set_session_history(ctx, session_id, history),
            SessionManagerEvent::SetSessionFailure { session_id } => {
                self.handle_set_session_failure(ctx, session_id)
            }
            SessionManagerEvent::ObserveState { observer } => {
                self.handle_observe_state(ctx, observer)
            }
            SessionManagerEvent::GetDriver { driver_id, result } => {
                self.handle_get_driver(driver_id, result)
            }
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        if let Some(gateway) = self.gateway {
            gateway.stop().await;
            info!("driver server has stopped");
        }
    }
}
