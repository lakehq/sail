use indexmap::IndexMap;
use log::{error, info};
use sail_execution::driver::DriverGateway;
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::SessionManagerEvent;
use crate::session_manager::options::SessionManagerOptions;

#[tonic::async_trait]
impl Actor for SessionManagerActor {
    type Message = SessionManagerEvent;
    type Options = SessionManagerOptions;

    fn name() -> &'static str {
        "SessionManagerActor"
    }

    fn new(options: Self::Options) -> Self {
        let factory = (options.factory)();
        let job_runner_factory = (options.job_runner_factory)();
        Self {
            options,
            factory,
            job_runner_factory,
            sessions: IndexMap::new(),
            drivers: Default::default(),
            gateway: None,
            next_driver_id: 1,
        }
    }

    async fn start(&mut self, _ctx: &mut ActorContext<Self>) {
        let Some(options) = self.options.driver_gateway.clone() else {
            return;
        };
        match DriverGateway::start(options, self.drivers.clone()).await {
            Ok(gateway) => {
                info!("driver server is ready on port {}", gateway.port());
                self.gateway = Some(gateway);
            }
            Err(e) => error!("failed to start driver server: {e}"),
        }
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
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        if let Some(gateway) = self.gateway {
            gateway.stop().await;
            info!("driver server has stopped");
        }
    }
}
