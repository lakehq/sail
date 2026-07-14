use indexmap::IndexMap;
use log::warn;
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
        Self {
            options,
            factory,
            sessions: IndexMap::new(),
            shutdown_contexts: Vec::new(),
            shutting_down: false,
            shutdown_results: Vec::new(),
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
                session_key,
                instant,
            } => self.handle_probe_idle_session(ctx, session_key, instant),
            SessionManagerEvent::DeleteSession {
                session_id,
                user_id,
                result,
            } => self.handle_delete_session(ctx, session_id, user_id, result),
            SessionManagerEvent::SetSessionHistory {
                session_key,
                history,
            } => self.handle_set_session_history(ctx, session_key, history),
            SessionManagerEvent::SetSessionFailure { session_key } => {
                self.handle_set_session_failure(ctx, session_key)
            }
            SessionManagerEvent::ObserveState { observer } => {
                self.handle_observe_state(ctx, observer)
            }
            SessionManagerEvent::Shutdown { result } => self.handle_shutdown(ctx, result),
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        let Self {
            options,
            factory,
            sessions,
            shutdown_contexts,
            shutting_down: _,
            shutdown_results,
        } = self;
        let resource_drop = tokio::task::spawn_blocking(move || {
            drop(sessions);
            drop(shutdown_contexts);
            drop(factory);
            drop(options);
        })
        .await;
        let resource_drop_error = resource_drop.err().map(|error| error.to_string());
        if let Some(error) = &resource_drop_error {
            warn!("failed to join session manager resource cleanup: {error}");
        }
        for result in shutdown_results {
            let output = match &resource_drop_error {
                Some(error) => Err(crate::error::SessionError::internal(format!(
                    "failed to join session manager resource cleanup: {error}"
                ))),
                None => Ok(()),
            };
            let _ = result.send(output);
        }
    }
}
