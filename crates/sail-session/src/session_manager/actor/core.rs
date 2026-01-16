use std::collections::HashMap;

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
            sessions: HashMap::new(),
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
            SessionManagerEvent::QuerySessions { result } => {
                self.handle_query_sessions(ctx, result)
            }
        }
    }
}
