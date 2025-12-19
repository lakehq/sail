use std::collections::HashMap;

use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::SessionManagerEvent;
use crate::session_manager::options::SessionManagerOptions;
use crate::session_manager::SessionKey;

#[tonic::async_trait]
impl<K: SessionKey> Actor for SessionManagerActor<K> {
    type Message = SessionManagerEvent<K>;
    type Options = SessionManagerOptions<K>;

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
            SessionManagerEvent::GetOrCreateSession { key, result } => {
                self.handle_get_or_create_session(ctx, key, result)
            }
            SessionManagerEvent::ProbeIdleSession { key, instant } => {
                self.handle_probe_idle_session(ctx, key, instant)
            }
            SessionManagerEvent::DeleteSession { key, result } => {
                self.handle_delete_session(ctx, key, result)
            }
        }
    }
}
