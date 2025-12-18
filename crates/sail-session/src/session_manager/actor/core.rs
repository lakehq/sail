use std::collections::HashMap;

use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::SessionManagerEvent;
use crate::session_manager::options::SessionManagerOptions;
use crate::session_manager::SessionKey;

#[tonic::async_trait]
impl<K: SessionKey> Actor for SessionManagerActor<K> {
    type Message = SessionManagerEvent<K>;
    type Options = SessionManagerOptions;

    fn name() -> &'static str {
        "SessionManagerActor"
    }

    fn new(options: Self::Options) -> Self {
        Self {
            options,
            sessions: HashMap::new(),
            global_file_listing_cache: None,
            global_file_statistics_cache: None,
            global_file_metadata_cache: None,
        }
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            SessionManagerEvent::GetOrCreateSession {
                key,
                system,
                result,
            } => self.handle_get_or_create_session(ctx, key, system, result),
            SessionManagerEvent::ProbeIdleSession { key, instant } => {
                self.handle_probe_idle_session(ctx, key, instant)
            }
            SessionManagerEvent::DeleteSession { key, result } => {
                self.handle_delete_session(ctx, key, result)
            }
        }
    }
}
