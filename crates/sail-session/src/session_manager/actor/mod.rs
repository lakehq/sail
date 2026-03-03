mod core;
mod handler;

use indexmap::IndexMap;

use crate::session_factory::{ServerSessionInfo, SessionFactory};
use crate::session_manager::session::ServerSession;

pub struct SessionManagerActor {
    options: super::options::SessionManagerOptions,
    factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    sessions: IndexMap<String, ServerSession>,
}
