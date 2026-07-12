mod core;
mod handler;

use datafusion::prelude::SessionContext;
use indexmap::IndexMap;
use tokio::sync::oneshot;

use crate::error::SessionResult;
use crate::session_factory::{ServerSessionInfo, SessionFactory};
use crate::session_manager::session::{ServerSession, SessionKey};

pub struct SessionManagerActor {
    options: super::options::SessionManagerOptions,
    factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    sessions: IndexMap<SessionKey, ServerSession>,
    shutdown_contexts: Vec<SessionContext>,
    shutting_down: bool,
    shutdown_results: Vec<oneshot::Sender<SessionResult<()>>>,
}
