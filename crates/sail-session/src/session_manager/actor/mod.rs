mod core;
mod handler;

use std::collections::HashMap;

use datafusion::prelude::SessionContext;

use crate::session_factory::{ServerSessionInfo, SessionFactory};

pub struct SessionManagerActor {
    options: super::options::SessionManagerOptions,
    factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    sessions: HashMap<String, SessionContext>,
}
