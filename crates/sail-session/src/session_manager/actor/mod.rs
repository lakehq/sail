mod core;
mod handler;

use std::collections::HashMap;

use datafusion::prelude::SessionContext;

use crate::session_factory::SessionFactory;

pub struct SessionManagerActor<K> {
    options: super::options::SessionManagerOptions<K>,
    factory: Box<dyn SessionFactory<K>>,
    sessions: HashMap<K, SessionContext>,
}
