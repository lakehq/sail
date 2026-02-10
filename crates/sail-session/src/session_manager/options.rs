use std::sync::{Arc, Mutex};
use std::time::Duration;

use sail_common::runtime::RuntimeHandle;
use sail_server::actor::ActorSystem;

use crate::session_factory::{ServerSessionInfo, SessionFactory};

#[readonly::make]
pub struct SessionManagerOptions {
    pub session_timeout: Duration,
    pub runtime: RuntimeHandle,
    pub system: Arc<Mutex<ActorSystem>>,
    pub factory: Box<dyn Fn() -> Box<dyn SessionFactory<ServerSessionInfo>> + Send>,
}

impl SessionManagerOptions {
    pub fn new(
        runtime: RuntimeHandle,
        system: Arc<Mutex<ActorSystem>>,
        factory: Box<dyn Fn() -> Box<dyn SessionFactory<ServerSessionInfo>> + Send>,
    ) -> Self {
        Self {
            session_timeout: Duration::MAX,
            runtime,
            system,
            factory,
        }
    }

    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }
}
