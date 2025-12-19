use std::sync::{Arc, Mutex};

use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::actor::ActorSystem;

use crate::session_factory::SessionFactory;

#[readonly::make]
pub struct SessionManagerOptions<I> {
    pub config: Arc<AppConfig>,
    pub runtime: RuntimeHandle,
    pub system: Arc<Mutex<ActorSystem>>,
    pub factory: Box<dyn Fn() -> Box<dyn SessionFactory<I>> + Send>,
}

impl<I> SessionManagerOptions<I> {
    pub fn new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        system: Arc<Mutex<ActorSystem>>,
        factory: Box<dyn Fn() -> Box<dyn SessionFactory<I>> + Send>,
    ) -> Self {
        Self {
            config,
            runtime,
            system,
            factory,
        }
    }
}
