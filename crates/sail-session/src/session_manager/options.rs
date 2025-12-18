use std::sync::Arc;

use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;

#[readonly::make]
pub struct SessionManagerOptions {
    pub config: Arc<AppConfig>,
    pub runtime: RuntimeHandle,
}

impl SessionManagerOptions {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        Self { config, runtime }
    }
}
