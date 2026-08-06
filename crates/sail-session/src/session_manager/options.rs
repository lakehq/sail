use std::time::Duration;

use sail_common::runtime::RuntimeHandle;
use sail_execution::driver::DriverGateway;

use crate::session_factory::{ServerSessionInfo, SessionFactory, SessionJobRunnerFactory};

#[readonly::make]
pub struct SessionManagerOptions {
    pub session_timeout: Duration,
    pub runtime: RuntimeHandle,
    /// The application configuration options as key-value pairs,
    /// used to populate the `system.session.options` table.
    pub options: Vec<(String, String)>,
}

pub struct SessionManagerComponents {
    pub session_factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    pub job_runner_factory: Box<dyn SessionJobRunnerFactory>,
    pub driver_gateway: Option<DriverGateway>,
}

impl SessionManagerOptions {
    pub fn new(runtime: RuntimeHandle) -> Self {
        Self {
            session_timeout: Duration::MAX,
            runtime,
            options: Vec::new(),
        }
    }

    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    pub fn with_options(mut self, options: Vec<(String, String)>) -> Self {
        self.options = options;
        self
    }
}
