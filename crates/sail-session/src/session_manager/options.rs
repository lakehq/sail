use std::sync::{Arc, Mutex};
use std::time::Duration;

use sail_common::runtime::RuntimeHandle;
use sail_execution::driver::DriverGateway;
use sail_server::actor::ActorSystem;

use crate::session_factory::{ServerSessionInfo, SessionFactory, SessionJobRunnerFactory};

#[readonly::make]
pub struct SessionManagerOptions {
    pub session_timeout: Duration,
    pub runtime: RuntimeHandle,
    pub system: Arc<Mutex<ActorSystem>>,
    pub factory: Box<dyn Fn() -> Box<dyn SessionFactory<ServerSessionInfo>> + Send>,
    pub job_runner_factory: Box<dyn Fn() -> Box<dyn SessionJobRunnerFactory> + Send>,
    pub driver_gateway: Option<DriverGateway>,
    /// The application configuration options as key-value pairs,
    /// used to populate the `system.session.options` table.
    pub options: Vec<(String, String)>,
}

impl SessionManagerOptions {
    pub fn new(
        runtime: RuntimeHandle,
        system: Arc<Mutex<ActorSystem>>,
        factory: Box<dyn Fn() -> Box<dyn SessionFactory<ServerSessionInfo>> + Send>,
        job_runner_factory: Box<dyn Fn() -> Box<dyn SessionJobRunnerFactory> + Send>,
    ) -> Self {
        Self {
            session_timeout: Duration::MAX,
            runtime,
            system,
            factory,
            job_runner_factory,
            driver_gateway: None,
            options: Vec::new(),
        }
    }

    pub fn with_driver_gateway(mut self, gateway: DriverGateway) -> Self {
        self.driver_gateway = Some(gateway);
        self
    }

    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    pub fn with_options(mut self, options: Vec<(String, String)>) -> Self {
        self.options = options;
        self
    }

    pub(crate) fn take_driver_gateway(&mut self) -> Option<DriverGateway> {
        self.driver_gateway.take()
    }
}
