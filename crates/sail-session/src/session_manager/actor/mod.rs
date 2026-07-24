mod core;
mod handler;

use indexmap::IndexMap;
use sail_execution::driver::{DriverGateway, DriverRegistry};
use sail_execution::{DriverId, IdGenerator};

use crate::session_factory::{ServerSessionInfo, SessionFactory, SessionJobRunnerFactory};
use crate::session_manager::session::ServerSession;

pub struct SessionManagerActor {
    options: super::options::SessionManagerOptions,
    session_factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    job_runner_factory: Box<dyn SessionJobRunnerFactory>,
    sessions: IndexMap<String, ServerSession>,
    drivers: DriverRegistry,
    gateway: Option<DriverGateway>,
    driver_id_generator: IdGenerator<DriverId>,
}
