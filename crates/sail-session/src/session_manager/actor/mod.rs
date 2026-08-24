mod core;
mod handler;
mod message;
mod options;

use indexmap::IndexMap;
pub(crate) use message::SessionManagerMessage;
pub use options::{SessionManagerComponents, SessionManagerOptions};
use sail_execution::driver::{DriverGateway, DriverRegistry};
use sail_execution::{DriverId, IdGenerator};
use sail_telemetry::system_event::SystemEventReporter;

use crate::session_factory::{ServerSessionInfo, SessionFactory, SessionJobRunnerFactory};
use crate::session_manager::session::ServerSession;

pub struct SessionManagerActor {
    options: SessionManagerOptions,
    session_factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    job_runner_factory: Box<dyn SessionJobRunnerFactory>,
    sessions: IndexMap<String, ServerSession>,
    drivers: DriverRegistry,
    driver_gateway: Option<DriverGateway>,
    driver_id_generator: IdGenerator<DriverId>,
    shutdown_notifier: Option<tokio::sync::oneshot::Sender<()>>,
    event_reporter: SystemEventReporter,
}
