mod core;
mod handler;
mod message;
mod options;
mod rpc;

pub(crate) use message::{WorkerLocation, WorkerMessage};
pub(crate) use options::WorkerOptions;
use sail_common::actor::ActorHandle;

use crate::driver::DriverClientSet;
use crate::rpc::ServerMonitor;
use crate::task_runner::TaskRunnerActor;

pub struct WorkerActor {
    options: WorkerOptions,
    server: ServerMonitor,
    driver_client_set: DriverClientSet,
    task_runner: Option<ActorHandle<TaskRunnerActor>>,
}
