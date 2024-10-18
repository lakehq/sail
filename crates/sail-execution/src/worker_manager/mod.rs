mod local;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

pub struct WorkerLaunchContext {
    pub driver_host: String,
    pub driver_port: u16,
}

pub trait WorkerManager: Send + 'static {
    fn launch_worker(&mut self, id: WorkerId, ctx: WorkerLaunchContext) -> ExecutionResult<()>;
}

pub(crate) use local::LocalWorkerManager;
