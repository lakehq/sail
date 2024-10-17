mod local;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

pub struct WorkerLaunchContext {
    pub driver_host: String,
    pub driver_port: u16,
}

#[tonic::async_trait]
pub trait WorkerManager: Send + 'static {
    async fn launch_worker(
        &mut self,
        id: WorkerId,
        ctx: WorkerLaunchContext,
    ) -> ExecutionResult<()>;
}

pub(crate) use local::LocalWorkerManager;
