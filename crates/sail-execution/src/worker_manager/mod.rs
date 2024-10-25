mod local;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

pub struct WorkerLaunchOptions {
    pub driver_host: String,
    pub driver_port: u16,
}

#[tonic::async_trait]
pub trait WorkerManager: Send + Sync + 'static {
    async fn start_worker(&self, id: WorkerId, options: WorkerLaunchOptions)
        -> ExecutionResult<()>;
    async fn stop_worker(&self, id: WorkerId) -> ExecutionResult<()>;
    /// Stop all workers on a best-effort basis.
    /// There can be workers that are being started concurrently,
    /// which may not be stopped by this method.
    /// Also, note that the driver process may terminate abnormally before all workers are stopped.
    /// In such cases, the external systems should be responsible for cleaning up the workers
    /// (e.g. via the cascading deletion mechanism in Kubernetes).
    async fn stop_all_workers(&self) -> ExecutionResult<()>;
}

pub(crate) use local::LocalWorkerManager;
