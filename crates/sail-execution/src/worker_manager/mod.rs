mod kubernetes;
mod local;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

#[derive(Debug)]
pub struct WorkerLaunchOptions {
    pub enable_tls: bool,
    pub driver_external_host: String,
    pub driver_external_port: u16,
}

#[tonic::async_trait]
pub trait WorkerManager: Send + Sync + 'static {
    /// Launch a worker.
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()>;

    /// Stop all workers on a best-effort basis.
    /// The driver have attempted to sent shutdown events to all workers
    /// at this point, but it is unknown whether the events have been received.
    /// The worker manager is supposed to wait for the termination of all workers
    /// before returning from this method.
    /// The worker manager may also decide to return early after a certain timeout.
    /// There can be workers that are being started concurrently,
    /// which may not be stopped by this method.
    /// Also, note that the driver process may terminate abnormally before all workers are stopped.
    /// In such cases, the external systems should be responsible for cleaning up the workers
    /// (e.g. via the cascading deletion mechanism in Kubernetes).
    async fn stop(&self) -> ExecutionResult<()>;
}

pub(crate) use kubernetes::KubernetesWorkerManager;
pub(crate) use local::LocalWorkerManager;
