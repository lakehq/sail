mod kubernetes;
mod local;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

#[derive(Debug)]
pub struct WorkerLaunchOptions {
    pub enable_tls: bool,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub memory_stream_buffer: usize,
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
    /// Note that the driver process may terminate abnormally before this method finishes.
    /// In such cases, the external systems should be responsible for cleaning up the workers
    /// (e.g. via the cascading deletion mechanism in Kubernetes).
    async fn stop(&self) -> ExecutionResult<()>;
}

pub(crate) use kubernetes::{KubernetesWorkerManager, KubernetesWorkerManagerOptions};
pub(crate) use local::LocalWorkerManager;
