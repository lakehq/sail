mod kubernetes;
mod local;
mod options;

use futures::future::BoxFuture;
pub(crate) use options::WorkerLaunchOptions;
use sail_common::actor::ActorSystem;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

#[tonic::async_trait]
pub trait WorkerManager: Send + Sync + 'static {
    /// Start launching a worker.
    ///
    /// Local workers are spawned into `system` immediately. The returned task launches an
    /// external worker when necessary.
    fn launch_worker(
        &self,
        system: &mut ActorSystem,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> BoxFuture<'static, ExecutionResult<()>>;

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

pub use kubernetes::{KubernetesWorkerManager, KubernetesWorkerManagerOptions};
pub use local::LocalWorkerManager;
