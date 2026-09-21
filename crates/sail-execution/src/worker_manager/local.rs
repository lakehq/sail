use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use sail_common::actor::ActorSystem;
use sail_common::runtime::RuntimeHandle;

use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::worker::{WorkerActor, WorkerOptions};
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

pub struct LocalWorkerManager {
    runtime: RuntimeHandle,
    session: SessionContext,
}

impl LocalWorkerManager {
    pub fn new(runtime: RuntimeHandle, session: SessionContext) -> Self {
        Self { runtime, session }
    }
}

#[tonic::async_trait]
impl WorkerManager for LocalWorkerManager {
    fn launch_worker(
        &self,
        system: &mut ActorSystem,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> BoxFuture<'static, ExecutionResult<()>> {
        let options = WorkerOptions::local(id, options, self.runtime.clone(), self.session.clone());
        system.spawn::<WorkerActor>(options);
        Box::pin(std::future::ready(Ok(())))
    }

    async fn stop(&self) -> ExecutionResult<()> {
        Ok(())
    }
}
