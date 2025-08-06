use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use sail_runtime::RuntimeHandle;
use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverEvent, DriverOptions};
use crate::error::{ExecutionError, ExecutionResult};

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream>;

    async fn stop(&self);
}

pub struct LocalJobRunner {
    stopped: AtomicBool,
    runtime: RuntimeHandle,
}

impl LocalJobRunner {
    pub fn new(runtime: RuntimeHandle) -> Self {
        Self {
            stopped: AtomicBool::new(false),
            runtime,
        }
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        if self.stopped.load(Ordering::Relaxed) {
            return Err(ExecutionError::InternalError(
                "job runner is stopped".to_string(),
            ));
        }
        // Ok(execute_stream(plan, ctx.task_ctx())?)
        let task_ctx = ctx.task_ctx();
        let handle = self.runtime.cpu().clone();
        let result = handle
            .spawn(async move {
                let result = execute_stream(plan, task_ctx)?;
                Ok(result)
            })
            .await
            .map_err(|e| {
                ExecutionError::InternalError(format!("failed to execute on CPU runtime: {e}"))
            })?;

        result
    }

    async fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

pub struct ClusterJobRunner {
    driver: ActorHandle<DriverActor>,
}

impl ClusterJobRunner {
    pub fn new(system: &mut ActorSystem, options: DriverOptions) -> Self {
        let driver = system.spawn(options);
        Self { driver }
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    async fn execute(
        &self,
        // TODO: propagate session context from the driver to the worker
        _ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteJob { plan, result: tx })
            .await?;
        rx.await.map_err(|e| {
            ExecutionError::InternalError(format!("failed to create job stream: {e}"))
        })?
    }

    async fn stop(&self) {
        let _ = self.driver.send(DriverEvent::Shutdown).await;
    }
}
