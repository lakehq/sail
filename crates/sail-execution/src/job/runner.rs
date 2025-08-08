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

// CHECK HERE DO NOT MERGE IF THIS FEATURE FLAG IS STILL HERE
pub static RUNNER_EXECUTE_FN_HANDLE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    std::env::var("SAIL_RUNNER_EXECUTE_FN_HANDLE")
        .map(|v| v.to_lowercase())
        .unwrap_or_else(|_| "default".to_string())
});

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream>;

    async fn stop(&self);

    fn runtime(&self) -> &RuntimeHandle;
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

        match RUNNER_EXECUTE_FN_HANDLE.to_lowercase().as_str() {
            "primary" => {
                let task_ctx = ctx.task_ctx();
                let handle = self.runtime.primary().clone();
                handle
                    .spawn(async move {
                        let result = execute_stream(plan, task_ctx)?;
                        Ok(result)
                    })
                    .await
                    .map_err(|e| {
                        ExecutionError::InternalError(format!(
                            "failed to execute on CPU runtime: {e}"
                        ))
                    })?
            }
            "cpu" => {
                let task_ctx = ctx.task_ctx();
                let handle = self.runtime.cpu().clone();
                handle
                    .spawn(async move {
                        let result = execute_stream(plan, task_ctx)?;
                        Ok(result)
                    })
                    .await
                    .map_err(|e| {
                        ExecutionError::InternalError(format!(
                            "failed to execute on CPU runtime: {e}"
                        ))
                    })?
            }
            "default" => Ok(execute_stream(plan, ctx.task_ctx())?),
            other => {
                log::warn!("Unsupported runner execute function handle: {other}, using default",);
                Ok(execute_stream(plan, ctx.task_ctx())?)
            }
        }
    }

    async fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }
}

pub struct ClusterJobRunner {
    driver: ActorHandle<DriverActor>,
    runtime: RuntimeHandle,
}

impl ClusterJobRunner {
    pub fn new(system: &mut ActorSystem, options: DriverOptions) -> Self {
        let runtime = options.runtime.clone();
        let driver = system.spawn(options);
        Self { driver, runtime }
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

    fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }
}
