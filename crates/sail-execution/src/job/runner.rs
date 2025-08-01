use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties};
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

    async fn execute_stream(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => self.execute_stream_partitioned(plan, 0, context).await,
            2.. => {
                self.execute_stream_partitioned(
                    Arc::new(CoalescePartitionsExec::new(plan)),
                    0,
                    context,
                )
                .await
            }
        }
    }

    async fn execute_stream_partitioned(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let handle = self.runtime.cpu().clone();
        let stream = handle
            .spawn(async move { plan.execute(partition, context) })
            .await
            .map_err(|e| {
                ExecutionError::InternalError(format!("failed to execute on CPU runtime: {e}"))
            })??;
        let schema = stream.schema();
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
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
        Ok(self.execute_stream(plan, ctx.task_ctx()).await?)
        // let task_ctx = ctx.task_ctx();
        // let handle = self.runtime.cpu().clone();
        // let result = handle
        //     .spawn(async move { execute_stream(plan, task_ctx) })
        //     .await
        //     .map_err(|e| {
        //         ExecutionError::InternalError(format!("failed to execute on CPU runtime: {e}"))
        //     })?;
        // Ok(result?)
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
