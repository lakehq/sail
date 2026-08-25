use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use sail_common::actor::ActorSystem;
use sail_common_datafusion::session::job::{JobRunner, PreparedJob, PreparedJobHandle};
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverComponents, DriverHandle, DriverMessage, DriverOptions};

pub struct LocalJobRunner {
    next_job_id: AtomicU64,
    stopped: AtomicBool,
}

impl LocalJobRunner {
    pub fn new() -> Self {
        Self {
            next_job_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
        }
    }

    fn start_job(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        if self.stopped.load(Ordering::Relaxed) {
            return internal_err!("job runner is stopped");
        }
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let options = TracingExecOptions {
            metrics: global_metrics(),
            job_id: Some(job_id),
            stage: None,
            attempt: None,
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        let stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
        Ok((stream, plan))
    }
}

impl Default for LocalJobRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    async fn prepare(&self, _plan: Arc<dyn ExecutionPlan>) -> Result<PreparedJob> {
        Err(DataFusionError::NotImplemented(
            "distributed explain is not supported in local execution mode".to_string(),
        ))
    }

    async fn execute_prepared(
        &self,
        _ctx: &SessionContext,
        _job: PreparedJob,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::NotImplemented(
            "prepared distributed jobs are not supported in local execution mode".to_string(),
        ))
    }

    async fn discard_prepared(&self, _job: PreparedJob) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "prepared distributed jobs are not supported in local execution mode".to_string(),
        ))
    }

    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let (stream, _) = self.start_job(ctx, plan)?;
        Ok(stream)
    }

    async fn execute_for_explain(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        if self.stopped.load(Ordering::Relaxed) {
            return internal_err!("job runner is stopped");
        }
        let _ = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        // Keep metrics attached to the exact plan rendered by EXPLAIN ANALYZE.
        let stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
        Ok((stream, plan))
    }

    async fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

pub struct ClusterJobRunner {
    driver: DriverHandle,
}

impl ClusterJobRunner {
    pub fn new(
        system: &mut ActorSystem,
        options: DriverOptions,
        components: DriverComponents,
    ) -> Self {
        let driver = DriverHandle::new(system.spawn::<DriverActor>((options, components)));
        Self { driver }
    }

    pub fn driver(&self) -> DriverHandle {
        self.driver.clone()
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    async fn prepare(&self, plan: Arc<dyn ExecutionPlan>) -> Result<PreparedJob> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverMessage::PrepareJob { plan, result: tx })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        let (job_id, distributed_plan) = rx
            .await
            .map_err(|e| internal_datafusion_err!("failed to prepare job: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(PreparedJob::new(
            PreparedJobHandle::new(job_id.into()),
            distributed_plan,
        ))
    }

    async fn execute_prepared(
        &self,
        ctx: &SessionContext,
        job: PreparedJob,
    ) -> Result<SendableRecordBatchStream> {
        let (handle, _) = job.into_parts();
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverMessage::ExecutePreparedJob {
                job_id: handle.value().into(),
                context: ctx.task_ctx(),
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }

    async fn discard_prepared(&self, job: PreparedJob) -> Result<()> {
        let (handle, _) = job.into_parts();
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverMessage::DiscardPreparedJob {
                job_id: handle.value().into(),
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to discard prepared job: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }

    /// Executes a plan on the cluster. This is where the cool stuff happens.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let job = self.prepare(plan).await?;
        self.execute_prepared(ctx, job).await
    }

    async fn stop(&self) {
        let _ = self.driver.shutdown_and_wait().await;
    }
}
