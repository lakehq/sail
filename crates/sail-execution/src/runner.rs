use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::common::{internal_datafusion_err, internal_err, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use sail_common_datafusion::session::JobRunner;
use sail_server::actor::{ActorHandle, ActorSystem};
use sail_telemetry::telemetry::global_metric_registry;
use sail_telemetry::{trace_execution_plan, TracingExecOptions};
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverEvent, DriverOptions};

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
}

impl Default for LocalJobRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if self.stopped.load(Ordering::Relaxed) {
            return internal_err!("job runner is stopped");
        }
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let options = TracingExecOptions {
            metric_registry: global_metric_registry(),
            job_id: Some(job_id),
            task_id: None,
            task_attempt: None,
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        Ok(execute_stream(plan, ctx.task_ctx())?)
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
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteJob { plan, result: tx })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }

    async fn stop(&self) {
        let _ = self.driver.send(DriverEvent::Shutdown).await;
    }
}
