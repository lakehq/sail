use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream, replace_children_if_necessary};
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use sail_common::actor::ActorSystem;
use sail_common_datafusion::session::job::JobRunner;
use sail_physical_plan::range_repartition::RangeRepartitionExec;
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{TracingExecOptions, trace_execution_plan};
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverComponents, DriverHandle, DriverMessage, DriverOptions};
use crate::job_graph::{JobGraph, JobGraphOptions};
use crate::shuffle::ShuffleBackendKind;

fn explain_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    shuffle_backend: ShuffleBackendKind,
) -> Result<String> {
    JobGraph::try_new(plan, JobGraphOptions { shuffle_backend })
        .map(|graph| graph.to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub struct LocalJobRunner {
    session_id: String,
    next_job_id: AtomicU64,
    stopped: AtomicBool,
}

impl LocalJobRunner {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            next_job_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
        }
    }
}

impl Default for LocalJobRunner {
    fn default() -> Self {
        Self::new(String::new())
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(
            plan,
            ShuffleBackendKind::Flight {
                compression: crate::shuffle::ShuffleCompression::None,
                connection_count: std::num::NonZeroUsize::MIN,
                initial_stream_window_size: None,
                initial_connection_window_size: None,
            },
        )
    }

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
            metrics: global_metrics(),
            session_id: Some(self.session_id.clone()),
            job_id: Some(job_id),
            stage: None,
            attempt: None,
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
    driver: DriverHandle,
    shuffle_backend: ShuffleBackendKind,
}

impl ClusterJobRunner {
    pub fn new(
        system: &mut ActorSystem,
        options: DriverOptions,
        components: DriverComponents,
    ) -> Self {
        let shuffle_backend = options.shuffle_backend.clone();
        let driver = DriverHandle::new(system.spawn::<DriverActor>((options, components)));
        Self {
            driver,
            shuffle_backend,
        }
    }

    pub fn driver(&self) -> DriverHandle {
        self.driver.clone()
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(plan, self.shuffle_backend.clone())
    }

    /// Executes a plan on the cluster. This is where the cool stuff happens.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if !has_unsampled_range_repartition(&plan)? {
            return execute_job(&self.driver, plan, ctx.task_ctx()).await;
        }
        // Range repartitioning needs its split points before the stages are planned.
        // The sample jobs run when the stream is first polled, so they belong to the
        // operation that consumes the stream, and dropping the stream cancels them.
        let driver = self.driver.clone();
        let context = ctx.task_ctx();
        let schema = plan.schema();
        let output = futures::stream::once(async move {
            let plan =
                sample_range_repartitions(driver.clone(), Arc::clone(&context), plan).await?;
            execute_job(&driver, plan, context).await
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }

    async fn stop(&self) {
        let _ = self.driver.shutdown_and_wait().await;
    }
}

async fn execute_job(
    driver: &DriverHandle,
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let (tx, rx) = oneshot::channel();
    driver
        .send(DriverMessage::ExecuteJob {
            plan,
            context,
            result: tx,
        })
        .await
        .map_err(|e| internal_datafusion_err!("{e}"))?;
    rx.await
        .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
        .map_err(|e| internal_datafusion_err!("{e}"))
}

fn has_unsampled_range_repartition(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    plan.exists(|node| {
        Ok(node
            .downcast_ref::<RangeRepartitionExec>()
            .is_some_and(|node| !node.is_sampled()))
    })
}

/// Sets the split points of every range repartitioning in the plan, from the bottom up.
/// Each sample runs as a separate job on the cluster. A sample plan can contain range
/// repartitioning of its own, which is sampled first.
fn sample_range_repartitions(
    driver: DriverHandle,
    context: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
) -> BoxFuture<'static, Result<Arc<dyn ExecutionPlan>>> {
    async move {
        let mut children = vec![];
        for child in plan.children() {
            children.push(
                sample_range_repartitions(driver.clone(), Arc::clone(&context), Arc::clone(child))
                    .await?,
            );
        }
        let plan = replace_children_if_necessary(plan, children)?;
        let Some(node) = plan.downcast_ref::<RangeRepartitionExec>() else {
            return Ok(plan);
        };
        if node.is_sampled() {
            return Ok(plan);
        }
        let sample = sample_range_repartitions(
            driver.clone(),
            Arc::clone(&context),
            Arc::clone(node.sample()),
        )
        .await?;
        let batches = execute_job(&driver, sample, context)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(Arc::new(node.with_sample(&batches)?) as Arc<dyn ExecutionPlan>)
    }
    .boxed()
}
