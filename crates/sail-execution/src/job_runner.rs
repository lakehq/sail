use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, internal_err, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common_datafusion::cache_manager::CacheManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::{JobRunner, JobRunnerHistory};
use sail_common_datafusion::system::observable::{JobRunnerObserver, Observer, StateObservable};
use sail_server::actor::{ActorHandle, ActorSystem};
use sail_telemetry::telemetry::global_metric_registry;
use sail_telemetry::{trace_execution_plan, TracingExecOptions};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverEvent, DriverOptions};
use crate::plan::{CacheReadExec, CacheWriteExec};

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
impl StateObservable<JobRunnerObserver> for LocalJobRunner {
    async fn observe(&self, observer: JobRunnerObserver) {
        observer.nothing()
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
            stage: None,
            attempt: None,
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        Ok(execute_stream(plan, ctx.task_ctx())?)
    }

    async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>) {
        self.stopped.store(true, Ordering::Relaxed);
        let _ = history.send(JobRunnerHistory {
            jobs: vec![],
            stages: vec![],
            tasks: vec![],
            workers: vec![],
        });
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

    /// Submits a physical plan to the driver for execution and returns the result stream.
    async fn submit_job(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteJob {
                plan,
                context: ctx.task_ctx(),
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to execute job: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }

    /// Submits a cache materialization job through the normal execution path and waits for completion.
    async fn materialize_cache(
        &self,
        ctx: &SessionContext,
        cache_id: u64,
        plan: &LogicalPlan,
    ) -> Result<()> {
        let physical = ctx.state().create_physical_plan(plan).await?;
        let cache_plan = Arc::new(CacheWriteExec::new_stub(physical, cache_id));
        let mut stream = self.submit_job(ctx, cache_plan).await?;
        while let Some(batch) = stream.next().await {
            batch?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl StateObservable<JobRunnerObserver> for ClusterJobRunner {
    async fn observe(&self, observer: JobRunnerObserver) {
        let result = self
            .driver
            .send(DriverEvent::ObserveState { observer })
            .await;
        if let Err(SendError(DriverEvent::ObserveState { observer })) = result {
            observer.fail(internal_datafusion_err!(
                "failed to observe state for cluster job runner"
            ));
        }
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    /// Executes a plan on the cluster.
    ///
    /// Before running the main plan, checks for any CacheReadExec nodes that reference
    /// unmaterialized caches and submits cache materialization jobs for them first.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let cache_ids = collect_cache_node_ids(&plan);
        let cache = ctx.extension::<CacheManager>()?;

        for cache_id in cache_ids {
            if let Some(entry) = cache.find_by_id(cache_id) {
                if !entry.materialized {
                    self.materialize_cache(ctx, cache_id, &entry.plan).await?;
                    cache.mark_materialized(cache_id);
                }
            }
        }

        self.submit_job(ctx, plan).await
    }

    async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>) {
        let _ = self
            .driver
            .send(DriverEvent::Shutdown {
                history: Some(history),
            })
            .await;
    }
}

/// Walks a physical plan tree and collects cache IDs from any CacheReadExec nodes.
fn collect_cache_node_ids(plan: &Arc<dyn ExecutionPlan>) -> Vec<u64> {
    let mut ids = Vec::new();
    let _ = plan.apply(|node: &Arc<dyn ExecutionPlan>| {
        if let Some(cache_read) = node.as_any().downcast_ref::<CacheReadExec>() {
            let id = cache_read.cache_id();
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    ids
}
