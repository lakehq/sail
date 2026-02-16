use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, internal_err, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
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
use crate::plan::CacheReadExec;

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

    /// Submits a cache materialization job to the driver and waits for completion.
    async fn execute_cache_job(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        cache_id: String,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteCacheJob {
                plan,
                context: ctx.task_ctx(),
                cache_id,
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to execute cache job: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
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
        let cache_ids = collect_cache_read_ids(&plan);
        if !cache_ids.is_empty() {
            if let Ok(cache) = ctx.extension::<CacheManager>() {
                for cache_id in cache_ids {
                    if let Some(entry) = cache.find_by_id(&cache_id) {
                        if !entry.materialized {
                            let physical = ctx.state().create_physical_plan(&entry.plan).await?;
                            self.execute_cache_job(ctx, physical, cache_id.clone())
                                .await?;
                            cache.mark_materialized(&cache_id);
                        }
                    }
                }
            }
        }

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
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
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
fn collect_cache_read_ids(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut ids = Vec::new();
    let _ = plan.apply(|node: &Arc<dyn ExecutionPlan>| {
        if let Some(cache_read) = node.as_any().downcast_ref::<CacheReadExec>() {
            let id = cache_read.cache_id().to_string();
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    ids
}
