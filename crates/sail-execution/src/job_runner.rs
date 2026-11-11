use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
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
            sample_range_repartitions(
                driver.clone(),
                Arc::clone(&context),
                Arc::clone(&plan),
                vec![],
            )
            .await?;
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

/// A range repartitioning and the scalar subqueries above it, outermost first.
struct RangeRepartition {
    node: Arc<dyn ExecutionPlan>,
    subqueries: Vec<Arc<dyn ExecutionPlan>>,
}

/// Collects the range repartitioning in the plan. Each node comes after every node that
/// must be sampled after it: after its ancestors, and after the nodes whose sample runs
/// a scalar subquery that contains it.
fn collect_range_repartitions(
    plan: &Arc<dyn ExecutionPlan>,
    subqueries: &mut Vec<Arc<dyn ExecutionPlan>>,
    output: &mut Vec<RangeRepartition>,
) {
    if plan.is::<RangeRepartitionExec>() {
        output.push(RangeRepartition {
            node: Arc::clone(plan),
            subqueries: subqueries.clone(),
        });
    }
    let Some(scalar) = plan.downcast_ref::<ScalarSubqueryExec>() else {
        for child in plan.children() {
            collect_range_repartitions(child, subqueries, output);
        }
        return;
    };
    // Only the input reads the results of the subqueries, so only its samples
    // compute them, as in the job graph.
    subqueries.push(Arc::clone(plan));
    collect_range_repartitions(scalar.input(), subqueries, output);
    subqueries.pop();
    // A subquery does not read the results of the scalar subqueries around it. The
    // samples of the input run the subqueries, so the subqueries come after the input
    // and are sampled first.
    for link in scalar.subqueries() {
        collect_range_repartitions(&link.plan, &mut vec![], output);
    }
}

/// Sets the split points of every range repartitioning in the plan, from the bottom up.
/// Each sample runs as a separate job on the cluster. A sample can contain range
/// repartitioning of its own, which is sampled first. Copies of a range repartitioning
/// share their sample, so each sample job runs once.
fn sample_range_repartitions(
    driver: DriverHandle,
    context: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    subqueries: Vec<Arc<dyn ExecutionPlan>>,
) -> BoxFuture<'static, Result<()>> {
    async move {
        let mut nodes = vec![];
        collect_range_repartitions(&plan, &mut subqueries.clone(), &mut nodes);
        // Reversing the order samples every input before the node that reads it.
        for RangeRepartition { node, subqueries } in nodes.into_iter().rev() {
            let Some(node) = node.downcast_ref::<RangeRepartitionExec>() else {
                continue;
            };
            if node.is_sampled() {
                continue;
            }
            let sample = node.sample();
            sample_range_repartitions(
                driver.clone(),
                Arc::clone(&context),
                sample.plan(),
                subqueries.clone(),
            )
            .await?;
            let batches = execute_sample_job(&driver, sample.plan(), &subqueries, &context).await?;
            let mut sketch = sample.sketch(&batches)?;
            if let Some(resample) = sample.resample_plan(&sketch)? {
                let batches = execute_sample_job(&driver, resample, &subqueries, &context).await?;
                sketch.add_resample(&batches)?;
            }
            sample.set(sketch)?;
        }
        Ok(())
    }
    .boxed()
}

/// Runs a sample as a job and collects its output. The sample computes a copy of the
/// input, so it reads the results of the scalar subqueries above the range
/// repartitioning. The job computes them too.
async fn execute_sample_job(
    driver: &DriverHandle,
    sample: Arc<dyn ExecutionPlan>,
    subqueries: &[Arc<dyn ExecutionPlan>],
    context: &Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let sample = subqueries.iter().rev().try_fold(sample, |plan, subquery| {
        let Some(subquery) = subquery.downcast_ref::<ScalarSubqueryExec>() else {
            return internal_err!("expected a scalar subquery plan");
        };
        Ok(Arc::new(ScalarSubqueryExec::new(
            plan,
            subquery.subqueries().to_vec(),
            subquery.results().clone(),
        )) as Arc<dyn ExecutionPlan>)
    })?;
    execute_job(driver, sample, Arc::clone(context))
        .await?
        .try_collect::<Vec<_>>()
        .await
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::scalar_subquery::ScalarSubqueryLink;
    use sail_physical_plan::range_repartition::RangeSample;
    use sail_physical_plan::range_sample::RangePartitionSampleExec;

    use super::*;

    fn range_repartition() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let options = SortOptions::default();
        let sample = Arc::new(RangeSample::new(
            Arc::new(RangePartitionSampleExec::try_new(Arc::clone(&input), 1).unwrap()),
            40.0,
            vec![options],
            2,
        ));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 0)),
            options,
        )])
        .unwrap();
        Arc::new(RangeRepartitionExec::try_new(input, sample, ordering).unwrap())
    }

    #[test]
    fn test_scalar_subquery_scope_covers_only_its_input() {
        let in_input = range_repartition();
        let in_subquery = range_repartition();
        let scalar: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
            Arc::clone(&in_input),
            vec![ScalarSubqueryLink {
                plan: Arc::clone(&in_subquery),
                index: SubqueryIndex::new(0),
            }],
            ScalarSubqueryResults::new(1),
        ));
        let mut nodes = vec![];
        collect_range_repartitions(&scalar, &mut vec![], &mut nodes);

        // The input reads the subquery results, so its sample runs the subquery. The
        // range repartitioning in the subquery comes last, so it is sampled first.
        let [input, subquery] = nodes.as_slice() else {
            unreachable!("expected two range repartitions, got {}", nodes.len());
        };
        assert!(Arc::ptr_eq(&input.node, &in_input));
        assert_eq!(input.subqueries.len(), 1);
        assert!(Arc::ptr_eq(&input.subqueries[0], &scalar));
        assert!(Arc::ptr_eq(&subquery.node, &in_subquery));
        assert!(subquery.subqueries.is_empty());
    }
}
