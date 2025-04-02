use std::any::Any;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_err, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{AvroExec, CsvExec, NdJsonExec, ParquetExec};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, LexRequirement};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::insert::DataSinkExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::plan::{ShuffleReadExec, ShuffleWriteExec};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum RuntimeKind {
    Default,
    Secondary,
}

impl Display for RuntimeKind {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            RuntimeKind::Default => write!(f, "default"),
            RuntimeKind::Secondary => write!(f, "secondary"),
        }
    }
}

#[derive(Debug, Default)]
pub struct RuntimeExtension {
    handle: Option<Handle>,
}

impl RuntimeExtension {
    pub fn new(handle: Handle) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn get_secondary_runtime(ctx: &SessionContext) -> Option<Handle> {
        ctx.state_ref()
            .read()
            .config()
            .get_extension::<RuntimeExtension>()
            .and_then(|x| x.handle.clone())
    }

    fn build_runtime_aware_plan(
        plan: Arc<dyn ExecutionPlan>,
        handle: Handle,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform(|plan| {
            let any = plan.as_any();
            if any.is::<ShuffleReadExec>()
                || any.is::<ShuffleWriteExec>()
                || any.is::<ParquetExec>()
                || any.is::<NdJsonExec>()
                || any.is::<AvroExec>()
                || any.is::<CsvExec>()
                || any.is::<DataSourceExec>()
                || any.is::<DataSinkExec>()
            {
                let children = plan
                    .children()
                    .into_iter()
                    .map(|child| {
                        if child.as_any().is::<RuntimeAwareExec>() {
                            child.clone()
                        } else {
                            Arc::new(RuntimeAwareExec::new(
                                child.clone(),
                                Handle::current(),
                                RuntimeKind::Default,
                            ))
                        }
                    })
                    .collect();
                Ok(Transformed::yes(Arc::new(RuntimeAwareExec::new(
                    plan.with_new_children(children)?,
                    handle.clone(),
                    RuntimeKind::Secondary,
                ))))
            } else {
                Ok(Transformed::no(plan))
            }
        });
        result.data()
    }

    fn simplify_runtime_aware_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform(|plan| {
            if let Some(exec) = plan.as_any().downcast_ref::<RuntimeAwareExec>() {
                let children = exec
                    .input
                    .children()
                    .into_iter()
                    .map(|child| {
                        child
                            .as_any()
                            .downcast_ref::<RuntimeAwareExec>()
                            .and_then(|x| (x.runtime == exec.runtime).then(|| x.input.clone()))
                            .unwrap_or(child.clone())
                    })
                    .collect();
                let input = exec.input.clone().with_new_children(children)?;
                Ok(Transformed::yes(plan.with_new_children(vec![input])?))
            } else {
                Ok(Transformed::no(plan))
            }
        });
        result.data()
    }

    pub fn rewrite_runtime_aware_plan(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(handle) = Self::get_secondary_runtime(ctx) else {
            return Ok(plan);
        };
        let plan = Self::build_runtime_aware_plan(plan, handle)?;
        let plan = Self::simplify_runtime_aware_plan(plan)?;
        Ok(plan)
    }
}

#[derive(Debug)]
struct RuntimeAwareExec {
    input: Arc<dyn ExecutionPlan>,
    handle: Handle,
    runtime: RuntimeKind,
}

impl RuntimeAwareExec {
    fn new(input: Arc<dyn ExecutionPlan>, handle: Handle, runtime: RuntimeKind) -> Self {
        Self {
            input,
            handle,
            runtime,
        }
    }

    fn with_input(&self, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            handle: self.handle.clone(),
            runtime: self.runtime,
        }
    }
}

impl DisplayAs for RuntimeAwareExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RuntimeAwareExec: runtime={}", self.runtime)
    }
}

impl ExecutionPlan for RuntimeAwareExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        self.input.check_invariants(check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        self.input.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.input.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.input.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let first = children.pop();
        match (first, children.is_empty()) {
            (Some(first), true) => Ok(Arc::new(self.with_input(first))),
            _ => plan_err!("RuntimeAwareExec should have one child"),
        }
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .input
            .repartitioned(target_partitions, config)?
            .map(|x| -> Arc<dyn ExecutionPlan> { Arc::new(self.with_input(x)) }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = mpsc::channel(1);
        let inner = self.input.clone();
        let schema = inner.schema();
        let task = async move {
            // The `execute()` method is not async, but we call it inside the async task
            // in case its implementation requires access to the Tokio runtime.
            let mut stream = match inner.execute(partition, context) {
                Ok(stream) => stream,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            while let Some(x) = stream.next().await {
                if tx.send(x).await.is_err() {
                    break;
                }
            }
        };
        let mut join_set = JoinSet::new();
        join_set.spawn_on(task, &self.handle);
        let stream = RuntimeAwareStream {
            rx,
            schema: schema.clone(),
            join_set,
        };
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.input
            .with_fetch(limit)
            .map(|x| -> Arc<dyn ExecutionPlan> { Arc::new(self.with_input(x)) })
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.input.cardinality_effect()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .input
            .try_swapping_with_projection(projection)?
            .map(|x| -> Arc<dyn ExecutionPlan> { Arc::new(self.with_input(x)) }))
    }
}

struct RuntimeAwareStream {
    rx: mpsc::Receiver<Result<RecordBatch>>,
    schema: SchemaRef,
    /// The join set stores the handle to the task for the stream.
    /// The task is aborted when the stream is dropped.
    #[expect(unused)]
    join_set: JoinSet<()>,
}

impl Stream for RuntimeAwareStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl RecordBatchStream for RuntimeAwareStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
