use std::any::Any;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::{exec_err, internal_err, plan_err, Result, Statistics};
use futures::{Stream, StreamExt};
pub use sail_logical_plan::repartition::ExplicitRepartitionKind;

/// A physical plan node for explicit repartitioning in the query.
/// This is a placeholder node that should be rewritten during physical optimization.
#[derive(Debug)]
pub struct ExplicitRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    kind: ExplicitRepartitionKind,
    properties: Arc<PlanProperties>,
}

impl ExplicitRepartitionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        kind: ExplicitRepartitionKind,
    ) -> Self {
        let mut eq_properties = input.equivalence_properties().clone();
        if input.output_partitioning().partition_count() > 1 {
            eq_properties.clear_orderings();
            eq_properties.clear_per_partition_constants();
        }
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                partitioning,
                input.pipeline_behavior(),
                input.boundedness(),
            )
            .with_scheduling_type(SchedulingType::Cooperative)
            .with_evaluation_type(EvaluationType::Eager),
        );
        Self {
            input,
            kind,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn kind(&self) -> ExplicitRepartitionKind {
        self.kind
    }
}

impl DisplayAs for ExplicitRepartitionExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}: kind={:?}", Self::static_name(), self.kind)
    }
}

impl ExecutionPlan for ExplicitRepartitionExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(ExplicitRepartitionExec::new(
            child,
            self.properties.partitioning.clone(),
            self.kind,
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!(
            "{} should be eliminated during physical optimization",
            self.name()
        )
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_none() {
            self.input.partition_statistics(None)
        } else {
            Ok(Statistics::new_unknown(&self.schema()))
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    // TODO: Implement the logic to push down filters or projections.
    //   The filters and projections are safe to push down if they are
    //   column references. For other expressions, we may not want to
    //   push them down since the evaluation can be potentially expensive,
    //   and the presence of explicit repartitioning indicates that the user
    //   wants to evaluate these expressions after repartitioning.
}

/// A narrow, non-shuffling coalesce from M input partitions to N output partitions.
///
/// Each output partition reads a contiguous group of input partitions. This matches the
/// important Spark `DataFrame.coalesce(n)` contract: reducing partitions does not shuffle
/// records, while requesting more partitions than the input has is a no-op.
#[derive(Debug)]
pub struct NarrowCoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
    properties: Arc<PlanProperties>,
}

impl NarrowCoalesceExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, target_partitions: usize) -> Result<Self> {
        if target_partitions == 0 {
            return plan_err!("number of coalesce partitions cannot be zero");
        }
        let input_partitions = input.output_partitioning().partition_count();
        let target_partitions = target_partitions.min(input_partitions);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(target_partitions),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            target_partitions,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn target_partitions(&self) -> usize {
        self.target_partitions
    }

    fn input_partition_range(&self, output_partition: usize) -> Range<usize> {
        let input_partitions = self.input.output_partitioning().partition_count();
        let start = output_partition * input_partitions / self.target_partitions;
        let end = (output_partition + 1) * input_partitions / self.target_partitions;
        start..end
    }
}

impl DisplayAs for NarrowCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "NarrowCoalesceExec: target_partitions={}",
            self.target_partitions
        )
    }
}

impl ExecutionPlan for NarrowCoalesceExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(Self::try_new(child, self.target_partitions)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.target_partitions {
            return exec_err!(
                "{}: partition index {} out of range ({})",
                self.name(),
                partition,
                self.target_partitions
            );
        }
        Ok(Box::pin(NarrowCoalesceStream::new(
            self.input.clone(),
            self.input_partition_range(partition),
            context,
            self.schema(),
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_none() {
            self.input.partition_statistics(None)
        } else {
            Ok(Statistics::new_unknown(&self.schema()))
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

struct NarrowCoalesceStream {
    input: Arc<dyn ExecutionPlan>,
    input_partitions: Range<usize>,
    context: Arc<TaskContext>,
    schema: SchemaRef,
    current: Option<SendableRecordBatchStream>,
}

impl NarrowCoalesceStream {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        input_partitions: Range<usize>,
        context: Arc<TaskContext>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            input_partitions,
            context,
            schema,
            current: None,
        }
    }

    fn poll_current(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            if self.current.is_none() {
                let Some(input_partition) = self.input_partitions.next() else {
                    return Poll::Ready(None);
                };
                match self.input.execute(input_partition, self.context.clone()) {
                    Ok(stream) => self.current = Some(stream),
                    Err(error) => return Poll::Ready(Some(Err(error))),
                }
            }
            let stream = self.current.as_mut().expect("stream is initialized");
            match stream.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(batch)) => return Poll::Ready(Some(batch)),
                Poll::Ready(None) => {
                    self.current = None;
                }
            }
        }
    }
}

impl Stream for NarrowCoalesceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_current(cx)
    }
}

impl RecordBatchStream for NarrowCoalesceStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
