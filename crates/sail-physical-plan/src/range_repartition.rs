use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, reset_plan_states,
};
use datafusion::physical_plan::projection::{
    ProjectionExec, all_columns, make_with_child, update_expr,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, apply_expression_roots, collect,
};
use datafusion_common::{DataFusionError, Result, Statistics, internal_err};
use futures::TryStreamExt;
use tokio::sync::OnceCell;

use crate::range_sample::{RangePartitionResampleExec, RangePartitionSampleExec, RangeSketch};

type SharedResult<T> = std::result::Result<T, Arc<DataFusionError>>;

/// The sample of a range repartitioning and the split points picked from it.
///
/// Every copy of the same range repartitioning shares one `RangeSample`, so the
/// sample runs at most once. The copies include the nodes that the optimizer
/// rewrites and the copy of the node inside the sample of a later range
/// repartitioning, which samples a copy of its input. Without sharing, every nested
/// range repartitioning would double the number of samples.
#[derive(Debug)]
pub struct RangeSample {
    /// The plan that computes the input again and emits the sampled range keys.
    plan: Arc<RangePartitionSampleExec>,
    /// The total number of rows the sample should have.
    sample_size: f64,
    sort_options: Vec<SortOptions>,
    num_partitions: usize,
    split_points: OnceCell<SharedResult<Vec<SplitPoint>>>,
}

impl RangeSample {
    pub fn new(
        plan: Arc<RangePartitionSampleExec>,
        sample_size: f64,
        sort_options: Vec<SortOptions>,
        num_partitions: usize,
    ) -> Self {
        Self {
            plan,
            sample_size,
            sort_options,
            num_partitions,
            split_points: OnceCell::new(),
        }
    }

    /// The plan of the first sample.
    pub fn plan(&self) -> Arc<dyn ExecutionPlan> {
        Arc::clone(&self.plan) as Arc<dyn ExecutionPlan>
    }

    /// The candidates for split points from the output of the first sample.
    pub fn sketch(&self, batches: &[RecordBatch]) -> Result<RangeSketch> {
        RangeSketch::try_new(
            batches,
            self.sort_options.len(),
            self.sample_size,
            self.plan.sample_size(),
        )
    }

    /// The plan that samples the imbalanced partitions of the sketch again, if any.
    /// It computes the keys again, so it runs on a copy of the keys plan.
    pub fn resample_plan(&self, sketch: &RangeSketch) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if sketch.imbalanced_partitions().is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(RangePartitionResampleExec::try_new(
            reset_plan_states(Arc::clone(self.plan.input()))?,
            sketch.fraction(),
            sketch.imbalanced_partitions().to_vec(),
        )?)))
    }

    /// Picks the split points from the sketch, unless they are known.
    pub fn set(&self, sketch: RangeSketch) -> Result<()> {
        let split_points = sketch.split_points(&self.sort_options, self.num_partitions)?;
        // Another copy may have set the same split points first.
        let _ = self.split_points.set(Ok(split_points));
        Ok(())
    }

    fn is_set(&self) -> bool {
        self.split_points.initialized()
    }

    fn get(&self) -> Option<Result<&[SplitPoint]>> {
        self.split_points.get().map(|result| match result {
            Ok(split_points) => Ok(split_points.as_slice()),
            Err(error) => Err(DataFusionError::Shared(Arc::clone(error))),
        })
    }

    /// Runs the sample in this process once, and returns the split points.
    /// A failed sample fails every later call instead of running again.
    async fn get_or_run(&self, context: Arc<TaskContext>) -> Result<Vec<SplitPoint>> {
        self.split_points
            .get_or_init(|| async { self.run(context).await.map_err(Arc::new) })
            .await
            .clone()
            .map_err(DataFusionError::Shared)
    }

    async fn run(&self, context: Arc<TaskContext>) -> Result<Vec<SplitPoint>> {
        let batches = collect(self.plan(), Arc::clone(&context)).await?;
        let mut sketch = self.sketch(&batches)?;
        if let Some(resample) = self.resample_plan(&sketch)? {
            sketch.add_resample(&collect(resample, context).await?)?;
        }
        sketch.split_points(&self.sort_options, self.num_partitions)
    }
}

/// Range repartitioning whose split points come from a sample of the input,
/// like the shuffle exchange of Spark's `RangePartitioner`.
///
/// The sample runs when the node is first executed, not when the query is planned,
/// so EXPLAIN does not run it, and it runs as part of the operation that executes
/// the query.
///
/// The node always has `num_partitions` output partitions. When the sample
/// gives fewer split points, the last partitions are empty.
///
/// For distributed execution, the job runner runs the sample as a separate job
/// and sets the split points with [`RangeSample::set`] before it plans the stages.
/// <https://github.com/apache/spark/blob/v4.1.0/core/src/main/scala/org/apache/spark/Partitioner.scala>
#[derive(Debug)]
pub struct RangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    sample: Arc<RangeSample>,
    ordering: LexOrdering,
    properties: Arc<PlanProperties>,
    /// The repartitioning of the input, created once for all output partitions
    /// of this node. Each copy has its own, because a repartitioning runs only once.
    state: Arc<OnceCell<SharedResult<Arc<RepartitionExec>>>>,
}

impl RangeRepartitionExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        sample: Arc<RangeSample>,
        ordering: LexOrdering,
    ) -> Result<Self> {
        if sample.num_partitions == 0 {
            return internal_err!("range repartitioning requires at least one partition");
        }
        if ordering
            .iter()
            .map(|x| x.options)
            .ne(sample.sort_options.iter().copied())
        {
            return internal_err!("range repartitioning ordering does not match its sample");
        }
        let mut equivalence = input.equivalence_properties().clone();
        equivalence.clear_orderings();
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(sample.num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            sample,
            ordering,
            properties,
            state: Arc::new(OnceCell::new()),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sample(&self) -> &Arc<RangeSample> {
        &self.sample
    }

    pub fn num_partitions(&self) -> usize {
        self.sample.num_partitions
    }

    /// Returns whether the split points are known.
    pub fn is_sampled(&self) -> bool {
        self.sample.is_set()
    }

    /// Returns the range partitioning once the split points are known.
    pub fn range_partitioning(&self) -> Result<Option<RangePartitioning>> {
        self.sample
            .get()
            .map(|split_points| {
                RangePartitioning::try_new(self.ordering.clone(), split_points?.to_vec())
            })
            .transpose()
    }
}

impl DisplayAs for RangeRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangeRepartitionExec: ordering=[{}], partitions={}, split_points=",
            self.ordering,
            self.num_partitions()
        )?;
        match self.sample.get() {
            Some(Ok(split_points)) => {
                let split_points = split_points
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                write!(f, "[{}]", split_points.join(", "))
            }
            Some(Err(_)) => write!(f, "failed"),
            None => write!(f, "sampled at execution"),
        }
    }
}

impl ExecutionPlan for RangeRepartitionExec {
    fn name(&self) -> &str {
        "RangeRepartitionExec"
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

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.ordering.iter().map(|sort_expr| &sort_expr.expr), f)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("{} requires exactly one child", self.name());
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            Arc::clone(&self.sample),
            self.ordering.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.num_partitions() {
            return internal_err!(
                "{} has {} partitions but partition {partition} was requested",
                self.name(),
                self.num_partitions()
            );
        }
        let state = Arc::clone(&self.state);
        let input = Arc::clone(&self.input);
        let sample = Arc::clone(&self.sample);
        let ordering = self.ordering.clone();
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let output = futures::stream::once(async move {
            // The first partition to run creates the repartitioning. The others wait
            // for it, and a failure fails them all instead of running it again.
            let repartition = state
                .get_or_init(|| async {
                    repartition_by_sample(input, &sample, ordering, Arc::clone(&context))
                        .await
                        .map_err(Arc::new)
                })
                .await
                .clone()
                .map_err(DataFusionError::Shared)?;
            if partition < repartition.partitioning().partition_count() {
                repartition.execute(partition, context)
            } else {
                Ok(Box::pin(EmptyRecordBatchStream::new(schema)) as SendableRecordBatchStream)
            }
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            output,
        )))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![if partition.is_none() {
            ChildStats::At(None)
        } else {
            ChildStats::Skip
        }]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if args.partition().is_none() {
            Ok(Arc::clone(&input_stats[0]))
        } else {
            Ok(Arc::new(Statistics::new_unknown(&self.schema())))
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // No benefit if the projection does not narrow the schema.
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }
        if projection.benefits_from_input_partitioning()[0] || !all_columns(projection.expr()) {
            return Ok(None);
        }
        let mut sort_exprs = Vec::with_capacity(self.ordering.len());
        for sort_expr in self.ordering.iter() {
            let Some(expr) = update_expr(&sort_expr.expr, projection.expr(), false)? else {
                return Ok(None);
            };
            sort_exprs.push(PhysicalSortExpr::new(expr, sort_expr.options));
        }
        let Some(ordering) =
            LexOrdering::new(sort_exprs).filter(|x| x.len() == self.ordering.len())
        else {
            return Ok(None);
        };
        // The sample plan computes the keys from its own input, so it stays the same.
        Ok(Some(Arc::new(Self::try_new(
            make_with_child(projection, self.input())?,
            Arc::clone(&self.sample),
            ordering,
        )?)))
    }
}

/// Repartitions the input by the split points of the sample, which runs here
/// unless a copy of the node has run it already.
async fn repartition_by_sample(
    input: Arc<dyn ExecutionPlan>,
    sample: &RangeSample,
    ordering: LexOrdering,
    context: Arc<TaskContext>,
) -> Result<Arc<RepartitionExec>> {
    let split_points = sample.get_or_run(context).await?;
    let partitioning = RangePartitioning::try_new(ordering, split_points)?;
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Range(partitioning),
    )?))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{AsArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::collect_partitioned;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_imbalanced_input_is_sampled_again() {
        // Spark's defaults for two partitions: 20 sample points per partition give a
        // sample of 40 rows, so each of the 1,024 input partitions keeps 1 row. Only one
        // input partition has rows, and one row gives no split point.
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mut partitions = vec![vec![]; 1_024];
        partitions[100] = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(0..100_000))],
            )
            .unwrap(),
        ];
        let input = TestMemoryExec::try_new_exec(&partitions, Arc::clone(&schema), None).unwrap();
        let options = SortOptions::default();
        let sample = Arc::new(RangeSample::new(
            Arc::new(RangePartitionSampleExec::try_new(Arc::clone(&input) as _, 1).unwrap()),
            40.0,
            vec![options],
            2,
        ));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 0)),
            options,
        )])
        .unwrap();
        let node = Arc::new(RangeRepartitionExec::try_new(input, sample, ordering).unwrap());

        let output = collect_partitioned(node, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let rows = output
            .iter()
            .map(|batches| {
                batches
                    .iter()
                    .map(|batch| batch.column(0).as_primitive::<Int64Type>().len())
                    .sum::<usize>()
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.iter().sum::<usize>(), 100_000);
        // The sample of about 40 keys splits the rows near the middle.
        assert!(rows.iter().all(|&n| n > 30_000), "{rows:?}");
    }
}
