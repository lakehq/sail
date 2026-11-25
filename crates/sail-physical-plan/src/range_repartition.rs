// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, reset_plan_states,
};
use datafusion::physical_plan::projection::{
    ProjectionExec, all_columns, make_with_child, update_expr,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, apply_expression_roots, execute_stream, replace_children_if_necessary,
};
use datafusion_common::{DataFusionError, Result, Statistics, internal_err};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use tokio::sync::OnceCell;

use crate::range_sample::{
    RangePartitionResampleExec, RangePartitionSampleExec, RangeSketch, RangeSplit,
};
use crate::range_split::RangeSampleOptions;
use crate::shared_cte::{SharedCteExec, SharedCteScanExec};

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
    /// A copy of the keys plan of the sample, which the resample computes again. It is
    /// planned with the sample, so that the readers of the shared CTEs that it reads are
    /// planned too (see [`crate::shared_cte::share_ctes`]).
    resample_input: Arc<dyn ExecutionPlan>,
    /// The total number of rows the sample should have.
    sample_size: f64,
    sort_options: Vec<SortOptions>,
    num_partitions: usize,
    split: OnceCell<SharedResult<RangeSplit>>,
}

impl RangeSample {
    pub fn try_new(
        plan: Arc<RangePartitionSampleExec>,
        sample_size: f64,
        sort_options: Vec<SortOptions>,
        num_partitions: usize,
    ) -> Result<Self> {
        let resample_input = reset_plan_states(Arc::clone(plan.input()))?;
        Ok(Self {
            plan,
            resample_input,
            sample_size,
            sort_options,
            num_partitions,
            split: OnceCell::new(),
        })
    }

    /// The copy of the keys plan that the resample computes again.
    pub fn resample_input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.resample_input
    }

    /// The sample with each of its plans rewritten, which is not run yet.
    fn map_plans(
        &self,
        f: &mut impl FnMut(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        let keys = f(Arc::clone(self.plan.input()))?;
        Ok(Self {
            plan: Arc::new(RangePartitionSampleExec::try_new(
                keys,
                self.plan.sample_size(),
            )?),
            resample_input: f(Arc::clone(&self.resample_input))?,
            sample_size: self.sample_size,
            sort_options: self.sort_options.clone(),
            num_partitions: self.num_partitions,
            split: OnceCell::new(),
        })
    }

    /// The plan of the first sample.
    pub fn plan(&self) -> Arc<dyn ExecutionPlan> {
        Arc::clone(&self.plan) as Arc<dyn ExecutionPlan>
    }

    /// The candidates for split points from the output of the first sample.
    pub async fn sketch(
        &self,
        sample: SendableRecordBatchStream,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<RangeSketch> {
        RangeSketch::try_new(
            sample,
            self.sort_options.len(),
            self.sample_size,
            self.plan.sample_size(),
            pool,
        )
        .await
    }

    /// The plan that samples the imbalanced partitions of the sketch again, if any.
    /// It computes the keys again, so it runs on a copy of the keys plan.
    pub fn resample_plan(&self, sketch: &RangeSketch) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if sketch.imbalanced_partitions().is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(RangePartitionResampleExec::try_new(
            reset_plan_states(Arc::clone(&self.resample_input))?,
            sketch.fraction(),
            sketch.imbalanced_partitions().to_vec(),
        )?)))
    }

    /// How a job picks the split points from the sample (see
    /// [`crate::range_split::RangeSplitPointsExec`]).
    pub fn options(&self) -> RangeSampleOptions {
        RangeSampleOptions {
            sort_options: self.sort_options.clone(),
            num_partitions: self.num_partitions,
            sample_size: self.sample_size,
            partition_sample_size: self.plan.sample_size(),
        }
    }

    fn get(&self) -> Option<Result<&RangeSplit>> {
        self.split.get().map(|result| match result {
            Ok(split) => Ok(split),
            Err(error) => Err(DataFusionError::Shared(Arc::clone(error))),
        })
    }

    /// Runs the sample in this process once, and returns the split points.
    /// A failed sample fails every later call instead of running again.
    async fn get_or_run(&self, context: Arc<TaskContext>) -> Result<RangeSplit> {
        self.split
            .get_or_init(|| async { self.run(context).await.map_err(Arc::new) })
            .await
            .clone()
            .map_err(DataFusionError::Shared)
    }

    async fn run(&self, context: Arc<TaskContext>) -> Result<RangeSplit> {
        let pool = context.memory_pool();
        let sample = execute_stream(self.plan(), Arc::clone(&context))?;
        let mut sketch = self.sketch(sample, pool).await?;
        match self.resample_plan(&sketch)? {
            Some(resample) => {
                sketch
                    .add_resample(execute_stream(resample, Arc::clone(&context))?)
                    .await?;
            }
            // The shared CTEs keep no rows for the resample that does not run.
            None => {
                self.resample_input.apply(|node| {
                    if let Some(scan) = node.downcast_ref::<SharedCteScanExec>() {
                        scan.skip()?;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
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
/// Spark's `RangePartitioner` has one partition per bound plus one, which can be fewer
/// than the partitions asked for, for an input with few keys. The node has as many
/// partitions once its sample has run before the plan is executed (see
/// [`resolve_range_partitions`]). Until then, it has the partitions asked for, and the
/// ones that the sample does not give are empty.
///
/// In a job, the sample runs in stages of the same job as the shuffle (see
/// [`RangeSample::split_points_exec`]).
/// <https://github.com/apache/spark/blob/v4.1.0/core/src/main/scala/org/apache/spark/Partitioner.scala>
#[derive(Debug)]
pub struct RangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    sample: Arc<RangeSample>,
    ordering: LexOrdering,
    /// The number of output partitions.
    partitions: usize,
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
        let partitions = sample.num_partitions;
        Self::try_new_with_partitions(input, sample, ordering, partitions)
    }

    fn try_new_with_partitions(
        input: Arc<dyn ExecutionPlan>,
        sample: Arc<RangeSample>,
        ordering: LexOrdering,
        partitions: usize,
    ) -> Result<Self> {
        if partitions == 0 || partitions > sample.num_partitions {
            return internal_err!(
                "range repartitioning into {} partitions cannot have {partitions} partitions",
                sample.num_partitions
            );
        }
        if ordering
            .iter()
            .map(|x| x.options)
            .ne(sample.sort_options.iter().copied())
        {
            return internal_err!("range repartitioning ordering does not match its sample");
        }
        // An output partition gets rows from every input partition, so it keeps
        // neither the input order nor a value that is constant only per input partition.
        let mut equivalence = input.equivalence_properties().clone();
        equivalence.clear_orderings();
        if input.output_partitioning().partition_count() > 1 {
            equivalence.clear_per_partition_constants();
        }
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            sample,
            ordering,
            partitions,
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
        self.partitions
    }

    pub fn ordering(&self) -> &LexOrdering {
        &self.ordering
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
            Some(Ok(split)) => {
                let split_points = split
                    .split_points()
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
        Ok(Arc::new(Self::try_new_with_partitions(
            Arc::clone(input),
            Arc::clone(&self.sample),
            self.ordering.clone(),
            self.partitions,
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
        Ok(Some(Arc::new(Self::try_new_with_partitions(
            make_with_child(projection, self.input())?,
            Arc::clone(&self.sample),
            ordering,
            self.partitions,
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
    let split = sample.get_or_run(context).await?;
    let partitioning = RangePartitioning::try_new(ordering, split.split_points().to_vec())?;
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Range(partitioning),
    )?))
}

/// Runs the sample of every range repartitioning in the plan, and gives each one the
/// partitions of Spark's `RangePartitioner`, before the plan is executed in this
/// process. The partitions of a plan cannot change once it runs.
///
/// The sample can read scalar subqueries, whose values a [`ScalarSubqueryExec`] above
/// the range repartitioning gives when it runs. So such a node runs its subqueries
/// here first, and is then left out, since it only passes its input through.
///
/// The sample of a range repartitioning that reads another one is planned with the
/// partitions asked for of the other one. Its empty partitions add no sampled keys, but
/// the rows that each partition keeps are counted for the partitions asked for.
pub async fn resolve_range_partitions(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    resolve(plan, context).await
}

fn resolve(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> BoxFuture<'static, Result<Arc<dyn ExecutionPlan>>> {
    Box::pin(async move {
        if !plan.exists(|node| Ok(node.is::<RangeRepartitionExec>()))? {
            return Ok(plan);
        }
        if let Some(scalar) = plan.downcast_ref::<ScalarSubqueryExec>() {
            let mut subqueries = vec![];
            for link in scalar.subqueries() {
                subqueries.push(ScalarSubqueryLink {
                    plan: resolve(Arc::clone(&link.plan), Arc::clone(&context)).await?,
                    index: link.index,
                });
            }
            // The node runs the subqueries before its input, so this runs only them.
            let input = Arc::clone(scalar.input());
            let subqueries = ScalarSubqueryExec::new(
                Arc::new(EmptyExec::new(input.schema())),
                subqueries,
                scalar.results().clone(),
            );
            subqueries
                .execute(0, Arc::clone(&context))?
                .try_collect::<Vec<_>>()
                .await?;
            return resolve(input, context).await;
        }
        // A sample can read the shared CTEs of the node, before the node is executed.
        if let Some(shared) = plan.downcast_ref::<SharedCteExec>() {
            shared.produce(&context)?;
        }
        let mut children = vec![];
        for child in plan.children() {
            children.push(resolve(Arc::clone(child), Arc::clone(&context)).await?);
        }
        let plan = replace_children_if_necessary(plan, children)?;
        let Some(range) = plan.downcast_ref::<RangeRepartitionExec>() else {
            return Ok(plan);
        };
        let partitions = range.sample.get_or_run(context).await?.partitions();
        if partitions == range.partitions {
            return Ok(plan);
        }
        Ok(Arc::new(RangeRepartitionExec::try_new_with_partitions(
            Arc::clone(&range.input),
            Arc::clone(&range.sample),
            range.ordering.clone(),
            partitions,
        )?) as Arc<dyn ExecutionPlan>)
    })
}

/// Calls the function for every node of the plan and of the plans of the samples of its
/// range repartitionings, which are not children of the repartitionings. Each sample is
/// visited once, since the copies of a range repartitioning share it and it runs once.
pub fn apply_with_samples(
    plan: &Arc<dyn ExecutionPlan>,
    f: &mut impl FnMut(&Arc<dyn ExecutionPlan>) -> Result<()>,
) -> Result<()> {
    fn visit(
        plan: &Arc<dyn ExecutionPlan>,
        f: &mut impl FnMut(&Arc<dyn ExecutionPlan>) -> Result<()>,
        seen: &mut HashSet<*const RangeSample>,
    ) -> Result<()> {
        plan.apply(|node| {
            f(node)?;
            if let Some(range) = node.downcast_ref::<RangeRepartitionExec>()
                && seen.insert(Arc::as_ptr(&range.sample))
            {
                visit(range.sample.plan.input(), f, seen)?;
                visit(&range.sample.resample_input, f, seen)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
    visit(plan, f, &mut HashSet::new())
}

/// Rewrites the plan and the plans of the samples of its range repartitionings from the
/// leaves up, like [`apply_with_samples`] visits them. The copies of a range
/// repartitioning keep sharing one sample.
pub fn transform_up_with_samples(
    plan: Arc<dyn ExecutionPlan>,
    f: &mut impl FnMut(Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    /// The rewritten sample of each sample, which is kept so that its address is not reused.
    type Samples = HashMap<*const RangeSample, (Arc<RangeSample>, Arc<RangeSample>)>;

    fn rewrite(
        plan: Arc<dyn ExecutionPlan>,
        f: &mut impl FnMut(Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>>,
        samples: &mut Samples,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(range) = node.downcast_ref::<RangeRepartitionExec>() else {
                return f(node);
            };
            let key = Arc::as_ptr(&range.sample);
            let sample = match samples.get(&key) {
                Some((_, sample)) => Arc::clone(sample),
                None => {
                    let sample = Arc::new(
                        range
                            .sample
                            .map_plans(&mut |plan| rewrite(plan, f, samples))?,
                    );
                    samples.insert(key, (Arc::clone(&range.sample), Arc::clone(&sample)));
                    sample
                }
            };
            let node = Arc::new(RangeRepartitionExec::try_new_with_partitions(
                Arc::clone(&range.input),
                sample,
                range.ordering.clone(),
                range.partitions,
            )?) as Arc<dyn ExecutionPlan>;
            Ok(Transformed::yes(f(node)?.data))
        })
        .data()
    }
    rewrite(plan, f, &mut HashMap::new())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{AsArray, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{AcrossPartitions, ConstExpr, EquivalenceProperties};
    use datafusion::physical_plan::collect_partitioned;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::shared_cte::{SharedCte, share_ctes};

    /// A leaf whose column `k` is constant within each of its partitions only.
    #[derive(Debug)]
    struct PartitionConstantExec(Arc<PlanProperties>);

    impl PartitionConstantExec {
        fn new(partitions: usize) -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
            let mut equivalence = EquivalenceProperties::new(Arc::clone(&schema));
            equivalence
                .add_constants([ConstExpr::new(
                    Arc::new(Column::new("k", 0)),
                    AcrossPartitions::Heterogeneous,
                )])
                .unwrap();
            Self(Arc::new(PlanProperties::new(
                equivalence,
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )))
        }
    }

    impl DisplayAs for PartitionConstantExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "PartitionConstantExec")
        }
    }

    impl ExecutionPlan for PartitionConstantExec {
        fn name(&self) -> &str {
            "PartitionConstantExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.0
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            internal_err!("PartitionConstantExec is not executed")
        }
    }

    fn has_constants(input_partitions: usize) -> bool {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PartitionConstantExec::new(input_partitions));
        let options = SortOptions::default();
        let sample = Arc::new(
            RangeSample::try_new(
                Arc::new(RangePartitionSampleExec::try_new(Arc::clone(&input), 1).unwrap()),
                40.0,
                vec![options],
                2,
            )
            .unwrap(),
        );
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 0)),
            options,
        )])
        .unwrap();
        assert!(!input.equivalence_properties().constants().is_empty());
        let node = RangeRepartitionExec::try_new(input, sample, ordering).unwrap();
        !node
            .properties()
            .equivalence_properties()
            .constants()
            .is_empty()
    }

    #[test]
    fn test_per_partition_constants_are_dropped_for_many_input_partitions() {
        // Each output partition gets rows from every input partition.
        assert!(!has_constants(3));
        // With one input partition, a constant stays constant in every output partition.
        assert!(has_constants(1));
    }

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
        let sample = Arc::new(
            RangeSample::try_new(
                Arc::new(RangePartitionSampleExec::try_new(Arc::clone(&input) as _, 1).unwrap()),
                40.0,
                vec![options],
                2,
            )
            .unwrap(),
        );
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

    /// The rows of each partition of a range repartitioning into 8 partitions by `k`,
    /// once the partitions come from its sample.
    async fn resolved_partition_rows(keys: Vec<i64>) -> Vec<usize> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(keys))])
                .unwrap();
        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let options = SortOptions::default();
        let sample = Arc::new(
            RangeSample::try_new(
                Arc::new(RangePartitionSampleExec::try_new(Arc::clone(&input) as _, 100).unwrap()),
                800.0,
                vec![options],
                8,
            )
            .unwrap(),
        );
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 0)),
            options,
        )])
        .unwrap();
        let node: Arc<dyn ExecutionPlan> =
            Arc::new(RangeRepartitionExec::try_new(input, sample, ordering).unwrap());
        assert_eq!(node.output_partitioning().partition_count(), 8);
        let context = SessionContext::new().task_ctx();
        let node = resolve_range_partitions(node, Arc::clone(&context))
            .await
            .unwrap();
        collect_partitioned(node, context)
            .await
            .unwrap()
            .iter()
            .map(|batches| batches.iter().map(|batch| batch.num_rows()).sum())
            .collect()
    }

    /// A range repartitioning into 2 partitions by `k` of the only reference to a shared
    /// CTE, whose sample and resample read the CTE too.
    fn range_repartition_of_a_shared_cte(
        partitions: &[Vec<RecordBatch>],
        partition_sample_size: usize,
    ) -> (Arc<SharedCte>, Arc<dyn ExecutionPlan>) {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let producer: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(partitions, Arc::clone(&schema), None).unwrap();
        let cte = Arc::new(SharedCte::new(
            0,
            "c".to_string(),
            producer.output_partitioning().clone(),
        ));
        let statistics = Statistics::new_unknown(&schema);
        let scan: Arc<dyn ExecutionPlan> = Arc::new(SharedCteScanExec::new(
            Arc::clone(&cte),
            schema,
            &statistics,
            None,
        ));
        let options = SortOptions::default();
        let sample = Arc::new(
            RangeSample::try_new(
                Arc::new(
                    RangePartitionSampleExec::try_new(
                        reset_plan_states(Arc::clone(&scan)).unwrap(),
                        partition_sample_size,
                    )
                    .unwrap(),
                ),
                40.0,
                vec![options],
                2,
            )
            .unwrap(),
        );
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 0)),
            options,
        )])
        .unwrap();
        let node = Arc::new(RangeRepartitionExec::try_new(scan, sample, ordering).unwrap());
        let plan = share_ctes(node, vec![(Arc::clone(&cte), producer)]).unwrap();
        (cte, plan)
    }

    fn keys(range: std::ops::Range<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(range))]).unwrap()
    }

    #[tokio::test]
    async fn test_sample_and_resample_of_a_shared_cte_read_their_own_rows() {
        // The balanced input is sampled once, and the imbalanced one again.
        let mut imbalanced = vec![vec![]; 1_024];
        imbalanced[100] = vec![keys(0..100_000)];
        for (partitions, partition_sample_size, rows) in [
            (vec![vec![keys(0..500)], vec![keys(500..1_000)]], 100, 1_000),
            (imbalanced, 1, 100_000),
        ] {
            let (_, plan) = range_repartition_of_a_shared_cte(&partitions, partition_sample_size);
            let context = SessionContext::new().task_ctx();
            let plan = resolve_range_partitions(plan, Arc::clone(&context))
                .await
                .unwrap();
            let output = collect_partitioned(plan, context).await.unwrap();
            let mut values = output
                .iter()
                .flatten()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            values.sort_unstable();
            assert_eq!(values, (0..rows).collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn test_shared_cte_keeps_no_rows_for_a_resample_that_does_not_run() {
        // Each partition of 500 rows keeps 20 rows of the sample of 40 rows of 1,000, which
        // is fewer than the 100 rows that it samples, so the input is not sampled again.
        let (cte, plan) =
            range_repartition_of_a_shared_cte(&[vec![keys(0..500)], vec![keys(500..1_000)]], 100);
        let context = SessionContext::new().task_ctx();
        let plan = resolve_range_partitions(plan, Arc::clone(&context))
            .await
            .unwrap();
        // The first partition ends once the whole input is read, before the output of
        // the CTE is released.
        let mut stream = plan.execute(0, Arc::clone(&context)).unwrap();
        while stream.try_next().await.unwrap().is_some() {}
        assert_eq!(cte.kept(), Some(0));
    }

    #[tokio::test]
    async fn test_partitions_are_those_of_the_spark_bounds() {
        // Spark has no bounds for an empty input, so one partition.
        assert_eq!(resolved_partition_rows(vec![]).await, vec![0]);
        // A constant key is one bound, so two partitions, the last one empty.
        assert_eq!(resolved_partition_rows(vec![1; 24]).await, vec![24, 0]);
        // Enough distinct keys give all the partitions asked for.
        let rows = resolved_partition_rows((0..800).collect()).await;
        assert_eq!(rows.len(), 8);
        assert_eq!(rows.iter().sum::<usize>(), 800);
    }
}
