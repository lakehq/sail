use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::execution_plan::{Boundedness, CardinalityEffect, EmissionType};
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

use crate::range_sample::range_split_points;

type SharedResult<T> = std::result::Result<T, Arc<DataFusionError>>;

/// Range repartitioning whose split points come from a sample of the input,
/// like the shuffle exchange of Spark's `RangePartitioner`.
///
/// The `sample` plan computes the input again and emits the sampled range keys
/// (see `RangePartitionSampleExec`). It runs when the node is first executed,
/// not when the query is planned, so EXPLAIN does not run it, and it runs
/// as part of the operation that executes the query.
///
/// The node always has `num_partitions` output partitions. When the sample
/// gives fewer split points, the last partitions are empty.
///
/// For distributed execution, the job runner runs the sample as a separate job
/// and sets the split points with [`Self::with_sample`] before it plans the stages.
/// <https://github.com/apache/spark/blob/v4.1.0/core/src/main/scala/org/apache/spark/Partitioner.scala>
#[derive(Debug)]
pub struct RangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    sample: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    num_partitions: usize,
    split_points: Option<Vec<SplitPoint>>,
    properties: Arc<PlanProperties>,
    /// The repartitioning of the input, created once for all output partitions.
    state: Arc<OnceCell<SharedResult<Arc<RepartitionExec>>>>,
}

impl RangeRepartitionExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        sample: Arc<dyn ExecutionPlan>,
        ordering: LexOrdering,
        num_partitions: usize,
    ) -> Result<Self> {
        Self::try_new_with_split_points(input, sample, ordering, num_partitions, None)
    }

    fn try_new_with_split_points(
        input: Arc<dyn ExecutionPlan>,
        sample: Arc<dyn ExecutionPlan>,
        ordering: LexOrdering,
        num_partitions: usize,
        split_points: Option<Vec<SplitPoint>>,
    ) -> Result<Self> {
        if num_partitions == 0 {
            return internal_err!("range repartitioning requires at least one partition");
        }
        if let Some(split_points) = &split_points
            && split_points.len() >= num_partitions
        {
            return internal_err!(
                "range repartitioning into {num_partitions} partitions cannot have {} split points",
                split_points.len()
            );
        }
        let mut equivalence = input.equivalence_properties().clone();
        equivalence.clear_orderings();
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            sample,
            ordering,
            num_partitions,
            split_points,
            properties,
            state: Arc::new(OnceCell::new()),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sample(&self) -> &Arc<dyn ExecutionPlan> {
        &self.sample
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Returns whether the split points are known.
    pub fn is_sampled(&self) -> bool {
        self.split_points.is_some()
    }

    /// Returns the range partitioning once the split points are known.
    pub fn range_partitioning(&self) -> Result<Option<RangePartitioning>> {
        self.split_points
            .as_ref()
            .map(|split_points| {
                RangePartitioning::try_new(self.ordering.clone(), split_points.clone())
            })
            .transpose()
    }

    /// Returns the node with the split points picked from the output of the sample plan.
    pub fn with_sample(&self, batches: &[RecordBatch]) -> Result<Self> {
        let options = self.ordering.iter().map(|x| x.options).collect::<Vec<_>>();
        let split_points = range_split_points(batches, &options, self.num_partitions)?;
        Self::try_new_with_split_points(
            Arc::clone(&self.input),
            Arc::clone(&self.sample),
            self.ordering.clone(),
            self.num_partitions,
            Some(split_points),
        )
    }
}

impl DisplayAs for RangeRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangeRepartitionExec: ordering=[{}], partitions={}, split_points=",
            self.ordering, self.num_partitions
        )?;
        match &self.split_points {
            Some(split_points) => {
                let split_points = split_points
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                write!(f, "[{}]", split_points.join(", "))
            }
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
        Ok(Arc::new(Self::try_new_with_split_points(
            Arc::clone(input),
            Arc::clone(&self.sample),
            self.ordering.clone(),
            self.num_partitions,
            self.split_points.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.num_partitions {
            return internal_err!(
                "{} has {} partitions but partition {partition} was requested",
                self.name(),
                self.num_partitions
            );
        }
        let state = Arc::clone(&self.state);
        let input = Arc::clone(&self.input);
        let sample = Arc::clone(&self.sample);
        let ordering = self.ordering.clone();
        let num_partitions = self.num_partitions;
        let split_points = self.split_points.clone();
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let output = futures::stream::once(async move {
            // The first partition to run samples the input. The others wait for it,
            // and a failed sample fails them all instead of running it again.
            let repartition = state
                .get_or_init(|| async {
                    repartition_by_sample(
                        input,
                        sample,
                        ordering,
                        num_partitions,
                        split_points,
                        Arc::clone(&context),
                    )
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
        Ok(Some(Arc::new(Self::try_new_with_split_points(
            make_with_child(projection, self.input())?,
            Arc::clone(&self.sample),
            ordering,
            self.num_partitions,
            self.split_points.clone(),
        )?)))
    }
}

/// Repartitions the input by the given split points, or by split points picked
/// from the output of the sample plan.
async fn repartition_by_sample(
    input: Arc<dyn ExecutionPlan>,
    sample: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    num_partitions: usize,
    split_points: Option<Vec<SplitPoint>>,
    context: Arc<TaskContext>,
) -> Result<Arc<RepartitionExec>> {
    let split_points = match split_points {
        Some(split_points) => split_points,
        None => {
            let batches = collect(sample, context).await?;
            let options = ordering.iter().map(|x| x.options).collect::<Vec<_>>();
            range_split_points(&batches, &options, num_partitions)?
        }
    };
    let partitioning = RangePartitioning::try_new(ordering, split_points)?;
    Ok(Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Range(partitioning),
    )?))
}
