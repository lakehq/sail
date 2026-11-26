// SPDX-License-Identifier: Apache-2.0

//! The split points of a range repartitioning as data, for distributed execution.
//!
//! In a job, the sample of a range repartitioning runs as stages of the same job as
//! the shuffle. [`RangeSplitPointsExec`] picks the split points from the sample and
//! emits them as rows. [`RangeChannelExec`] reads those rows in each task of the
//! shuffle and adds the output channel of each input row, so the shuffle writes every
//! row to its channel. The sample then reads the same stages as the shuffle, such as
//! the stage of a shared CTE.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, BooleanArray, Float64Array, RecordBatch, UInt64Array, new_empty_array,
};
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef, UInt64Type};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RangeExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, apply_expression_roots, collect, execute_stream,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use futures::{StreamExt, TryStreamExt};

use crate::range_sample::{RangeSketch, RangeSplit, range_sample_imbalance, resample_partition};

/// The column of [`RangeChannelExec`] with the output channel of each row.
pub const CHANNEL_COLUMN: &str = "__channel";

/// The column of [`RangeSplitPointsExec`] that tells whether a Spark bound has a
/// split point.
pub const SPLIT_POINT_COLUMN: &str = "__split_point";

/// How the split points of a range repartitioning are picked from its sample.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSampleOptions {
    /// The sort options of the range keys.
    pub sort_options: Vec<SortOptions>,
    /// The number of partitions of the range repartitioning.
    pub num_partitions: usize,
    /// The total number of rows the sample should have.
    pub sample_size: f64,
    /// The number of rows each partition of the first sample keeps.
    pub partition_sample_size: usize,
}

impl RangeSampleOptions {
    /// The candidates for split points from the output of the first sample.
    async fn sketch(
        &self,
        sample: SendableRecordBatchStream,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<RangeSketch> {
        RangeSketch::try_new(
            sample,
            self.sort_options.len(),
            self.sample_size,
            self.partition_sample_size,
            pool,
        )
        .await
    }

    /// The fraction and the imbalanced partitions from the output of the first sample.
    async fn imbalance(
        &self,
        sample: SendableRecordBatchStream,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<(f64, Vec<usize>)> {
        range_sample_imbalance(
            sample,
            self.sort_options.len(),
            self.sample_size,
            self.partition_sample_size,
            pool,
        )
        .await
    }

    /// Checks that the plan emits the output of the first sample: the range keys, the
    /// input partition, and the number of rows the partition read.
    fn check_sample(&self, sample: &Arc<dyn ExecutionPlan>) -> Result<()> {
        let num_keys = self.sort_options.len();
        let columns = sample.schema().fields().len();
        if num_keys == 0 || columns != num_keys + 2 {
            return internal_err!(
                "a range sample of {num_keys} keys needs 2 more columns, found {columns} columns"
            );
        }
        Ok(())
    }
}

/// Picks the split points of a range repartitioning, and emits one row per Spark bound,
/// in order: the split point of the bound in one column per range key, and whether the
/// bound has one in [`SPLIT_POINT_COLUMN`]. Only the last bound can have no split
/// point (see [`crate::range_sample::range_split_points`]), and then its keys are null.
/// So the range repartitioning has as many partitions as rows plus one.
///
/// The first child is the output of [`crate::range_sample::RangePartitionSampleExec`],
/// and the second child is the output of [`RangeResampleExec`] for the imbalanced
/// partitions of the first sample. The node has one partition and reads every
/// partition of its children, so the split points are picked once for all the tasks
/// that read them.
#[derive(Debug, Clone)]
pub struct RangeSplitPointsExec {
    sample: Arc<dyn ExecutionPlan>,
    resample: Arc<dyn ExecutionPlan>,
    options: RangeSampleOptions,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RangeSplitPointsExec {
    pub fn try_new(
        sample: Arc<dyn ExecutionPlan>,
        resample: Arc<dyn ExecutionPlan>,
        options: RangeSampleOptions,
    ) -> Result<Self> {
        options.check_sample(&sample)?;
        let num_keys = options.sort_options.len();
        let mut fields = sample.schema().fields()[..num_keys]
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>();
        fields.push(Field::new(SPLIT_POINT_COLUMN, DataType::Boolean, false));
        let schema = Arc::new(Schema::new(fields));
        if resample.schema().fields().len() != num_keys {
            return internal_err!("a range resample needs {num_keys} key columns");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            sample,
            resample,
            options,
            schema,
            properties,
        })
    }

    pub fn sample(&self) -> &Arc<dyn ExecutionPlan> {
        &self.sample
    }

    pub fn resample(&self) -> &Arc<dyn ExecutionPlan> {
        &self.resample
    }

    pub fn options(&self) -> &RangeSampleOptions {
        &self.options
    }
}

impl DisplayAs for RangeSplitPointsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RangeSplitPointsExec: partitions={}, sample_size={}",
            self.options.num_partitions, self.options.sample_size
        )
    }
}

impl ExecutionPlan for RangeSplitPointsExec {
    fn name(&self) -> &'static str {
        "RangeSplitPointsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.sample, &self.resample]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
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
        let [sample, resample] = children.as_slice() else {
            return internal_err!("{} requires exactly two children", self.name());
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(sample),
            Arc::clone(resample),
            self.options.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("{} has no partition {partition}", self.name());
        }
        let sample = Arc::clone(&self.sample);
        let resample = Arc::clone(&self.resample);
        let options = self.options.clone();
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let output = futures::stream::once(async move {
            let pool = context.memory_pool();
            let sample = execute_stream(sample, Arc::clone(&context))?;
            let mut sketch = options.sketch(sample, pool).await?;
            // The resample is empty unless the first sample has imbalanced partitions.
            sketch
                .add_resample(execute_stream(resample, Arc::clone(&context))?)
                .await?;
            let split = sketch.split_points(&options.sort_options, options.num_partitions)?;
            split_points_batch(&split, schema)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            output,
        )))
    }
}

/// Reduces the first sample to one (partition, fraction) row per imbalanced
/// partition before the result is broadcast to the resampling tasks.
#[derive(Debug, Clone)]
pub struct RangeResampleDecisionExec {
    sample: Arc<dyn ExecutionPlan>,
    options: RangeSampleOptions,
    properties: Arc<PlanProperties>,
}

fn resample_decision_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt64, false),
        Field::new("fraction", DataType::Float64, false),
    ]))
}

impl RangeResampleDecisionExec {
    pub fn try_new(sample: Arc<dyn ExecutionPlan>, options: RangeSampleOptions) -> Result<Self> {
        options.check_sample(&sample)?;
        if sample.output_partitioning().partition_count() != 1 {
            return internal_err!("range resampling decisions require a single sample partition");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(resample_decision_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            sample,
            options,
            properties,
        })
    }

    pub fn sample(&self) -> &Arc<dyn ExecutionPlan> {
        &self.sample
    }

    pub fn options(&self) -> &RangeSampleOptions {
        &self.options
    }
}

impl DisplayAs for RangeResampleDecisionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RangeResampleDecisionExec")
    }
}

impl ExecutionPlan for RangeResampleDecisionExec {
    fn name(&self) -> &'static str {
        "RangeResampleDecisionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.sample]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [sample] = children.as_slice() else {
            return internal_err!("{} requires one child", self.name());
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(sample),
            self.options.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let sample = self.sample.execute(partition, Arc::clone(&context))?;
        let schema = self.schema();
        let options = self.options.clone();
        let stream = futures::stream::once(async move {
            let (fraction, partitions) = options.imbalance(sample, context.memory_pool()).await?;
            let fractions = Float64Array::from_value(fraction, partitions.len());
            let partitions =
                UInt64Array::from_iter_values(partitions.into_iter().map(|id| id as u64));
            Ok(RecordBatch::try_new(
                schema,
                vec![Arc::new(partitions), Arc::new(fractions)],
            )?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Samples again the imbalanced partitions of the first sample of a range
/// repartitioning, like [`crate::range_sample::RangePartitionResampleExec`], for a job
/// that cannot plan the resample after the first sample.
///
/// The first child computes the range keys again. The second broadcasts the
/// decisions from RangeResampleDecisionExec.
/// If no partition is imbalanced, no task reads the keys.
#[derive(Debug, Clone)]
pub struct RangeResampleExec {
    input: Arc<dyn ExecutionPlan>,
    decisions: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl RangeResampleExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        decisions: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        if decisions.schema() != resample_decision_schema()
            || decisions.output_partitioning().partition_count() != 1
        {
            return internal_err!(
                "a range resample requires a single partition of resampling decisions"
            );
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            decisions,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn decisions(&self) -> &Arc<dyn ExecutionPlan> {
        &self.decisions
    }
}

impl DisplayAs for RangeResampleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RangeResampleExec")
    }
}

impl ExecutionPlan for RangeResampleExec {
    fn name(&self) -> &'static str {
        "RangeResampleExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input, &self.decisions]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
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
        let [input, decisions] = children.as_slice() else {
            return internal_err!("{} requires exactly two children", self.name());
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            Arc::clone(decisions),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let decisions = self.decisions.execute(0, Arc::clone(&context))?;
        let schema = self.schema();
        let output = futures::stream::once(async move {
            let mut decisions = decisions;
            let mut has_imbalance = false;
            let mut fraction = None;
            while let Some(batch) = decisions.try_next().await? {
                has_imbalance |= batch.num_rows() > 0;
                let ids = batch.column(0).as_primitive::<UInt64Type>();
                if let Ok(index) = ids.values().binary_search(&(partition as u64)) {
                    fraction = Some(batch.column(1).as_primitive::<Float64Type>().value(index));
                }
            }
            if !has_imbalance {
                let empty: SendableRecordBatchStream =
                    Box::pin(EmptyRecordBatchStream::new(input.schema()));
                return Ok::<_, DataFusionError>(empty);
            }
            Ok(resample_partition(
                input.execute(partition, context)?,
                partition,
                fraction,
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

/// Adds the output channel of each input row of a range repartitioning, as the last
/// column [`CHANNEL_COLUMN`]. The channel is the range partition of the row for the
/// split points in the rows that the second child emits (see [`RangeSplitPointsExec`]).
///
/// Each partition reads all the split points before its first input row.
#[derive(Debug, Clone)]
pub struct RangeChannelExec {
    input: Arc<dyn ExecutionPlan>,
    split_points: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RangeChannelExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        split_points: Arc<dyn ExecutionPlan>,
        ordering: LexOrdering,
    ) -> Result<Self> {
        if split_points.schema().fields().len() != ordering.len() + 1 {
            return internal_err!(
                "range channels need {} split point columns, found {}",
                ordering.len() + 1,
                split_points.schema().fields().len()
            );
        }
        if split_points.output_partitioning().partition_count() != 1 {
            return internal_err!("range channels need the split points in one partition");
        }
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            CHANNEL_COLUMN,
            DataType::UInt64,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            split_points,
            ordering,
            schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn split_points(&self) -> &Arc<dyn ExecutionPlan> {
        &self.split_points
    }

    pub fn ordering(&self) -> &LexOrdering {
        &self.ordering
    }
}

impl DisplayAs for RangeChannelExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RangeChannelExec: ordering=[{}]", self.ordering)
    }
}

impl ExecutionPlan for RangeChannelExec {
    fn name(&self) -> &'static str {
        "RangeChannelExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input, &self.split_points]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.ordering.iter().map(|sort_expr| &sort_expr.expr), f)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
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
        let [input, split_points] = children.as_slice() else {
            return internal_err!("{} requires exactly two children", self.name());
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            Arc::clone(split_points),
            self.ordering.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let split_points = Arc::clone(&self.split_points);
        let ordering = self.ordering.clone();
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let output = futures::stream::once(async move {
            let batches = collect(split_points, Arc::clone(&context)).await?;
            let router = range_partitioning(ordering, &batches)?
                .map(|range| {
                    let keys = range
                        .ordering()
                        .iter()
                        .map(|sort_expr| Arc::clone(&sort_expr.expr))
                        .collect();
                    RangeExpr::try_new(keys, &range)
                })
                .transpose()?;
            let input = input.execute(partition, context)?;
            Ok::<_, DataFusionError>(input.map(move |batch| {
                let batch = batch?;
                let rows = batch.num_rows();
                let channels: ArrayRef = match &router {
                    Some(router) => router.evaluate(&batch)?.into_array(rows)?,
                    None => Arc::new(UInt64Array::from(vec![0; rows])),
                };
                let mut columns = batch.columns().to_vec();
                columns.push(channels);
                Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
            }))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            output,
        )))
    }
}

/// The rows of [`RangeSplitPointsExec`] for the split points.
fn split_points_batch(split: &RangeSplit, schema: SchemaRef) -> Result<RecordBatch> {
    let bounds = split.partitions() - 1;
    let split_points = split.split_points();
    let (keys, _) = schema.fields().split_at(schema.fields().len() - 1);
    let mut columns = keys
        .iter()
        .enumerate()
        .map(|(i, field)| {
            if bounds == 0 {
                return Ok(new_empty_array(field.data_type()));
            }
            let null = ScalarValue::try_from(field.data_type())?;
            ScalarValue::iter_to_array((0..bounds).map(|bound| match split_points.get(bound) {
                Some(split_point) => split_point.values()[i].clone(),
                None => null.clone(),
            }))
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    columns.push(Arc::new(BooleanArray::from_iter(
        (0..bounds).map(|bound| Some(bound < split_points.len())),
    )));
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// The split points in the rows of [`RangeSplitPointsExec`].
pub fn range_split_from_batches(batches: &[RecordBatch]) -> Result<RangeSplit> {
    let mut bounds = 0;
    let mut split_points = vec![];
    for batch in batches {
        let Some((has_split_point, keys)) = batch.columns().split_last() else {
            return internal_err!("range split points need a column for each bound");
        };
        let Some(has_split_point) = has_split_point.as_boolean_opt() else {
            return internal_err!("range split points need a boolean column for each bound");
        };
        for row in 0..batch.num_rows() {
            bounds += 1;
            if has_split_point.value(row) {
                let values = keys
                    .iter()
                    .map(|column| ScalarValue::try_from_array(column, row))
                    .collect::<Result<Vec<_>>>()?;
                split_points.push(SplitPoint::new(values));
            }
        }
    }
    RangeSplit::try_new(split_points, bounds + 1)
}

/// The range partitioning of the ordering at the split points, unless there are none.
fn range_partitioning(
    ordering: LexOrdering,
    batches: &[RecordBatch],
) -> Result<Option<RangePartitioning>> {
    let split = range_split_from_batches(batches)?;
    if split.split_points().is_empty() {
        return Ok(None);
    }
    Ok(Some(RangePartitioning::try_new(
        ordering,
        split.split_points().to_vec(),
    )?))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, Int64Array};
    use datafusion::arrow::datatypes::{Int64Type, UInt64Type};
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::collect_partitioned;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion::physical_plan::test::exec::ErrorExec;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::range_sample::{RangePartitionResampleExec, RangePartitionSampleExec};

    fn key_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]))
    }

    fn split_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new(SPLIT_POINT_COLUMN, DataType::Boolean, false),
        ]))
    }

    fn keys(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(key_schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn ordering(options: SortOptions) -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 0)),
            options,
        )])
        .unwrap()
    }

    /// The key and the channel of each row, by partition.
    async fn channels(
        partitions: Vec<Vec<RecordBatch>>,
        split_points: Vec<i64>,
        options: SortOptions,
    ) -> Vec<Vec<(Option<i64>, u64)>> {
        let input = TestMemoryExec::try_new_exec(&partitions, key_schema(), None).unwrap();
        let split_points = split_points
            .into_iter()
            .map(|k| SplitPoint::new(vec![ScalarValue::Int64(Some(k))]))
            .collect::<Vec<_>>();
        let partitions = split_points.len() + 1;
        let split = RangeSplit::try_new(split_points, partitions).unwrap();
        let rows = split_points_batch(&split, split_schema()).unwrap();
        let split_points =
            TestMemoryExec::try_new_exec(&[vec![rows]], split_schema(), None).unwrap();
        let exec =
            Arc::new(RangeChannelExec::try_new(input, split_points, ordering(options)).unwrap());
        let ctx = SessionContext::new();
        collect_partitioned(exec, ctx.task_ctx())
            .await
            .unwrap()
            .into_iter()
            .map(|batches| {
                batches
                    .iter()
                    .flat_map(|batch| {
                        let k = batch.column(0).as_primitive::<Int64Type>();
                        let channel = batch.column(1).as_primitive::<UInt64Type>();
                        (0..batch.num_rows())
                            .map(|row| (k.is_valid(row).then(|| k.value(row)), channel.value(row)))
                            .collect::<Vec<_>>()
                    })
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn each_row_goes_to_the_range_of_its_key() {
        let asc = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let partitions = vec![
            vec![keys(vec![Some(1), Some(4), None])],
            vec![keys(vec![Some(8), Some(3)]), keys(vec![Some(9), Some(7)])],
        ];
        // DataFusion puts a key equal to a split point in the range above it.
        assert_eq!(
            channels(partitions, vec![4, 8], asc).await,
            vec![
                vec![(Some(1), 0), (Some(4), 1), (None, 0)],
                vec![(Some(8), 2), (Some(3), 0), (Some(9), 2), (Some(7), 1)],
            ]
        );
    }

    #[tokio::test]
    async fn a_descending_key_with_nulls_last_goes_to_its_range() {
        let desc = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let partitions = vec![vec![keys(vec![Some(9), Some(5), Some(1), None])]];
        assert_eq!(
            channels(partitions, vec![5], desc).await,
            vec![vec![(Some(9), 0), (Some(5), 1), (Some(1), 1), (None, 1)]]
        );
    }

    #[tokio::test]
    async fn without_split_points_every_row_goes_to_the_first_channel() {
        let partitions = vec![vec![keys(vec![Some(3), None])], vec![keys(vec![Some(1)])]];
        assert_eq!(
            channels(partitions, vec![], SortOptions::default()).await,
            vec![vec![(Some(3), 0), (None, 0)], vec![(Some(1), 0)]]
        );
    }

    fn sample_options(
        num_partitions: usize,
        sample_size: f64,
        partition_sample_size: usize,
    ) -> RangeSampleOptions {
        RangeSampleOptions {
            sort_options: vec![SortOptions::default()],
            num_partitions,
            sample_size,
            partition_sample_size,
        }
    }

    /// The split points that a job picks from the sample and the resample of the input,
    /// those that the local sample picks (see `RangeSample::run`), and whether the local
    /// sample samples imbalanced partitions again.
    async fn job_and_local_split_points(
        partitions: Vec<Vec<RecordBatch>>,
        options: RangeSampleOptions,
    ) -> (RangeSplit, RangeSplit, bool) {
        let input = || TestMemoryExec::try_new_exec(&partitions, key_schema(), None).unwrap();
        let sample = || -> Arc<dyn ExecutionPlan> {
            Arc::new(
                RangePartitionSampleExec::try_new(input(), options.partition_sample_size).unwrap(),
            )
        };
        let ctx = SessionContext::new();

        let task = ctx.task_ctx();
        let mut sketch = options
            .sketch(
                execute_stream(sample(), Arc::clone(&task)).unwrap(),
                task.memory_pool(),
            )
            .await
            .unwrap();
        let resampled = !sketch.imbalanced_partitions().is_empty();
        if resampled {
            let resample = RangePartitionResampleExec::try_new(
                input(),
                sketch.fraction(),
                sketch.imbalanced_partitions().to_vec(),
            )
            .unwrap();
            let resample = execute_stream(Arc::new(resample), Arc::clone(&task)).unwrap();
            sketch.add_resample(resample).await.unwrap();
        }
        let local = sketch
            .split_points(&options.sort_options, options.num_partitions)
            .unwrap();

        let resample = RangeResampleExec::try_new(
            input(),
            Arc::new(
                RangeResampleDecisionExec::try_new(
                    Arc::new(CoalescePartitionsExec::new(sample())),
                    options.clone(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let exec = RangeSplitPointsExec::try_new(sample(), Arc::new(resample), options).unwrap();
        let batches = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();
        (
            range_split_from_batches(&batches).unwrap(),
            local,
            resampled,
        )
    }

    fn balanced_partitions() -> Vec<Vec<RecordBatch>> {
        vec![
            vec![keys((0..50).map(Some).collect())],
            vec![keys((50..80).map(Some).collect())],
            vec![keys((80..100).map(Some).collect())],
        ]
    }

    #[tokio::test]
    async fn the_split_points_of_a_balanced_sample_are_those_of_the_local_sample() {
        let (job, local, resampled) =
            job_and_local_split_points(balanced_partitions(), sample_options(4, 40.0, 60)).await;
        assert!(!resampled);
        assert_eq!(local.split_points().len(), 3);
        assert_eq!(job, local);
    }

    #[tokio::test]
    async fn the_split_points_of_an_imbalanced_sample_are_those_of_the_local_resample() {
        // One partition has far more than three times the average number of rows.
        let mut partitions = vec![vec![keys((0..1000).map(Some).collect())]];
        for i in 0..7 {
            partitions.push(vec![keys(
                (1000 + i * 10..1010 + i * 10).map(Some).collect(),
            )]);
        }
        let (job, local, resampled) =
            job_and_local_split_points(partitions, sample_options(4, 40.0, 15)).await;
        assert!(resampled);
        assert_eq!(local.split_points().len(), 3);
        assert_eq!(job, local);
    }

    #[tokio::test]
    async fn a_balanced_sample_reads_no_keys_again() {
        let options = sample_options(4, 40.0, 60);
        let input =
            TestMemoryExec::try_new_exec(&balanced_partitions(), key_schema(), None).unwrap();
        let sample = Arc::new(RangePartitionSampleExec::try_new(input, 60).unwrap());
        // The keys fail if they are read.
        let resample = RangeResampleExec::try_new(
            Arc::new(ErrorExec::new()),
            Arc::new(
                RangeResampleDecisionExec::try_new(
                    Arc::new(CoalescePartitionsExec::new(sample)),
                    options,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let ctx = SessionContext::new();
        let batches = collect(Arc::new(resample), ctx.task_ctx()).await.unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn split_points_round_trip_through_their_rows() {
        let point = |k| SplitPoint::new(vec![ScalarValue::Int64(k)]);
        for (split_points, partitions, rows) in [
            (vec![point(Some(4)), point(None)], 3, 2),
            // The last bound has no split point, so the last partition is empty.
            (vec![point(Some(4))], 3, 2),
            (vec![], 2, 1),
            (vec![], 1, 0),
        ] {
            let split = RangeSplit::try_new(split_points, partitions).unwrap();
            let batch = split_points_batch(&split, split_schema()).unwrap();
            assert_eq!(batch.num_rows(), rows);
            assert_eq!(range_split_from_batches(&[batch]).unwrap(), split);
        }
    }
}
