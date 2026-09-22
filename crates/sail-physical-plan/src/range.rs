use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use sail_logical_plan::range::Range;

const RANGE_BATCH_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct RangeExec {
    range: Range,
    num_partitions: usize,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl RangeExec {
    /// Creates a new execution plan for the range source.
    /// The schema should be the original schema before projection.
    pub fn try_new(
        range: Range,
        num_partitions: usize,
        schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(&projection)?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::RoundRobinBatch(num_partitions),
            EmissionType::Both,
            Boundedness::Bounded,
        ));
        Ok(Self {
            range,
            num_partitions,
            original_schema: schema,
            projected_schema,
            projection,
            properties,
        })
    }

    pub fn range(&self) -> &Range {
        &self.range
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub fn projection(&self) -> &[usize] {
        &self.projection
    }
}

impl DisplayAs for RangeExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RangeExec")
    }
}

impl ExecutionPlan for RangeExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
        if !children.is_empty() {
            return internal_err!("RangeExec should have no children");
        }
        Ok(self)
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<datafusion_common::Statistics>],
        args: &datafusion::physical_plan::statistics::StatisticsArgs,
    ) -> Result<Arc<datafusion_common::Statistics>> {
        use datafusion_common::stats::Precision;
        let count = |partition| {
            let range = self.range.partition(partition, self.num_partitions);
            let distance = i128::from(range.end) - i128::from(range.start);
            let step = i128::from(range.step);
            if step == 0 {
                return None;
            }
            let rows = if distance.signum() == step.signum() {
                (distance.abs() + step.abs() - 1) / step.abs()
            } else {
                0
            };
            usize::try_from(rows).ok()
        };
        let rows = match args.partition() {
            Some(partition) => count(partition),
            None => (0..self.num_partitions).try_fold(0usize, |rows, partition| {
                rows.checked_add(count(partition)?)
            }),
        };
        let mut statistics = datafusion_common::Statistics::new_unknown(&self.projected_schema);
        statistics.num_rows = rows.map(Precision::Exact).unwrap_or(Precision::Absent);
        for column in &mut statistics.column_statistics {
            column.null_count = Precision::Exact(0);
        }
        Ok(Arc::new(statistics))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.num_partitions {
            return exec_err!("partition index out of range: {}", partition);
        }
        let mut iter = self
            .range
            .partition(partition, self.num_partitions)
            .into_iter();
        let projected_schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        let chunks = std::iter::from_fn(move || {
            Some(iter.by_ref().take(RANGE_BATCH_SIZE).collect::<Vec<i64>>())
                .filter(|x| !x.is_empty())
                .map(|x| -> Result<RecordBatch> {
                    let num_rows = x.len();
                    if projection.is_empty() {
                        return Ok(RecordBatch::try_new_with_options(
                            projected_schema.clone(),
                            vec![],
                            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                        )?);
                    }
                    let id_array: ArrayRef = Arc::new(Int64Array::from(x));
                    let columns: Vec<ArrayRef> = projection
                        .iter()
                        .map(|&i| match i {
                            0 => Ok(id_array.clone()),
                            _ => plan_err!("invalid projection index {i} for range table"),
                        })
                        .collect::<Result<_>>()?;
                    Ok(RecordBatch::try_new(projected_schema.clone(), columns)?)
                })
        });
        let stream = tokio_stream::iter(chunks);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion_common::stats::Precision;
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn exact_row_counts_match_execution_for_each_partition() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for range in [
            Range {
                start: -7,
                end: 12,
                step: 3,
            },
            Range {
                start: 12,
                end: -7,
                step: -3,
            },
            Range {
                start: 1,
                end: 1,
                step: 1,
            },
            Range {
                start: 9,
                end: 1,
                step: 1,
            },
            Range {
                start: i64::MAX - 10,
                end: i64::MAX,
                step: 2,
            },
            Range {
                start: i64::MIN + 10,
                end: i64::MIN,
                step: -2,
            },
        ] {
            for partitions in [1, 2, 8] {
                for projection in [vec![], vec![0]] {
                    let plan =
                        RangeExec::try_new(range.clone(), partitions, schema.clone(), projection)?;
                    let mut total = 0;
                    for partition in 0..partitions {
                        let batches = plan
                            .execute(partition, Arc::new(TaskContext::default()))?
                            .try_collect::<Vec<_>>()
                            .await?;
                        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                        total += rows;
                        let stats = plan.statistics_from_inputs(
                            &[],
                            &StatisticsArgs::new().with_partition(Some(partition)),
                        )?;
                        assert_eq!(stats.num_rows, Precision::Exact(rows));
                    }
                    let stats = StatisticsContext::new().compute(&plan, &StatisticsArgs::new())?;
                    assert_eq!(stats.num_rows, Precision::Exact(total));
                }
            }
        }
        Ok(())
    }
}
