use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, exec_err, internal_err, plan_err};
use futures::Stream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityGenerateColumn {
    pub output: String,
    pub mask: Option<usize>,
    pub start: i64,
    pub step: i64,
    pub high_water_mark: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct IdentityGenerateExec {
    input: Arc<dyn ExecutionPlan>,
    columns: Vec<IdentityGenerateColumn>,
    /// Set when serializing a distributed stage; worker input replacement cannot change it.
    pinned_partitions: Option<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl IdentityGenerateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        columns: Vec<IdentityGenerateColumn>,
        pinned_partitions: Option<usize>,
    ) -> Result<Self> {
        if columns.is_empty() || pinned_partitions == Some(0) {
            return plan_err!("invalid IdentityGenerate allocation domain");
        }
        let schema = input.schema();
        let mut fields = schema.fields().to_vec();
        let mut names = fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<HashSet<_>>();
        for column in &columns {
            if column.step == 0 || column.output.is_empty() || !names.insert(column.output.clone())
            {
                return plan_err!("IdentityGenerate requires nonzero steps and fresh output names");
            }
            if let Some(mask) = column.mask {
                if schema
                    .fields()
                    .get(mask)
                    .is_none_or(|f| f.data_type() != &DataType::Boolean)
                {
                    return plan_err!("invalid IdentityGenerate mask index {mask}");
                }
            }
            fields.push(Arc::new(Field::new(
                &column.output,
                DataType::Int64,
                column.mask.is_some(),
            )));
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let equivalences =
            EquivalenceProperties::new(schema).extend(input.equivalence_properties().clone())?;
        let properties = Arc::new(PlanProperties::new(
            equivalences,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            columns,
            pinned_partitions,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn columns(&self) -> &[IdentityGenerateColumn] {
        &self.columns
    }
    pub fn allocation_partitions(&self) -> usize {
        self.pinned_partitions
            .unwrap_or_else(|| self.input.output_partitioning().partition_count())
    }
}

impl DisplayAs for IdentityGenerateExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IdentityGenerateExec: columns={:?}, allocation_partitions={}",
            self.columns,
            self.allocation_partitions()
        )
    }
}
impl ExecutionPlan for IdentityGenerateExec {
    fn name(&self) -> &'static str {
        "IdentityGenerateExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
        let [input] = children.as_slice() else {
            return internal_err!("IdentityGenerate requires one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.columns.clone(),
            self.pinned_partitions,
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partitions = self.allocation_partitions();
        if partitions == 0 || partition >= partitions {
            return exec_err!("invalid identity allocation partition {partition}/{partitions}");
        }
        Ok(Box::pin(IdentityGenerateStream {
            output_rows: MetricBuilder::new(&self.metrics).output_rows(partition),
            input: self.input.execute(partition, context)?,
            schema: self.schema(),
            generators: self
                .columns
                .iter()
                .cloned()
                .map(|column| Generator {
                    column,
                    next: None,
                    increment: None,
                })
                .collect(),
            partition,
            partitions,
        }))
    }
}

struct Generator {
    column: IdentityGenerateColumn,
    next: Option<i64>,
    increment: Option<i64>,
}
impl Generator {
    // [CREDIT]: https://github.com/delta-io/delta/blob/bb1057f43/spark/src/main/scala/org/apache/spark/sql/delta/GenerateIdentityValues.scala#L108-L154
    fn next(&mut self, partition: usize, partitions: usize) -> Result<i64> {
        let overflow = || {
            DataFusionError::Execution(format!(
                "DELTA_IDENTITY_COLUMN_VALUE_OVERFLOW: identity column `{}` cannot generate a next value without overflowing BIGINT",
                self.column.output
            ))
        };
        let increment = match self.increment {
            Some(value) => value,
            None => i64::try_from(partitions)
                .ok()
                .and_then(|n| n.checked_mul(self.column.step))
                .ok_or_else(overflow)?,
        };
        let current = match self.next {
            Some(value) => value,
            None => {
                let base = match self.column.high_water_mark {
                    Some(hwm) => hwm.checked_add(self.column.step).ok_or_else(overflow)?,
                    None => self.column.start,
                };
                i64::try_from(partition)
                    .ok()
                    .and_then(|p| p.checked_mul(self.column.step))
                    .and_then(|offset| base.checked_add(offset))
                    .ok_or_else(overflow)?
            }
        };
        self.next = Some(current.checked_add(increment).ok_or_else(overflow)?);
        self.increment = Some(increment);
        Ok(current)
    }
}
struct IdentityGenerateStream {
    output_rows: Count,
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    generators: Vec<Generator>,
    partition: usize,
    partitions: usize,
}
impl IdentityGenerateStream {
    fn generate(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        for generator in &mut self.generators {
            let mask = generator
                .column
                .mask
                .map(|index| {
                    batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .filter(|mask| mask.null_count() == 0)
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "identity masks must be non-null Boolean values".into(),
                            )
                        })
                })
                .transpose()?;
            let values = (0..batch.num_rows())
                .map(|row| {
                    if mask.is_none_or(|mask| mask.value(row)) {
                        generator.next(self.partition, self.partitions).map(Some)
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            columns.push(Arc::new(Int64Array::from(values)));
        }
        let result = RecordBatch::try_new(self.schema.clone(), columns)?;
        self.output_rows.add(result.num_rows());
        Ok(result)
    }
}
impl RecordBatchStream for IdentityGenerateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::empty::EmptyExec;
    use futures::TryStreamExt;

    fn descriptor(step: i64) -> IdentityGenerateColumn {
        IdentityGenerateColumn {
            output: "id".into(),
            mask: Some(0),
            start: 7,
            step,
            high_water_mark: None,
        }
    }

    #[tokio::test]
    async fn masks_partitions_batches_and_independent_streams() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "mask",
            DataType::Boolean,
            false,
        )]));
        let batch = |values: Vec<bool>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(BooleanArray::from(values))])
        };
        let partitions = vec![
            vec![
                batch(vec![true, false])?,
                RecordBatch::new_empty(schema.clone()),
                batch(vec![true, true])?,
            ],
            vec![batch(vec![false, true])?],
            vec![],
        ];
        let input = MemorySourceConfig::try_new_exec(&partitions, schema, None)?;
        for step in [3, -3] {
            let plan = IdentityGenerateExec::try_new(input.clone(), vec![descriptor(step)], None)?;
            assert_eq!(plan.allocation_partitions(), 3);
            for _ in 0..2 {
                for (partition, expected) in [
                    (
                        0,
                        vec![
                            Some(7),
                            None,
                            Some(7 + 9 * step / 3),
                            Some(7 + 18 * step / 3),
                        ],
                    ),
                    (1, vec![None, Some(7 + step)]),
                    (2, vec![]),
                ] {
                    let output = plan
                        .execute(partition, Arc::new(TaskContext::default()))?
                        .try_collect::<Vec<_>>()
                        .await?;
                    let mut actual = Vec::new();
                    for batch in output {
                        let array = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal("expected identity array".into())
                            })?;
                        actual.extend(array.iter());
                    }
                    assert_eq!(actual, expected);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn successor_overflow_and_pinned_watermark() -> Result<()> {
        for (start, step) in [(i64::MAX, 1), (i64::MIN, -1)] {
            let mut generator = Generator {
                column: IdentityGenerateColumn {
                    start,
                    ..descriptor(step)
                },
                next: None,
                increment: None,
            };
            assert!(generator.next(0, 1).is_err());
        }
        let mut generator = Generator {
            column: IdentityGenerateColumn {
                high_water_mark: Some(100),
                ..descriptor(3)
            },
            next: None,
            increment: None,
        };
        assert_eq!(generator.next(1, 4)?, 106);
        assert_eq!(generator.next(1, 4)?, 118);
        Ok(())
    }

    #[test]
    fn serialized_allocation_domain_survives_child_replacement() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "mask",
            DataType::Boolean,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let plan = Arc::new(IdentityGenerateExec::try_new(
            input.clone(),
            vec![descriptor(-3)],
            Some(11),
        )?);
        #[expect(deprecated)]
        let replaced = plan.with_new_children(vec![input])?;
        let generator = replaced
            .downcast_ref::<IdentityGenerateExec>()
            .ok_or_else(|| DataFusionError::Internal("missing generator".into()))?;
        assert_eq!(generator.allocation_partitions(), 11);
        Ok(())
    }
}
impl Stream for IdentityGenerateStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.generate(batch))),
            other => other,
        }
    }
}
