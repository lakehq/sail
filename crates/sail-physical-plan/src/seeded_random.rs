use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::{
    ColumnStatistics, Result, ScalarValue, Statistics, exec_err, internal_err,
};
use datafusion_expr::{ColumnarValue, ScalarUDF};
use futures::Stream;
use sail_function::scalar::math::partition_random::{
    PartitionRandomGenerator, PartitionSeed, create_partition_random_generator,
};

/// A column computed by a random function called with a seed.
#[derive(Debug, Clone)]
pub struct SeededRandomExprColumn {
    pub name: String,
    /// The call of the random function, whose arguments are constant.
    pub expr: Arc<dyn PhysicalExpr>,
    pub seed: PartitionSeed,
}

/// The random function and the values of its arguments for a column.
#[derive(Debug, Clone)]
struct SeededRandomCall {
    func: ScalarUDF,
    args: Vec<ScalarValue>,
}

/// Appends columns computed by random functions called with a seed. For each partition,
/// a generator per column is seeded once and continues across the batches of the partition.
#[derive(Debug, Clone)]
pub struct SeededRandomExec {
    input: Arc<dyn ExecutionPlan>,
    columns: Vec<SeededRandomExprColumn>,
    calls: Vec<SeededRandomCall>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SeededRandomExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        columns: Vec<SeededRandomExprColumn>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let input_len = input.schema().fields().len();
        if schema.fields().len() != input_len + columns.len() {
            return internal_err!(
                "SeededRandomExec expects {} output columns, got {}",
                input_len + columns.len(),
                schema.fields().len()
            );
        }
        let calls = columns
            .iter()
            .map(|column| {
                let Some(call) = column.expr.downcast_ref::<ScalarFunctionExpr>() else {
                    return internal_err!(
                        "SeededRandomExec expects a random function call, got {}",
                        column.expr
                    );
                };
                let args = call
                    .args()
                    .iter()
                    .map(evaluate_constant)
                    .collect::<Result<Vec<_>>>()?;
                Ok(SeededRandomCall {
                    func: call.fun().clone(),
                    args,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone())
                .extend(input.equivalence_properties().clone())?,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            columns,
            calls,
            schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn columns(&self) -> &[SeededRandomExprColumn] {
        &self.columns
    }
}

/// Evaluates an expression that does not depend on the input rows.
fn evaluate_constant(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    match expr.evaluate(&batch)? {
        ColumnarValue::Scalar(value) => Ok(value),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0),
    }
}

impl DisplayAs for SeededRandomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SeededRandomExec: ")?;
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} AS {}", column.expr, column.name)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for SeededRandomExec {
    fn name(&self) -> &'static str {
        "SeededRandomExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for column in &self.columns {
            if f(&column.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // The rows keep their order, but a sort must not be pushed below this node, since the
        // random values depend on the order of the rows in each partition.
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // The values depend on the partition of each row.
        vec![false]
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
            return internal_err!("SeededRandomExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.columns.clone(),
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_len = self.input.schema().fields().len();
        let generators = self
            .calls
            .iter()
            .zip(&self.columns)
            .enumerate()
            .map(|(i, (call, column))| {
                create_partition_random_generator(
                    &call.func,
                    &call.args,
                    self.schema.field(input_len + i).data_type(),
                    column.seed,
                    partition,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SeededRandomStream {
            input,
            schema: self.schema.clone(),
            generators,
        }))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let mut stats = input_stats[0].as_ref().clone();
        stats
            .column_statistics
            .truncate(self.input.schema().fields().len());
        stats
            .column_statistics
            .extend(self.columns.iter().map(|_| ColumnStatistics::new_unknown()));
        stats.total_byte_size = stats.total_byte_size.to_inexact();
        Ok(Arc::new(stats))
    }
}

struct SeededRandomStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    generators: Vec<Box<dyn PartitionRandomGenerator>>,
}

impl SeededRandomStream {
    fn append_columns(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let mut columns = batch.columns().to_vec();
        for generator in &mut self.generators {
            columns.push(generator.generate(num_rows)?);
        }
        if columns.len() != self.schema.fields().len() {
            return exec_err!(
                "SeededRandomExec produced {} columns, expected {}",
                columns.len(),
                self.schema.fields().len()
            );
        }
        Ok(RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?)
    }
}

impl RecordBatchStream for SeededRandomStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeededRandomStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.append_columns(batch))),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{AsArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Float64Type};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_plan::collect_partitioned;
    use datafusion::prelude::SessionContext;
    use datafusion_common::config::ConfigOptions;
    use sail_function::scalar::math::random::Random;

    use super::*;

    fn rand_expr(seed: i64, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(Random::new())),
            vec![lit(seed)],
            schema,
            Arc::new(ConfigOptions::default()),
        )?))
    }

    /// Returns the values of `rand(42)` for two partitions with 6 rows each, split into
    /// batches of the given size.
    async fn rand_values(batch_size: usize) -> Result<Vec<Vec<f64>>> {
        let input_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let partitions = (0..2)
            .map(|p| {
                (0..6)
                    .collect::<Vec<i64>>()
                    .chunks(batch_size)
                    .map(|ids| {
                        let ids = ids.iter().map(|id| id + p * 6);
                        Ok(RecordBatch::try_new(
                            input_schema.clone(),
                            vec![Arc::new(Int64Array::from_iter_values(ids))],
                        )?)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let input = MemorySourceConfig::try_new_exec(&partitions, input_schema.clone(), None)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("r", DataType::Float64, false),
        ]));
        let exec = SeededRandomExec::try_new(
            input,
            vec![SeededRandomExprColumn {
                name: "r".to_string(),
                expr: rand_expr(42, &input_schema)?,
                seed: PartitionSeed::Offset,
            }],
            schema,
        )?;
        let ctx = SessionContext::new();
        let output = collect_partitioned(Arc::new(exec), ctx.task_ctx()).await?;
        Ok(output
            .iter()
            .map(|batches| {
                batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(1)
                            .as_primitive::<Float64Type>()
                            .values()
                            .to_vec()
                    })
                    .collect()
            })
            .collect())
    }

    #[tokio::test]
    async fn test_seeded_random_values_per_partition() -> Result<()> {
        let values = rand_values(6).await?;
        // `SELECT rand(42) FROM range(0, 12, 1, 2)` in Spark 4.2.
        assert_eq!(values[0][..2], [0.619189370225301, 0.5096018842446481]);
        assert_eq!(values[1][0], 0.8017532427858894);
        assert_ne!(values[0], values[1]);
        Ok(())
    }

    #[tokio::test]
    async fn test_seeded_random_values_do_not_depend_on_batch_size() -> Result<()> {
        let expected = rand_values(6).await?;
        for batch_size in [1, 2, 4] {
            assert_eq!(rand_values(batch_size).await?, expected);
        }
        Ok(())
    }
}
