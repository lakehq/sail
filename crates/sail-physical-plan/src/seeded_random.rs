use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::kernels::boolean::{and_kleene, or_kleene};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    ColumnStatistics, Result, ScalarValue, Statistics, exec_datafusion_err, exec_err, internal_err,
};
use datafusion_expr::{ColumnarValue, Operator};
use futures::Stream;
use sail_function::scalar::math::partition_random::{
    PartitionRandomGenerator, PartitionSeed, create_partition_random_generator,
    seed_argument_position,
};

/// A column computed by an expression with calls of random functions with a seed.
#[derive(Debug, Clone)]
pub struct SeededRandomExprColumn {
    pub name: String,
    /// The expression, where each call of a random function with a seed and constant
    /// arguments is evaluated by its own generator for each partition.
    pub expr: Arc<dyn PhysicalExpr>,
    pub seed: PartitionSeed,
}

/// Appends columns computed by expressions with calls of random functions with a seed.
/// For each partition, a generator per call is seeded once and continues across the
/// batches of the partition, advancing only for the rows that evaluate the call, as in Spark.
#[derive(Debug, Clone)]
pub struct SeededRandomExec {
    input: Arc<dyn ExecutionPlan>,
    columns: Vec<SeededRandomExprColumn>,
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

/// Returns the expression to evaluate for a partition, where each call of a random function
/// with a seed and constant arguments is evaluated by its own generator for the partition.
fn partition_expr(
    expr: &Arc<dyn PhysicalExpr>,
    seed: PartitionSeed,
    partition: usize,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::clone(expr)
        .transform_up(|expr| {
            if let Some(call) = expr.downcast_ref::<ScalarFunctionExpr>()
                && seed_argument_position(call.fun(), call.args().len()).is_some()
                && let Some(args) = constant_arguments(call.args())
            {
                let generator = create_partition_random_generator(
                    call.fun(),
                    &args,
                    call.return_type(),
                    seed,
                    partition,
                )?;
                return Ok(Transformed::yes(Arc::new(PartitionRandomExpr {
                    generator: Mutex::new(generator),
                    data_type: call.return_type().clone(),
                    nullable: call.nullable(),
                    name: call.to_string(),
                })
                    as Arc<dyn PhysicalExpr>));
            }
            // DataFusion may evaluate the right side of `AND` and `OR` for more rows than
            // Spark, which would advance the generators of the calls on that side.
            if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
                && matches!(binary.op(), Operator::And | Operator::Or)
                && binary
                    .right()
                    .exists(|expr| Ok(expr.is::<PartitionRandomExpr>()))?
            {
                return Ok(Transformed::yes(Arc::new(LazyBooleanExpr::try_new(
                    Arc::clone(binary.left()),
                    *binary.op(),
                    Arc::clone(binary.right()),
                )?)
                    as Arc<dyn PhysicalExpr>));
            }
            Ok(Transformed::no(expr))
        })?
        .data)
}

/// Returns the values of the arguments if they do not depend on the input rows.
fn constant_arguments(args: &[Arc<dyn PhysicalExpr>]) -> Option<Vec<ScalarValue>> {
    args.iter()
        .map(|arg| {
            if collect_columns(arg).is_empty() {
                evaluate_constant(arg).ok()
            } else {
                None
            }
        })
        .collect()
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

/// A call of a random function with a seed, evaluated by a generator for one partition,
/// which advances by the number of rows that the call is evaluated for.
struct PartitionRandomExpr {
    generator: Mutex<Box<dyn PartitionRandomGenerator>>,
    data_type: DataType,
    nullable: bool,
    name: String,
}

impl std::fmt::Debug for PartitionRandomExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionRandomExpr")
            .field("name", &self.name)
            .finish()
    }
}

impl Display for PartitionRandomExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Each instance has its own generator, so an instance is only equal to itself.
impl PartialEq for PartitionRandomExpr {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for PartitionRandomExpr {}

impl Hash for PartitionRandomExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(self, state);
    }
}

impl PhysicalExpr for PartitionRandomExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let mut generator = self
            .generator
            .lock()
            .map_err(|e| exec_datafusion_err!("random generator lock poisoned: {e}"))?;
        Ok(ColumnarValue::Array(generator.generate(batch.num_rows())?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return internal_err!("PartitionRandomExpr does not have children");
        }
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn is_volatile_node(&self) -> bool {
        true
    }
}

/// `AND` or `OR` that evaluates the right side only for the rows where the left side does
/// not determine the result, as Spark does.
#[derive(Debug, Eq)]
struct LazyBooleanExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
    /// The input columns that the right side refers to.
    right_columns: Vec<usize>,
    /// The right side, referring to the columns of the input projected to `right_columns`,
    /// so that only these columns are filtered for the rows that evaluate it.
    projected_right: Arc<dyn PhysicalExpr>,
}

impl LazyBooleanExpr {
    fn try_new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Result<Self> {
        let mut right_columns = collect_columns(&right)
            .iter()
            .map(Column::index)
            .collect::<Vec<_>>();
        right_columns.sort_unstable();
        right_columns.dedup();
        // The calls of random functions are kept as they are, so that they keep their
        // generators.
        let projected_right = Arc::clone(&right)
            .transform_up(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let Ok(index) = right_columns.binary_search(&column.index()) else {
                    return internal_err!("missing column {column} in the right side");
                };
                Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
                ))
            })?
            .data;
        Ok(Self {
            left,
            op,
            right,
            right_columns,
            projected_right,
        })
    }
}

impl PartialEq for LazyBooleanExpr {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left) && self.op == other.op && self.right.eq(&other.right)
    }
}

impl Hash for LazyBooleanExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.op.hash(state);
        self.right.hash(state);
    }
}

impl Display for LazyBooleanExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl PhysicalExpr for LazyBooleanExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let left = self.left.evaluate(batch)?.into_array(num_rows)?;
        let left = as_boolean_array(&left)?;
        // The result is false where the left side of `AND` is false, and true where the
        // left side of `OR` is true.
        let is_or = self.op == Operator::Or;
        let selection = left
            .iter()
            .map(|value| Some(value != Some(is_or)))
            .collect::<BooleanArray>();
        let right = self
            .projected_right
            .evaluate_selection(&batch.project(&self.right_columns)?, &selection)?
            .into_array(num_rows)?;
        let right = as_boolean_array(&right)?;
        let result = if is_or {
            or_kleene(left, right)?
        } else {
            and_kleene(left, right)?
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [left, right] = children.as_slice() else {
            return internal_err!("LazyBooleanExpr requires two children");
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(left),
            self.op,
            Arc::clone(right),
        )?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
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
        let exprs = self
            .columns
            .iter()
            .map(|column| partition_expr(&column.expr, column.seed, partition))
            .collect::<Result<Vec<_>>>()?;
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SeededRandomStream {
            input,
            schema: self.schema.clone(),
            exprs,
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
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl SeededRandomStream {
    fn append_columns(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let mut columns = batch.columns().to_vec();
        for expr in &self.exprs {
            columns.push(expr.evaluate(&batch)?.into_array(num_rows)?);
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
    use datafusion::arrow::array::{ArrayRef, AsArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Float64Type};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::{
        LambdaExpr, LambdaVariable, binary, case, col, lit,
    };
    use datafusion::physical_plan::collect_partitioned;
    use datafusion::prelude::SessionContext;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarUDF;
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

    /// Evaluates an expression of `id` for one partition with `ids` split into batches of the
    /// given size.
    async fn column_values(
        expr: impl Fn(&Schema) -> Result<Arc<dyn PhysicalExpr>>,
        data_type: DataType,
        ids: std::ops::Range<i64>,
        batch_size: usize,
    ) -> Result<Vec<ArrayRef>> {
        // The input has another column `-id` before `id`, so that the expression refers to
        // `id` by a different index than in the columns that it uses.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("other", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let batches = ids
            .collect::<Vec<i64>>()
            .chunks(batch_size)
            .map(|ids| {
                let other = Arc::new(Int64Array::from_iter_values(ids.iter().map(|id| -id)));
                Ok(RecordBatch::try_new(
                    input_schema.clone(),
                    vec![other, Arc::new(Int64Array::from(ids.to_vec()))],
                )?)
            })
            .collect::<Result<Vec<_>>>()?;
        let input = MemorySourceConfig::try_new_exec(&[batches], input_schema.clone(), None)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("other", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("r", data_type, true),
        ]));
        let exec = SeededRandomExec::try_new(
            input,
            vec![SeededRandomExprColumn {
                name: "r".to_string(),
                expr: expr(&input_schema)?,
                seed: PartitionSeed::Offset,
            }],
            schema,
        )?;
        let ctx = SessionContext::new();
        let output = collect_partitioned(Arc::new(exec), ctx.task_ctx()).await?;
        Ok(output
            .iter()
            .flatten()
            .map(|batch| Arc::clone(batch.column(2)))
            .collect())
    }

    /// Returns the IDs in `0..20` for which the predicate is true, with the input split into
    /// batches of each size.
    async fn selected_ids(
        predicate: impl Fn(&Schema) -> Result<Arc<dyn PhysicalExpr>> + Copy,
    ) -> Result<Vec<(usize, Vec<i64>)>> {
        let mut result = vec![];
        for batch_size in [1, 3, 10] {
            let arrays = column_values(predicate, DataType::Boolean, 0..20, batch_size).await?;
            let values = arrays
                .iter()
                .flat_map(|array| array.as_boolean().iter().collect::<Vec<_>>())
                .collect::<Vec<_>>();
            let ids = (0..20)
                .zip(values)
                .filter(|(_, value)| *value == Some(true))
                .map(|(id, _)| id)
                .collect();
            result.push((batch_size, ids));
        }
        Ok(result)
    }

    #[tokio::test]
    async fn test_seeded_random_call_in_case_branch_advances_for_evaluated_rows() -> Result<()> {
        // `SELECT CASE WHEN id % 2 = 0 THEN rand(42) END FROM range(0, 10, 1, 1)` in Spark 4.2.
        let expected = [
            Some(0.619189370225301),
            None,
            Some(0.5096018842446481),
            None,
            Some(0.8325259388871524),
            None,
            Some(0.26322809041172357),
            None,
            Some(0.6702867696264135),
            None,
        ];
        let expr = |schema: &Schema| -> Result<Arc<dyn PhysicalExpr>> {
            let even = binary(
                binary(col("id", schema)?, Operator::Modulo, lit(2_i64), schema)?,
                Operator::Eq,
                lit(0_i64),
                schema,
            )?;
            case(None, vec![(even, rand_expr(42, schema)?)], None)
        };
        for batch_size in [1, 3, 10] {
            let arrays = column_values(expr, DataType::Float64, 0..10, batch_size).await?;
            let values = arrays
                .iter()
                .flat_map(|array| {
                    array
                        .as_primitive::<Float64Type>()
                        .iter()
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, expected, "batch size {batch_size}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_seeded_random_call_on_right_side_of_and_advances_for_evaluated_rows() -> Result<()>
    {
        // `SELECT id FROM range(0, 20, 1, 1) WHERE id > 5 AND rand(7) < 0.5` in Spark 4.2.
        let expected = vec![6, 7, 12, 14, 17, 19];
        let expr = |schema: &Schema| -> Result<Arc<dyn PhysicalExpr>> {
            binary(
                binary(col("id", schema)?, Operator::Gt, lit(5_i64), schema)?,
                Operator::And,
                binary(rand_expr(7, schema)?, Operator::Lt, lit(0.5), schema)?,
                schema,
            )
        };
        for (batch_size, ids) in selected_ids(expr).await? {
            assert_eq!(ids, expected, "batch size {batch_size}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_seeded_random_call_with_columns_on_right_side_advances_for_evaluated_rows()
    -> Result<()> {
        let id_ratio = |schema: &Schema, divisor: f64| -> Result<Arc<dyn PhysicalExpr>> {
            binary(
                Arc::new(datafusion::physical_expr::expressions::CastExpr::new(
                    col("id", schema)?,
                    DataType::Float64,
                    None,
                )),
                Operator::Divide,
                lit(divisor),
                schema,
            )
        };
        // `SELECT id FROM range(0, 20, 1, 1) WHERE id > 5 AND rand(7) < id / 20.0` in Spark 4.2.
        let and = |schema: &Schema| -> Result<Arc<dyn PhysicalExpr>> {
            binary(
                binary(col("id", schema)?, Operator::Gt, lit(5_i64), schema)?,
                Operator::And,
                binary(
                    rand_expr(7, schema)?,
                    Operator::Lt,
                    id_ratio(schema, 20.0)?,
                    schema,
                )?,
                schema,
            )
        };
        for (batch_size, ids) in selected_ids(and).await? {
            assert_eq!(ids, vec![6, 12, 14, 17, 18, 19], "batch size {batch_size}");
        }
        // `SELECT id FROM range(0, 20, 1, 1) WHERE id < 5 OR rand(7) < id / 40.0` in Spark 4.2.
        let or = |schema: &Schema| -> Result<Arc<dyn PhysicalExpr>> {
            binary(
                binary(col("id", schema)?, Operator::Lt, lit(5_i64), schema)?,
                Operator::Or,
                binary(
                    rand_expr(7, schema)?,
                    Operator::Lt,
                    id_ratio(schema, 40.0)?,
                    schema,
                )?,
                schema,
            )
        };
        for (batch_size, ids) in selected_ids(or).await? {
            assert_eq!(
                ids,
                vec![0, 1, 2, 3, 4, 13, 16, 18],
                "batch size {batch_size}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_lazy_boolean_right_side_projection_keeps_lambda_variables() -> Result<()> {
        // The input columns `other` and `id`, and the parameter `x` of a lambda on the
        // right side, which refers to `id` and `x`.
        let schema = Schema::new(vec![
            Field::new("other", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
        ]);
        let x = Arc::new(LambdaVariable::new(
            2,
            Arc::new(Field::new("x", DataType::Int64, false)),
        )) as Arc<dyn PhysicalExpr>;
        let body = binary(
            binary(x, Operator::Multiply, col("id", &schema)?, &schema)?,
            Operator::Gt,
            lit(0_i64),
            &schema,
        )?;
        let lambda = Arc::new(LambdaExpr::try_new(vec!["x".to_string()], body)?);
        let left = binary(col("id", &schema)?, Operator::Gt, lit(5_i64), &schema)?;
        let expr = LazyBooleanExpr::try_new(left, Operator::And, lambda)?;
        assert_eq!(expr.right_columns, vec![1]);
        let mut columns = vec![];
        let mut variables = vec![];
        expr.projected_right.apply(|expr| {
            if let Some(column) = expr.downcast_ref::<Column>() {
                columns.push(column.index());
            }
            if let Some(variable) = expr.downcast_ref::<LambdaVariable>() {
                variables.push(variable.index());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        // The input column is projected, and the lambda variable keeps its index after the
        // input columns.
        assert_eq!(columns, vec![0]);
        assert_eq!(variables, vec![2]);
        Ok(())
    }
}
