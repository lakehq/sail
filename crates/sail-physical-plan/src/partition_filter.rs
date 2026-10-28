use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::array::{BooleanArray, Int64Array};
use datafusion::arrow::compute::{and_kleene, or_kleene};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter::{FilterExec, batch_filter};
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties, apply_expression_roots,
};
use datafusion_common::{Result, ScalarValue, Statistics, exec_err};
use datafusion_expr::{ColumnarValue, Operator};
use futures::StreamExt;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId;
use sail_function::scalar::misc::spark_partition_id::SparkPartitionId;

#[derive(Debug)]
pub struct PartitionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpr>,
    properties: Arc<PlanProperties>,
}

impl PartitionFilterExec {
    /// Called during extension planning, before physical optimization can move
    /// filters, sorts, or repartitions across the stateful predicate.
    pub fn rewrite(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(filter) = plan.downcast_ref::<FilterExec>() else {
                return Ok(Transformed::no(plan));
            };
            let stateful = filter.predicate().exists(|expr| {
                Ok(expr
                    .downcast_ref::<ScalarFunctionExpr>()
                    .is_some_and(|function| {
                        function
                            .fun()
                            .inner()
                            .is::<SparkMonotonicallyIncreasingId>()
                    }))
            })?;
            if !stateful {
                return Ok(Transformed::no(plan));
            }
            let mut output: Arc<dyn ExecutionPlan> = Arc::new(Self::new(
                filter.input().clone(),
                filter.predicate().clone(),
            ));
            if let Some(projection) = filter.projection() {
                let schema = output.schema();
                let exprs = projection
                    .iter()
                    .map(|index| {
                        let name = schema.field(*index).name();
                        ProjectionExpr {
                            expr: Arc::new(Column::new(name, *index)),
                            alias: name.clone(),
                        }
                    })
                    .collect::<Vec<_>>();
                output = Arc::new(ProjectionExec::try_new(exprs, output)?);
            }
            if let Some(fetch) = filter.fetch() {
                output = Arc::new(LocalLimitExec::new(output, fetch));
            }
            Ok(Transformed::yes(output))
        })
        .data()
    }

    pub fn new(input: Arc<dyn ExecutionPlan>, predicate: Arc<dyn PhysicalExpr>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            predicate,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }
}

impl DisplayAs for PartitionFilterExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}: {}", Self::static_name(), self.predicate)
    }
}

impl ExecutionPlan for PartitionFilterExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    // Do not advertise maintains_input_order: pushing a sort through this node
    // would change the rows assigned each monotonically increasing ID.

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots([&self.predicate], f)
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
        Ok(Arc::new(Self::new(
            children.one()?,
            Arc::clone(&self.predicate),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Bind fresh state for every occurrence, partition, and execution. The
        // serializable plan retains only the unbound scalar function markers.
        let predicate = self
            .predicate
            .clone()
            .transform_up(|expr| {
                if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() {
                    if function
                        .fun()
                        .inner()
                        .is::<SparkMonotonicallyIncreasingId>()
                    {
                        return Ok(Transformed::yes(Arc::new(MonotonicIdExpr {
                            partition,
                            offset: AtomicU64::new(0),
                        })
                            as Arc<dyn PhysicalExpr>));
                    }
                    if function.fun().inner().is::<SparkPartitionId>() {
                        return Ok(Transformed::yes(
                            Arc::new(Literal::new(ScalarValue::Int32(Some(partition as i32))))
                                as Arc<dyn PhysicalExpr>,
                        ));
                    }
                }
                if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
                    && matches!(binary.op(), Operator::And | Operator::Or)
                {
                    return Ok(Transformed::yes(Arc::new(ShortCircuitExpr {
                        left: binary.left().clone(),
                        op: *binary.op(),
                        right: binary.right().clone(),
                    })
                        as Arc<dyn PhysicalExpr>));
                }
                Ok(Transformed::no(expr))
            })
            .data()?;
        let stream = self
            .input
            .execute(partition, context)?
            .map(move |batch| filter_batch(batch?, &predicate));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(input_stats[0].as_ref().clone().to_inexact()))
    }
}

fn filter_batch(batch: RecordBatch, predicate: &Arc<dyn PhysicalExpr>) -> Result<RecordBatch> {
    // Spark evaluates filter conjuncts in order and drops both false and null
    // rows before evaluating the next conjunct.
    if let Some(expr) = predicate.downcast_ref::<ShortCircuitExpr>()
        && expr.op == Operator::And
    {
        let batch = filter_batch(batch, &expr.left)?;
        if batch.num_rows() == 0 {
            return Ok(batch);
        }
        filter_batch(batch, &expr.right)
    } else {
        batch_filter(&batch, predicate)
    }
}

#[derive(Debug)]
struct MonotonicIdExpr {
    partition: usize,
    offset: AtomicU64,
}

// Stateful occurrences must never compare equal to another occurrence.
impl PartialEq for MonotonicIdExpr {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for MonotonicIdExpr {}

impl Hash for MonotonicIdExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(self, state);
    }
}

impl Display for MonotonicIdExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "monotonically_increasing_id()")
    }
}

impl PhysicalExpr for MonotonicIdExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let len = batch.num_rows() as u64;
        let start = self.offset.fetch_add(len, Ordering::Relaxed);
        if start + len > (1u64 << 33) {
            return exec_err!(
                "monotonically_increasing_id overflow: exceeded 2^33 rows in one partition"
            );
        }
        let base = (self.partition as u64) << 33;
        let values = Int64Array::from_iter_values((start..start + len).map(|i| (base + i) as i64));
        Ok(ColumnarValue::Array(Arc::new(values)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        children.zero()?;
        Ok(self)
    }

    fn is_volatile_node(&self) -> bool {
        true
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// DataFusion's AND/OR selection is an optional optimization. Stateful
/// expressions require short-circuiting for every batch, including null inputs.
#[derive(Debug, Eq)]
struct ShortCircuitExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl PartialEq for ShortCircuitExpr {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.left.eq(&other.left) && self.right.eq(&other.right)
    }
}

impl Hash for ShortCircuitExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.op.hash(state);
        self.right.hash(state);
    }
}

impl Display for ShortCircuitExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

impl PhysicalExpr for ShortCircuitExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let left = self.left.evaluate(batch)?.into_array(batch.num_rows())?;
        let left = as_boolean_array(&left)?;
        let short_circuit_value = self.op == Operator::Or;
        let selection = BooleanArray::from_iter(
            left.iter()
                .map(|value| Some(value != Some(short_circuit_value))),
        );
        let right = self
            .right
            .evaluate_selection(batch, &selection)?
            .into_array(batch.num_rows())?;
        let right = as_boolean_array(&right)?;
        let result = if self.op == Operator::And {
            and_kleene(left, right)?
        } else {
            or_kleene(left, right)?
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
        let (left, right) = children.two()?;
        Ok(Arc::new(Self {
            left,
            op: self.op,
            right,
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}
