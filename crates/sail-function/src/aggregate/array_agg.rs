use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::array_agg;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF,
    Signature, StatisticsArgs,
};
use datafusion_expr::expr::{AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::function::AggregateFunctionSimplification;
use datafusion_expr::utils::AggregateOrderSensitivity;

#[derive(Clone)]
pub struct SparkArrayAgg {
    inner: Arc<AggregateUDF>,
}

impl SparkArrayAgg {
    pub fn new() -> Self {
        Self {
            inner: array_agg::array_agg_udaf(),
        }
    }

    fn with_inner(inner: Arc<AggregateUDF>) -> Self {
        Self { inner }
    }

    fn output_value_field(return_field: &FieldRef) -> Option<FieldRef> {
        match return_field.data_type() {
            DataType::List(field) => Some(Arc::clone(field)),
            _ => None,
        }
    }
}

fn with_list_value_field(array: &ListArray, value_field: &FieldRef) -> ListArray {
    ListArray::new(
        Arc::clone(value_field),
        array.offsets().clone(),
        Arc::clone(array.values()),
        array.nulls().cloned(),
    )
}

fn with_list_scalar_value_field(value: ScalarValue, value_field: Option<&FieldRef>) -> ScalarValue {
    match (value, value_field) {
        (ScalarValue::List(array), Some(value_field)) => {
            ScalarValue::List(Arc::new(with_list_value_field(array.as_ref(), value_field)))
        }
        (value, _) => value,
    }
}

#[derive(Debug)]
struct SparkArrayAggAccumulator {
    inner: Box<dyn Accumulator>,
    output_value_field: Option<FieldRef>,
}

impl SparkArrayAggAccumulator {
    fn new(inner: Box<dyn Accumulator>, output_value_field: Option<FieldRef>) -> Self {
        Self {
            inner,
            output_value_field,
        }
    }
}

impl Accumulator for SparkArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.inner.evaluate()?;
        Ok(with_list_scalar_value_field(
            value,
            self.output_value_field.as_ref(),
        ))
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(values)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

struct SparkArrayAggGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    output_value_field: Option<FieldRef>,
}

impl SparkArrayAggGroupsAccumulator {
    fn new(inner: Box<dyn GroupsAccumulator>, output_value_field: Option<FieldRef>) -> Self {
        Self {
            inner,
            output_value_field,
        }
    }

    fn restore_output_field(&self, array: ArrayRef) -> ArrayRef {
        match (
            array.as_ref().as_any().downcast_ref::<ListArray>(),
            self.output_value_field.as_ref(),
        ) {
            (Some(array), Some(value_field)) => Arc::new(with_list_value_field(array, value_field)),
            _ => array,
        }
    }
}

impl GroupsAccumulator for SparkArrayAggGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let array = self.inner.evaluate(emit_to)?;
        Ok(self.restore_output_field(array))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.inner.supports_convert_to_state()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl Default for SparkArrayAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for SparkArrayAgg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkArrayAgg").finish()
    }
}

impl PartialEq for SparkArrayAgg {
    fn eq(&self, other: &Self) -> bool {
        self.inner.name() == other.inner.name()
            && self.inner.order_sensitivity() == other.inner.order_sensitivity()
    }
}

impl Eq for SparkArrayAgg {}

impl Hash for SparkArrayAgg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.name().hash(state);
        format!("{:?}", self.inner.order_sensitivity()).hash(state);
    }
}

impl AggregateUDFImpl for SparkArrayAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let input = arg_fields[0].as_ref();
        let value_field = Field::new_list_field(input.data_type().clone(), true)
            .with_metadata(input.metadata().clone());
        Ok(Arc::new(Field::new_list("array_agg", value_field, true)))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let output_value_field = Self::output_value_field(&acc_args.return_field);
        let accumulator = self.inner.accumulator(acc_args)?;
        Ok(Box::new(SparkArrayAggAccumulator::new(
            accumulator,
            output_value_field,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let output_value_field = Self::output_value_field(&args.return_field);
        let accumulator = self.inner.create_groups_accumulator(args)?;
        Ok(Box::new(SparkArrayAggGroupsAccumulator::new(
            accumulator,
            output_value_field,
        )))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        self.inner
            .as_ref()
            .clone()
            .with_beneficial_ordering(beneficial_ordering)
            .map(|udf| {
                udf.map(|udf| {
                    Arc::new(Self::with_inner(Arc::new(udf))) as Arc<dyn AggregateUDFImpl>
                })
            })
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_udf()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner.schema_name(params)
    }

    fn human_display(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner.human_display(params)
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner.display_name(params)
    }

    fn window_function_schema_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner.window_function_schema_name(params)
    }

    fn window_function_display_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner.window_function_display_name(params)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }
}
