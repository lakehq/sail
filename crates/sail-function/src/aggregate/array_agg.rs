use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::array_agg;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature,
    StatisticsArgs,
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
        self.inner.accumulator(acc_args)
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
        self.inner.create_groups_accumulator(args)
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
