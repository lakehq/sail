use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs,
};
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature,
};
use sail_common::spec::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, SAIL_INTERVAL_EXTENSION_NAME,
};

/// Wraps an inner `AggregateUDF` and forces its output `Field` to carry the
/// Sail interval qualifier metadata from its first input argument. Used to
/// preserve the qualifier on aggregates whose result type matches the input
/// type (e.g. `MIN(INTERVAL '1' DAY)` → `INTERVAL '1' DAY`). DataFusion's
/// built-in `min` / `max` / `sum` UDAFs use the default `return_field` which
/// drops input metadata, so without this wrapper the result renders via the
/// broadest qualifier formatter (`DAY TO SECOND` / `YEAR TO MONTH`) instead
/// of the input's narrower qualifier.
///
/// `as_any` deliberately forwards to the inner UDAF so DataFusion's
/// optimizers can still downcast on concrete types like `Min` / `Max` /
/// `Sum`. Every other method delegates to the inner UDAF, so accumulator
/// behavior, signatures, simplification rules, etc. are unchanged.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PreserveInputMetadataAggregate {
    inner: Arc<AggregateUDF>,
}

impl PreserveInputMetadataAggregate {
    pub fn new(inner: Arc<AggregateUDF>) -> Self {
        Self { inner }
    }
}

impl AggregateUDFImpl for PreserveInputMetadataAggregate {
    /// Forward to the inner UDAF's `as_any` so DataFusion optimizers that
    /// downcast on concrete UDAF types (`Min`, `Max`, `Sum`, ...) continue
    /// to find them through the wrapper. The wrapper itself is intentionally
    /// not downcastable — no caller in Sail needs to recover it.
    fn as_any(&self) -> &dyn Any {
        self.inner.inner().as_any()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
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

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    /// Copy the Sail interval extension metadata from the first input field
    /// onto the inner UDAF's return field. Only the Sail interval keys are
    /// propagated — other input metadata is left to the inner UDAF, which
    /// already handles e.g. dictionary-encoded inputs via its default
    /// `return_field` implementation.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let base = self.inner.return_field(arg_fields)?;
        let Some(first) = arg_fields.first() else {
            return Ok(base);
        };
        let in_meta = first.metadata();
        if in_meta.get(EXTENSION_TYPE_NAME_KEY).map(String::as_str)
            != Some(SAIL_INTERVAL_EXTENSION_NAME)
        {
            return Ok(base);
        }
        let Some(payload) = in_meta.get(EXTENSION_TYPE_METADATA_KEY) else {
            return Ok(base);
        };
        let mut combined = base.metadata().clone();
        combined.insert(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            SAIL_INTERVAL_EXTENSION_NAME.to_string(),
        );
        combined.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), payload.clone());
        Ok(Arc::new(base.as_ref().clone().with_metadata(combined)))
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
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

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.inner().reverse_expr()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}
