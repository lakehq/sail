use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::Deref;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/max_min_by.rs>
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::first_last::last_value_udaf;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::simplify::SimplifyInfo;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{function, Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::prelude::Expr;

#[derive(Debug)]
struct MaxMinByAccumulator {
    value: ScalarValue,
    ordering: ScalarValue,
    is_max: bool,
}

impl MaxMinByAccumulator {
    fn new(
        value_type: &DataType,
        ordering_type: &DataType,
        is_max: bool,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            value: ScalarValue::try_from(value_type)?,
            ordering: ScalarValue::try_from(ordering_type)?,
            is_max,
        })
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let value_array = &values[0];
        let ordering_array = &values[1];

        for i in 0..ordering_array.len() {
            if ordering_array.is_null(i) {
                continue;
            }
            let ordering_val = ScalarValue::try_from_array(&values[1], i)?;
            let should_update = if self.ordering.is_null() {
                true
            } else {
                match ordering_val.partial_cmp(&self.ordering) {
                    Some(Ordering::Greater) => self.is_max,
                    Some(Ordering::Less) => !self.is_max,
                    _ => false,
                }
            };
            if should_update {
                self.value = ScalarValue::try_from_array(value_array, i)?;
                self.ordering = ordering_val;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        Ok(vec![self.value.clone(), self.ordering.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        self.value.size() + self.ordering.size() + std::mem::size_of::<bool>()
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct MaxByFunction {
    signature: Signature,
}

impl Debug for MaxByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MaxBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}
impl Default for MaxByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(input_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl AggregateUDFImpl for MaxByFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            true,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let ordering_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "value"), value_type, true).into(),
            Field::new(
                format_state_name(args.name, "ordering"),
                ordering_type,
                true,
            )
            .into(),
        ])
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: AggregateFunction, _: &dyn SimplifyInfo| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let null_filter = second_arg.clone().is_not_null();
            let filter = match aggr_func.params.filter {
                Some(existing) => Some(Box::new((*existing).and(null_filter))),
                None => Some(Box::new(null_filter)),
            };

            order_by.push(Sort::new(second_arg, true, true)); // ASC,  NULLS FIRST

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![first_arg],
                aggr_func.params.distinct,
                filter,
                order_by,
                aggr_func.params.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct MinByFunction {
    signature: Signature,
}

impl Debug for MinByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MinBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for MinByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MinByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for MinByFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            false,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let ordering_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "value"), value_type, true).into(),
            Field::new(
                format_state_name(args.name, "ordering"),
                ordering_type,
                true,
            )
            .into(),
        ])
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: AggregateFunction, _: &dyn SimplifyInfo| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let null_filter = second_arg.clone().is_not_null();
            let filter = match aggr_func.params.filter {
                Some(existing) => Some(Box::new((*existing).and(null_filter))),
                None => Some(Box::new(null_filter)),
            };

            order_by.push(Sort::new(second_arg, false, true)); // DESC, NULLS FIRST

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![first_arg],
                aggr_func.params.distinct,
                filter,
                order_by,
                aggr_func.params.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }
}
