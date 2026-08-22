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
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility, function};
use datafusion::prelude::Expr;
use sail_common_datafusion::ordering::is_orderable;

use crate::error::{generic_exec_err, invalid_arg_count_exec_err};

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

/// The accepted argument count, which governs both the user-facing call surface and the
/// partial-aggregation state, always a `(value, ordering)` pair.
///
/// Spark 4.2 resolves `max_by` through `MaxByBuilder`, which accepts 2 or 3 arguments and
/// dispatches the 3-argument top-k form to `MaxMinByK`. Only the 2-argument form is
/// implemented here, so a third argument is rejected rather than silently dropped. Adding
/// top-k means widening this to `(2, 3)` and splitting off a separate constant for the
/// accumulator, which stays a pair regardless of the form.
const MAX_MIN_BY_ARG_COUNT: (i32, i32) = (2, 2);

/// Splits the arguments into the value and the ordering.
///
/// The signature is `user_defined`, so DataFusion runs no arity check of its own. Every
/// hook that reaches for the ordering argument must go through here, or it indexes a
/// short slice and panics instead of reporting an error.
fn max_min_by_args<'a, T>(
    function_name: &str,
    args: &'a [T],
) -> Result<(&'a T, &'a T), DataFusionError> {
    match args {
        [value, ordering] => Ok((value, ordering)),
        _ => Err(invalid_arg_count_exec_err(
            function_name,
            MAX_MIN_BY_ARG_COUNT,
            args.len(),
        )),
    }
}

fn get_min_max_by_result_type(
    function_name: &str,
    input_types: &[DataType],
) -> Result<Vec<DataType>, DataFusionError> {
    let (value_type, ordering_type) = max_min_by_args(function_name, input_types)?;
    if !is_orderable(ordering_type) {
        return Err(generic_exec_err(
            function_name,
            &format!("does not support ordering on type {ordering_type}"),
        ));
    }
    // Answering a shorter list than it was given makes DataFusion reject the call with a
    // `Failed to coerce arguments` planning error before any hook below runs, so every
    // argument is carried over and only the ones that need rewriting are replaced.
    let mut coerced = input_types.to_vec();
    // The value type is unwrapped from a dictionary so that the accumulator stores the plain
    // value. Not covered by a scenario because a `Dictionary` column cannot be built from SQL.
    if let DataType::Dictionary(_, dict_value_type) = value_type {
        // TODO add checker, if the value type is complex data type
        if let Some(first) = coerced.first_mut() {
            *first = dict_value_type.deref().clone();
        }
    }
    Ok(coerced)
}

impl AggregateUDFImpl for MaxByFunction {
    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        let (value_type, _) = max_min_by_args(self.name(), arg_types)?;
        Ok(value_type.to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let (_, ordering) = max_min_by_args(self.name(), acc_args.exprs)?;
        let ordering_type = ordering.data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            true,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let (_, ordering) = max_min_by_args(self.name(), args.input_fields)?;
        let ordering_type = ordering.data_type().clone();
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
        let function_name = self.name().to_string();
        let simplify = move |mut aggr_func: AggregateFunction, _: &SimplifyContext| {
            max_min_by_args(&function_name, &aggr_func.params.args)?;
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

            // `NullType` is orderable in Spark, so the orderability check above now lets
            // `max_by(x, NULL)` and `max_by(x, CAST(NULL AS VOID))` through to here. Sorting by
            // a literal is a no-op, and a constant sort key makes DataFusion's ordered
            // `last_value` panic on a global aggregate, so the key is only pushed when it is
            // not one. Foldable arguments reach this point already reduced to a literal.
            if !matches!(second_arg, Expr::Literal(_, _)) {
                order_by.push(Sort::new(second_arg, true, true)); // ASC,  NULLS FIRST
            }

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
        get_min_max_by_result_type(self.name(), arg_types)
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
    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        let (value_type, _) = max_min_by_args(self.name(), arg_types)?;
        Ok(value_type.to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let (_, ordering) = max_min_by_args(self.name(), acc_args.exprs)?;
        let ordering_type = ordering.data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            false,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let (_, ordering) = max_min_by_args(self.name(), args.input_fields)?;
        let ordering_type = ordering.data_type().clone();
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
        let function_name = self.name().to_string();
        let simplify = move |mut aggr_func: AggregateFunction, _: &SimplifyContext| {
            max_min_by_args(&function_name, &aggr_func.params.args)?;
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

            // `NullType` is orderable in Spark, so the orderability check above now lets
            // `min_by(x, NULL)` and `min_by(x, CAST(NULL AS VOID))` through to here. Sorting by
            // a literal is a no-op, and a constant sort key makes DataFusion's ordered
            // `last_value` panic on a global aggregate, so the key is only pushed when it is
            // not one. Foldable arguments reach this point already reduced to a literal.
            if !matches!(second_arg, Expr::Literal(_, _)) {
                order_by.push(Sort::new(second_arg, false, true)); // DESC, NULLS FIRST
            }

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
        get_min_max_by_result_type(self.name(), arg_types)
    }
}
