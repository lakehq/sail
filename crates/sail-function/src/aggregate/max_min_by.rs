use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/max_min_by.rs>
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::functions_aggregate::first_last::last_value_udaf;
use datafusion::functions_nested::expr_fn::array_slice;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility, function};
use datafusion::prelude::{Expr, lit};

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
        let function_name = if self.is_max { "max_by" } else { "min_by" };
        let (value_array, ordering_array) = max_min_by_args(function_name, values)?;

        for i in 0..ordering_array.len() {
            if ordering_array.is_null(i) {
                continue;
            }
            let ordering_val = ScalarValue::try_from_array(ordering_array, i)?;
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

/// The argument count Spark 4.2 accepts: `MaxByBuilder`/`MinByBuilder` route the
/// 3-argument form to the top-k variant `MaxMinByK`, which returns `ARRAY<value type>`.
const MAX_MIN_BY_ARG_COUNT: (i32, i32) = (2, 3);

/// The argument count the hooks that build a real accumulator accept.
///
/// The top-k form is implemented as a logical rewrite in [`max_min_by_simplify`], which the
/// window path never runs, so an accumulator is only ever built for the 2-argument form.
const MAX_MIN_BY_ACCUMULATOR_ARG_COUNT: (i32, i32) = (2, 2);

/// `MaxMinByK.MAX_K`.
const MAX_MIN_BY_MAX_K: i64 = 100_000;

/// Splits the arguments into the value and the ordering one.
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
            MAX_MIN_BY_ACCUMULATOR_ARG_COUNT,
            args.len(),
        )),
    }
}

/// Splits the arguments of either form, returning the top-k argument when present.
fn max_min_by_top_k_args<'a, T>(
    function_name: &str,
    args: &'a [T],
) -> Result<(&'a T, &'a T, Option<&'a T>), DataFusionError> {
    match args {
        [value, ordering] => Ok((value, ordering, None)),
        [value, ordering, k] => Ok((value, ordering, Some(k))),
        _ => Err(invalid_arg_count_exec_err(
            function_name,
            MAX_MIN_BY_ARG_COUNT,
            args.len(),
        )),
    }
}

/// Reads the top-k argument, which Spark requires to be foldable and within `[1, MAX_K]`.
///
/// Spark's rule is foldability, not "is a literal", so a constant-folded expression such as
/// `1 + 1` qualifies. This runs inside `simplify`, after constant folding, so those reach
/// here already reduced to a literal. A `Cast` around the literal is looked through because
/// the argument is coerced to an integer before this point.
fn max_min_by_k(function_name: &str, k_expr: &Expr) -> Result<i64, DataFusionError> {
    let scalar = match k_expr {
        Expr::Literal(scalar, _) => Some(scalar),
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::Literal(scalar, _) => Some(scalar),
            _ => None,
        },
        _ => None,
    };
    let k = match scalar {
        Some(ScalarValue::Int8(Some(v))) => Some(i64::from(*v)),
        Some(ScalarValue::Int16(Some(v))) => Some(i64::from(*v)),
        Some(ScalarValue::Int32(Some(v))) => Some(i64::from(*v)),
        Some(ScalarValue::Int64(Some(v))) => Some(*v),
        _ => None,
    };
    let Some(k) = k else {
        return Err(generic_exec_err(
            function_name,
            &format!("the input k should be a foldable int expression; however, got {k_expr}"),
        ));
    };
    if !(1..=MAX_MIN_BY_MAX_K).contains(&k) {
        return Err(generic_exec_err(
            function_name,
            &format!("The `k` must be between [1, {MAX_MIN_BY_MAX_K}] (current value = {k})"),
        ));
    }
    Ok(k)
}

/// The output type: the value type for the 2-argument form, and `ARRAY<value type>` for the
/// top-k form, matching `MaxMinByK.dataType`.
///
/// The element field is built the same way `array_agg` builds its own, because the top-k form
/// is rewritten to `array_agg` and the optimizer rejects a rewrite that changes the schema.
fn max_min_by_return_type(
    function_name: &str,
    arg_types: &[DataType],
) -> Result<DataType, DataFusionError> {
    let (value_type, _, k) = max_min_by_top_k_args(function_name, arg_types)?;
    match k {
        None => Ok(value_type.clone()),
        Some(_) => Ok(DataType::List(Arc::new(Field::new_list_field(
            value_type.clone(),
            true,
        )))),
    }
}

fn get_min_max_by_result_type(
    function_name: &str,
    input_types: &[DataType],
) -> Result<Vec<DataType>, DataFusionError> {
    let (value_type, _, _) = max_min_by_top_k_args(function_name, input_types)?;
    // Only the value type is rewritten; every other argument is carried over unchanged.
    // Answering a shorter list than it was given makes DataFusion reject the call with a
    // `Failed to coerce arguments` planning error before any hook below runs.
    // Not covered by a scenario because a `Dictionary` column cannot be built from SQL.
    let DataType::Dictionary(_, dict_value_type) = value_type else {
        return Ok(input_types.to_vec());
    };
    // TODO add checker, if the value type is complex data type
    let mut coerced = input_types.to_vec();
    if let Some(first) = coerced.first_mut() {
        *first = dict_value_type.deref().clone();
    }
    Ok(coerced)
}

/// Rewrites both forms into existing aggregates.
///
/// The 2-argument form becomes `last_value` ordered by the ordering argument. The top-k form
/// becomes `array_slice(array_agg(value ORDER BY ordering), 1, k)`, which reproduces
/// `MaxMinByK`: NULL orderings are dropped by the filter, the extreme values come first, and
/// an empty group yields NULL rather than an empty array because `array_agg` returns NULL.
fn max_min_by_simplify(is_max: bool) -> function::AggregateFunctionSimplification {
    Box::new(
        move |mut aggr_func: AggregateFunction, _: &SimplifyContext| {
            let function_name = aggr_func.func.name().to_string();
            max_min_by_top_k_args(&function_name, &aggr_func.params.args)?;

            let k = match aggr_func.params.args.len() {
                3 => Some(max_min_by_k(
                    &function_name,
                    &aggr_func.params.args.remove(2),
                )?),
                _ => None,
            };
            let (ordering_arg, value_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let null_filter = ordering_arg.clone().is_not_null();
            let filter = match aggr_func.params.filter {
                Some(existing) => Some(Box::new((*existing).and(null_filter))),
                None => Some(Box::new(null_filter)),
            };

            let mut order_by = aggr_func.params.order_by;
            match k {
                // `last_value` takes the row at the END of the ordering, so `max_by` sorts
                // ascending and `min_by` descending.
                None => order_by.push(Sort::new(ordering_arg, is_max, true)),
                // `array_agg` keeps every row, so the extreme must come FIRST instead.
                Some(_) => order_by.push(Sort::new(ordering_arg, !is_max, true)),
            }

            let (func, args) = match k {
                None => (last_value_udaf(), vec![value_arg]),
                Some(_) => (array_agg_udaf(), vec![value_arg]),
            };
            let aggregate = Expr::AggregateFunction(AggregateFunction::new_udf(
                func,
                args,
                aggr_func.params.distinct,
                filter,
                order_by,
                aggr_func.params.null_treatment,
            ));
            match k {
                None => Ok(aggregate),
                Some(k) => Ok(array_slice(aggregate, lit(1i64), lit(k), None)),
            }
        },
    )
}

impl AggregateUDFImpl for MaxByFunction {
    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        max_min_by_return_type(self.name(), arg_types)
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
        Some(max_min_by_simplify(true))
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
        max_min_by_return_type(self.name(), arg_types)
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
        Some(max_min_by_simplify(false))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(self.name(), arg_types)
    }
}
