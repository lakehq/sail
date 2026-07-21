/// [CREDIT]: https://github.com/apache/datafusion/blob/f911d529a57b211eb44a98b253f97d839f60019f/datafusion/functions-nested/src/lambda_utils.rs
///
/// Shared helpers for `(array, lambda)` higher-order array functions
/// (`filter`, `transform`, `exists`, `forall`, ...).
///
/// Mirrors the `pub(crate)` helpers in `datafusion-functions-nested` 54's
/// `src/lambda_utils.rs`, kept here so every Spark HOF reuses the same
/// argument-unpacking and list-coercion logic instead of duplicating it, with
/// the Spark-specific addition of the optional 0-based index parameter
/// (`index_array`/`offsets_to_indices`).
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, Int32Array, OffsetSizeTrait,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::utils::{
    adjust_offsets_for_slice, list_values, list_values_row_number, take_function_args,
};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, LambdaArgument, ValueOrLambda};

use crate::error::generic_exec_err;

/// Extracts a `(value, lambda)` pair from a [`ValueOrLambda`] slice.
pub(crate) fn value_lambda_pair<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a L)> {
    let [value, lambda] = take_function_args(name, args)?;

    let (ValueOrLambda::Value(value), ValueOrLambda::Lambda(lambda)) = (value, lambda) else {
        return plan_err!(
            "{name} expects a value followed by a lambda, got {value:?} and {lambda:?}"
        );
    };

    Ok((value, lambda))
}

/// Coerces a single list argument for `(array, lambda)` style higher-order functions.
///
/// Normalises `ListView`/`FixedSizeList` → `List` and `LargeListView` → `LargeList`.
pub(crate) fn coerce_single_list_arg(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let list = if arg_types.len() == 1 {
        &arg_types[0]
    } else {
        return plan_err!(
            "{name} function requires 1 value argument, got {}",
            arg_types.len()
        );
    };

    let coerced = match list {
        DataType::List(_) | DataType::LargeList(_) => list.clone(),
        DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::clone(field))
        }
        DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
        _ => return plan_err!("{name} expected a list as first argument, got {list}"),
    };

    Ok(vec![coerced])
}

/// Result of extracting flat list values, with fast-path short-circuits handled.
pub(crate) enum ListValuesResult {
    /// Caller should return this value immediately.
    EarlyReturn(ColumnarValue),
    /// Flat values extracted from the list; continue with execution.
    Values(ArrayRef),
}

/// Extracts flat list values, handling all fast-path short-circuits.
///
/// - All-null input → `EarlyReturn(null scalar)`
/// - All sublists empty and non-null → `EarlyReturn(default empty-list scalar)`
/// - Otherwise → `Values(flat_values)`
pub(crate) fn extract_list_values(
    list_array: &ArrayRef,
    return_type: &DataType,
) -> Result<ListValuesResult> {
    if list_array.null_count() == list_array.len() {
        return Ok(ListValuesResult::EarlyReturn(ColumnarValue::Scalar(
            ScalarValue::try_new_null(return_type)?,
        )));
    }

    let values = list_values(list_array)?;

    if values.is_empty()
        && list_array.null_count() == 0
        && matches!(return_type, DataType::List(_) | DataType::LargeList(_))
    {
        return Ok(ListValuesResult::EarlyReturn(ColumnarValue::Scalar(
            ScalarValue::new_default(return_type)?,
        )));
    }

    Ok(ListValuesResult::Values(values))
}

/// Reinterprets a `Null`-typed lambda result as a boolean that is NULL everywhere.
///
/// An untyped `NULL` body (`x -> NULL`, or a bare `NULL` in a lambda position)
/// carries the `Null` type rather than the boolean the predicate functions
/// require. Spark coerces it to the required type, so the predicate is simply
/// NULL for every element; without this the downcast below rejects it.
pub(crate) fn coerce_null_lambda_result(result: ArrayRef) -> ArrayRef {
    if result.data_type() == &DataType::Null {
        Arc::new(BooleanArray::new_null(result.len()))
    } else {
        result
    }
}

/// Evaluates a boolean lambda element by element, stopping each row at the first
/// `stop_on` value, and reduces with Spark's three-valued logic.
///
/// Spark's `ArrayExists`/`ArrayForAll` walk the elements in order and stop at the
/// first `true`/`false`, so an element past the stopping point is never evaluated
/// and can never raise. Evaluating the whole flattened batch at once — which is
/// what the vectorized path does — raises on elements Spark would have skipped.
/// This is the recovery path for exactly that case: it costs one lambda
/// evaluation per element, so callers must only reach it once the vectorized
/// evaluation has already failed.
pub(crate) fn short_circuit_boolean_reduce(
    name: &str,
    list_array: &ArrayRef,
    values: &ArrayRef,
    lambda: &LambdaArgument,
    stop_on: bool,
) -> Result<BooleanArray> {
    let (offsets, nulls) = match list_array.data_type() {
        DataType::List(_) => {
            let list = list_array.as_list::<i32>();
            (
                offsets_to_usize(&adjust_offsets_for_slice(list)),
                list.nulls().cloned(),
            )
        }
        DataType::LargeList(_) => {
            let list = list_array.as_list::<i64>();
            (
                offsets_to_usize(&adjust_offsets_for_slice(list)),
                list.nulls().cloned(),
            )
        }
        other => return exec_err!("{name} expected list, got {other}"),
    };

    let row_numbers = list_values_row_number(list_array)?;
    let num_rows = list_array.len();
    let mut builder = BooleanBuilder::with_capacity(num_rows);

    for row in 0..num_rows {
        if nulls.as_ref().is_some_and(|n| n.is_null(row)) {
            builder.append_null();
            continue;
        }
        let mut stopped = false;
        let mut any_null = false;
        for index in offsets[row]..offsets[row + 1] {
            let element = values.slice(index, 1);
            let element_param = || Ok(Arc::clone(&element));
            let params: [&dyn Fn() -> Result<ArrayRef>; 1] = [&element_param];
            let output = lambda.evaluate(&params, |arrays| {
                Ok(take_arrays(arrays, &row_numbers.slice(index, 1), None)?)
            })?;
            match single_boolean(name, &output)? {
                Some(value) if value == stop_on => {
                    stopped = true;
                    break;
                }
                Some(_) => {}
                None => any_null = true,
            }
        }
        if stopped {
            builder.append_value(stop_on);
        } else if any_null {
            builder.append_null();
        } else {
            builder.append_value(!stop_on);
        }
    }

    Ok(builder.finish())
}

fn offsets_to_usize<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> Vec<usize> {
    offsets.iter().map(|offset| offset.as_usize()).collect()
}

fn single_boolean(name: &str, output: &ColumnarValue) -> Result<Option<bool>> {
    if let ColumnarValue::Scalar(ScalarValue::Boolean(value)) = output {
        return Ok(*value);
    }
    let array = output.clone().into_array(1)?;
    let Some(array) = array.as_any().downcast_ref::<BooleanArray>() else {
        return exec_err!(
            "{name} lambda must return boolean, got {}",
            array.data_type()
        );
    };
    if array.is_null(0) {
        Ok(None)
    } else {
        Ok(Some(array.value(0)))
    }
}

/// 0-based per-sublist positions aligned with the flattened values of `list_array`.
pub(crate) fn index_array(name: &str, list_array: &ArrayRef) -> Result<ArrayRef> {
    match list_array.data_type() {
        DataType::List(_) => {
            offsets_to_indices(name, &adjust_offsets_for_slice(list_array.as_list::<i32>()))
        }
        DataType::LargeList(_) => {
            offsets_to_indices(name, &adjust_offsets_for_slice(list_array.as_list::<i64>()))
        }
        other => exec_err!("{name} expected list, got {other}"),
    }
}

fn offsets_to_indices<O: OffsetSizeTrait>(
    name: &str,
    offsets: &OffsetBuffer<O>,
) -> Result<ArrayRef> {
    let total = offsets
        .last()
        .map(|o| o.as_usize())
        .unwrap_or(0)
        .saturating_sub(offsets.first().map(|o| o.as_usize()).unwrap_or(0));
    let mut out: Vec<i32> = Vec::with_capacity(total);
    for &[start, end] in offsets.array_windows::<2>() {
        let len = end.as_usize() - start.as_usize();
        let len = i32::try_from(len)
            .map_err(|_| generic_exec_err(name, "array too large for Int32 index"))?;
        out.extend(0..len);
    }
    Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
}
