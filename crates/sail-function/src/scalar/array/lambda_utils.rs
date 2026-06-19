//! Shared helpers for Spark array higher-order functions (`filter`, `transform`, ...).
//!
//! Mirrors the `pub(crate)` helpers in `datafusion-functions-nested` 54's
//! `src/lambda_utils.rs`, with the Spark-specific addition of the optional
//! 0-based index parameter (`index_array`/`offsets_to_indices`).
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, OffsetSizeTrait};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::utils::{adjust_offsets_for_slice, list_values, take_function_args};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ValueOrLambda};

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
    for w in offsets.windows(2) {
        let len = w[1].as_usize() - w[0].as_usize();
        let len = i32::try_from(len)
            .map_err(|_| generic_exec_err(name, "array too large for Int32 index"))?;
        out.extend(0..len);
    }
    Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
}
