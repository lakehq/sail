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
use datafusion::arrow::datatypes::{DataType, Field};
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

/// Normalizes a list array at runtime to `ListArray` or `LargeListArray`.
///
/// This mirrors the type coercion logic in `coerce_single_list_arg` but operates on
/// runtime arrays rather than types. It converts:
/// - `ListView`/`FixedSizeList` → `ListArray`
/// - `LargeListView` → `LargeListArray`
/// - `List`/`LargeList` → pass through unchanged
///
/// This ensures that higher-order functions can accept all Arrow list variants
/// at runtime, matching what the type coercion already allows at planning time.
pub(crate) fn normalize_list_array(array: ArrayRef) -> Result<ArrayRef> {
    use datafusion::arrow::array::{FixedSizeListArray, ListArray};
    use datafusion::arrow::compute::cast;

    match array.data_type() {
        DataType::List(_) | DataType::LargeList(_) => Ok(array),
        DataType::ListView(_) | DataType::LargeListView(_) => {
            // Cast ListView/LargeListView to List/LargeList
            let target_type = match array.data_type() {
                DataType::ListView(field) => DataType::List(Arc::clone(field)),
                DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
                _ => unreachable!(),
            };
            cast(&array, &target_type).map_err(|e| e.into())
        }
        DataType::FixedSizeList(field, _) => {
            // Convert FixedSizeList to List by constructing a new ListArray
            let fixed_list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast FixedSizeListArray".to_string(),
                    )
                })?;

            let list_field = Arc::new(Field::new(
                Field::LIST_FIELD_DEFAULT_NAME,
                DataType::List(Arc::clone(field)),
                fixed_list.nulls().is_some(),
            ));

            // Build offsets for each sublist
            let size = fixed_list.value_length() as usize;
            let num_rows = fixed_list.len();
            let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
            let mut next = 0i32;
            offsets.push(next);
            for _ in 0..num_rows {
                next += size as i32;
                offsets.push(next);
            }

            let offsets = OffsetBuffer::new(offsets.into());
            let list_array = ListArray::new(
                list_field,
                offsets,
                fixed_list.values().clone(),
                fixed_list.nulls().cloned(),
            );

            Ok(Arc::new(list_array) as ArrayRef)
        }
        other => plan_err!("expected a list array, got {other}"),
    }
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    // FieldRef is needed for type annotations
    use datafusion::arrow::datatypes::FieldRef;

    use super::*;

    fn build_test_list_array(values: &[i32], offsets: &[i32]) -> Arc<ListArray> {
        let values_array = Arc::new(Int32Array::from(values.to_vec()));
        let offset_buffer = OffsetBuffer::new(offsets.to_vec().into());
        let field: FieldRef = Arc::new(Field::new("item", DataType::Int32, true));
        Arc::new(ListArray::new(field, offset_buffer, values_array, None))
    }

    #[test]
    fn test_normalize_list_array_pass_through() -> Result<()> {
        let list: ArrayRef = build_test_list_array(&[1, 2, 3, 4], &[0, 2, 4]);
        let result = normalize_list_array(list)?;
        assert_eq!(result.len(), 2);
        Ok(())
    }

    #[test]
    fn test_coerce_single_list_arg_with_non_list() {
        let result = coerce_single_list_arg("test", &[DataType::Int32]);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_single_list_arg_empty_input() {
        let result = coerce_single_list_arg("test", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_empty_args() {
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_single_arg() {
        let value = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))));
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![value];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_too_many_args() {
        let v1 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))));
        let v2 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(2))));
        let v3 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(3))));
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![v1, v2, v3];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_array_error_non_list_array() {
        let non_list: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = index_array("test", &non_list);
        assert!(result.is_err());
    }

    #[test]
    fn test_offsets_to_indices_single_element() -> Result<()> {
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2].into());
        let result = offsets_to_indices("test", &offsets)?;
        let expected = Int32Array::from(vec![0, 0]);
        let Some(actual) = result.as_any().downcast_ref::<Int32Array>() else {
            return Err(datafusion_common::DataFusionError::Internal(
                "offset conversion should return Int32Array".to_string(),
            ));
        };
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_offsets_to_indices_empty_arrays() -> Result<()> {
        let offsets = OffsetBuffer::new(vec![0i32, 0, 2].into());
        let result = offsets_to_indices("test", &offsets)?;
        let expected = Int32Array::from(vec![0, 1]);
        let Some(actual) = result.as_any().downcast_ref::<Int32Array>() else {
            return Err(datafusion_common::DataFusionError::Internal(
                "offset conversion should return Int32Array".to_string(),
            ));
        };
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_offsets_to_indices_varied_lengths() -> Result<()> {
        let offsets = OffsetBuffer::new(vec![0i32, 3, 5, 6].into());
        let result = offsets_to_indices("test", &offsets)?;
        let expected = Int32Array::from(vec![0, 1, 2, 0, 1, 0]);
        let Some(actual) = result.as_any().downcast_ref::<Int32Array>() else {
            return Err(datafusion_common::DataFusionError::Internal(
                "offset conversion should return Int32Array".to_string(),
            ));
        };
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_normalize_list_array_empty_lists() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![1, 2]));
        let offsets = OffsetBuffer::new(vec![0i32, 0, 2].into());
        let field: FieldRef = Arc::new(Field::new("item", DataType::Int32, true));
        let list: ArrayRef = Arc::new(ListArray::new(field, offsets, values, None));
        let result = normalize_list_array(list)?;
        assert_eq!(result.len(), 2);
        Ok(())
    }
}

#[cfg(any())]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;

    use super::*;

    // Helper to create a simple list array for testing
    fn build_test_list_array(values: &[i32], offsets: &[i32]) -> Arc<ListArray> {
        let values_array = Arc::new(Int32Array::from(values.to_vec()));
        let offset_buffer = OffsetBuffer::new(offsets.to_vec().into());
        let field: FieldRef = Arc::new(Field::new("item", DataType::Int32, true));
        Arc::new(ListArray::new(field, offset_buffer, values_array, None))
    }

    #[test]
    fn test_normalize_list_array_pass_through() {
        // Test that a regular ListArray is returned as-is
        let list: ArrayRef = build_test_list_array(&[1, 2, 3, 4], &[0, 2, 4]);
        let result = normalize_list_array_for_lambda("test", &list).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coerce_single_list_arg_error_not_array() {
        let value = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));
        let result = coerce_single_list_arg_for_lambda("test", &[value]);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_single_list_arg_empty_input() {
        let result = coerce_single_list_arg_for_lambda("test", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_empty_args() {
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_single_arg() {
        let value = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))));
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![value];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_too_many_args() {
        let v1 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))));
        let v2 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(2))));
        let v3 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(3))));
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![v1, v2, v3];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_array_error_non_list_array() {
        let non_list: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = index_array("test", &non_list);
        assert!(result.is_err());
    }

    #[test]
    fn test_offsets_to_indices_with_single_element_arrays() {
        // Arrays with single element each: offsets [0, 1, 2]
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2].into());
        let result = offsets_to_indices("test", &offsets).unwrap();
        let expected = Int32Array::from(vec![0, 0]);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &expected
        );
    }

    #[test]
    fn test_offsets_to_indices_with_empty_arrays() {
        // Array with empty subarray followed by elements: offsets [0, 0, 2]
        let offsets = OffsetBuffer::new(vec![0i32, 0, 2].into());
        let result = offsets_to_indices("test", &offsets).unwrap();
        let expected = Int32Array::from(vec![0, 1]);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &expected
        );
    }

    #[test]
    fn test_offsets_to_indices_with_varied_lengths() {
        // Arrays of varying lengths: offsets [0, 3, 5, 6]
        let offsets = OffsetBuffer::new(vec![0i32, 3, 5, 6].into());
        let result = offsets_to_indices("test", &offsets).unwrap();
        // First: 0, 1, 2 (3 elements)
        // Second: 0, 1 (2 elements)
        // Third: 0 (1 element)
        let expected = Int32Array::from(vec![0, 1, 2, 0, 1, 0]);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &expected
        );
    }

    #[test]
    fn test_normalize_list_array_extracts_empty_list() {
        // Create a regular list array with some empty lists
        let values = Arc::new(Int32Array::from(vec![1, 2]));
        let offsets = OffsetBuffer::new(vec![0i32, 0, 2].into()); // First list is empty
        let field: FieldRef = Arc::new(Field::new("item", DataType::Int32, true));
        let list: ArrayRef = Arc::new(ListArray::new(field, offsets, values, None));

        let result = normalize_list_array_for_lambda("test", &list).unwrap();
        assert_eq!(result.len(), 2);
    }
}

#[cfg(any())]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;

    use super::*;

    // Helper to create a simple list array for testing
    fn build_test_list_array(values: &[i32], offsets: &[i32]) -> Arc<ListArray> {
        let values_array = Arc::new(Int32Array::from(values.to_vec()));
        let offset_buffer = OffsetBuffer::new(offsets.to_vec().into());
        let field: FieldRef = Arc::new(Field::new("item", DataType::Int32, true));
        Arc::new(ListArray::new(field, offset_buffer, values_array, None))
    }

    #[test]
    fn test_normalize_list_array_pass_through() {
        // Test that a regular ListArray is returned as-is
        let list: ArrayRef = build_test_list_array(&[1, 2, 3, 4], &[0, 2, 4]);
        let result = normalize_list_array_for_lambda("test", &list).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coerce_single_list_arg_error_not_array() {
        let value = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));
        let result = coerce_single_list_arg_for_lambda("test", &[value]);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_single_list_arg_error_missing_field_type() {
        // Create a list array reference with explicit type
        let list_array: ArrayRef = Arc::new(ListArray::new_null(
            Arc::new(Field::new("item", DataType::Int32, true)),
            1,
        ));
        let value = ColumnarValue::Array(list_array);
        let result = coerce_single_list_arg_for_lambda("test", &[value]);
        // Should succeed with normalized array
        assert!(result.is_ok());
    }

    #[test]
    fn test_value_lambda_pair_error_empty_args() {
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_single_arg() {
        let value = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))));
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![value];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_lambda_pair_error_too_many_args() {
        let v1 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))));
        let v2 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(2))));
        let v3 = ValueOrLambda::Value(ColumnarValue::Scalar(ScalarValue::Int32(Some(3))));
        let args: Vec<ValueOrLambda<ColumnarValue, ()>> = vec![v1, v2, v3];
        let result = value_lambda_pair::<ColumnarValue, ()>("test", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_array_error_non_list_array() {
        let non_list: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = index_array("test", &non_list);
        assert!(result.is_err());
    }

    #[test]
    fn test_offsets_to_indices_with_single_element_arrays() {
        // Arrays with single element each: offsets [0, 1, 2]
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2].into());
        let result = offsets_to_indices("test", &offsets).unwrap();
        let expected = Int32Array::from(vec![0, 0]);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &expected
        );
    }

    #[test]
    fn test_offsets_to_indices_with_empty_arrays() {
        // Array with empty subarray followed by elements: offsets [0, 0, 2]
        let offsets = OffsetBuffer::new(vec![0i32, 0, 2].into());
        let result = offsets_to_indices("test", &offsets).unwrap();
        let expected = Int32Array::from(vec![0, 1]);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &expected
        );
    }

    #[test]
    fn test_offsets_to_indices_with_varied_lengths() {
        // Arrays of varying lengths: offsets [0, 3, 5, 6]
        let offsets = OffsetBuffer::new(vec![0i32, 3, 5, 6].into());
        let result = offsets_to_indices("test", &offsets).unwrap();
        // First: 0, 1, 2 (3 elements)
        // Second: 0, 1 (2 elements)
        // Third: 0 (1 element)
        let expected = Int32Array::from(vec![0, 1, 2, 0, 1, 0]);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &expected
        );
    }

    #[test]
    fn test_normalize_list_array_extracts_empty_list() {
        // Create a regular list array with some empty lists
        let values = Arc::new(Int32Array::from(vec![1, 2]));
        let offsets = OffsetBuffer::new(vec![0i32, 0, 2].into()); // First list is empty
        let field: FieldRef = Arc::new(Field::new("item", DataType::Int32, true));
        let list: ArrayRef = Arc::new(ListArray::new(field, offsets, values, None));

        let result = normalize_list_array_for_lambda("test", &list).unwrap();
        assert_eq!(result.len(), 2);
    }
}

#[cfg(any())]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::FieldRef;

    use super::*;

    /// Creates a test list array with the given values and offsets
    fn create_test_list(values: Vec<i32>, offsets: Vec<i32>) -> ArrayRef {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        Arc::new(ListArray::new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(Int32Array::from(values)),
            None,
        ))
    }

    /// Creates a FixedSizeList array with the given values
    fn create_fixed_size_list(values: Vec<i32>, size: i32) -> ArrayRef {
        let values = Arc::new(Int32Array::from(values)) as ArrayRef;
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            size,
            values,
            None,
        ))
    }

    #[test]
    fn test_normalize_list_array_pass_through() {
        // Regular ListArray should pass through unchanged
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let normalized = normalize_list_array(list.clone()).unwrap();
        assert!(Arc::ptr_eq(&normalized, &list));
    }

    #[test]
    fn test_normalize_fixed_size_list() {
        // FixedSizeList should be converted to List
        let fixed = create_fixed_size_list(vec![1, 2, 3, 4, 5, 6], 3);
        let normalized = normalize_list_array(fixed).unwrap();
        assert_eq!(
            normalized.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
        );
        // Verify it can be used as a ListArray
        let list = normalized.as_list::<i32>();
        assert_eq!(list.len(), 2); // Two rows with 3 elements each
        assert_eq!(list.value_lengths().values(), &[3, 3]);
    }

    #[test]
    fn test_normalize_list_error_for_non_list() {
        // Non-list types should error
        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = normalize_list_array(int_array);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expected a list array")
        );
    }

    #[test]
    fn test_extract_list_values_all_null() {
        // All-null array should return EarlyReturn with null scalar
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Some(NullBuffer::from(vec![false, false])),
        ));
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::EarlyReturn(ColumnarValue::Scalar(scalar)) => {
                assert!(scalar.is_null());
            }
            _ => panic!("Expected EarlyReturn with null scalar"),
        }
    }

    #[test]
    fn test_extract_list_values_empty_sublists() {
        // All sublists empty should return EarlyReturn with default empty list scalar
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        ));
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::EarlyReturn(ColumnarValue::Scalar(scalar)) => {
                // Should be an empty list
                assert!(!scalar.is_null());
            }
            _ => panic!("Expected EarlyReturn with default empty list"),
        }
    }

    #[test]
    fn test_extract_list_values_normal() {
        // Normal case should return Values
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::Values(values) => {
                assert_eq!(values.len(), 5);
                let ints = values.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(ints.values(), &[1, 2, 3, 4, 5]);
            }
            _ => panic!("Expected Values"),
        }
    }

    #[test]
    fn test_coerce_single_list_arg_errors() {
        // Wrong number of arguments
        let result = coerce_single_list_arg("filter", &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 1 value argument")
        );

        let result = coerce_single_list_arg("filter", &[DataType::Int32, DataType::Int32]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 1 value argument")
        );

        // Non-list type
        let result = coerce_single_list_arg("filter", &[DataType::Int32]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected a list"));
    }

    #[test]
    fn test_coerce_single_list_arg_fixed_size() {
        // FixedSizeList should be coerced to List
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::FixedSizeList(field.clone(), 3)]).unwrap();
        assert_eq!(result, vec![DataType::List(field)]);
    }

    #[test]
    fn test_coerce_single_list_arg_list_view() {
        // ListView should be coerced to List
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::ListView(field.clone())]).unwrap();
        assert_eq!(result, vec![DataType::List(field)]);
    }

    #[test]
    fn test_coerce_single_list_arg_large_list_view() {
        // LargeListView should be coerced to LargeList
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::LargeListView(field.clone())]).unwrap();
        assert_eq!(result, vec![DataType::LargeList(field)]);
    }

    #[test]
    fn test_index_array() {
        // Test that index_array generates correct indices
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let result = index_array("transform", &list).unwrap();
        let indices = result.as_any().downcast_ref::<Int32Array>().unwrap();
        // First row has 3 elements: indices 0, 1, 2
        // Second row has 2 elements: indices 0, 1
        assert_eq!(indices.values(), &[0, 1, 2, 0, 1]);
    }

    #[test]
    fn test_index_array_empty() {
        // Empty list should have no indices
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        ));
        let result = index_array("transform", &list).unwrap();
        let indices = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(indices.is_empty());
    }

    #[test]
    fn test_value_lambda_pair_error_cases() {
        use datafusion::arrow::datatypes::DataType;

        // Create test fields for ValueOrLambda
        let field1 = Arc::new(Field::new("f1", DataType::Int32, true));
        let field2 = Arc::new(Field::new("f2", DataType::Int32, true));

        // Wrong number of arguments - empty
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>("filter", &[]);
        assert!(result.is_err());

        // Wrong number of arguments - only one arg
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[ValueOrLambda::Value(Arc::clone(&field1))],
        );
        assert!(result.is_err());

        // Value in lambda position (two values instead of value + lambda)
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[
                ValueOrLambda::Value(Arc::clone(&field1)),
                ValueOrLambda::Value(Arc::clone(&field2)),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects a value followed by a lambda")
        );

        // Lambda in value position
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[
                ValueOrLambda::Lambda(Some(Arc::clone(&field1))),
                ValueOrLambda::Value(Arc::clone(&field2)),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects a value followed by a lambda")
        );
    }
}

#[cfg(any())]
mod tests {
    use datafusion::arrow::array::{FixedSizeListArray, Int32Array, ListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::Field;

    use super::*;

    /// Creates a test list array with the given values and offsets
    fn create_test_list(values: Vec<i32>, offsets: Vec<i32>) -> ArrayRef {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        Arc::new(ListArray::new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(Int32Array::from(values)),
            None,
        ))
    }

    /// Creates a FixedSizeList array with the given values
    fn create_fixed_size_list(values: Vec<i32>, size: i32) -> ArrayRef {
        let values = Arc::new(Int32Array::from(values)) as ArrayRef;
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            size,
            values,
            None,
        ))
    }

    #[test]
    fn test_normalize_list_array_pass_through() {
        // Regular ListArray should pass through unchanged
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let normalized = normalize_list_array(list.clone()).unwrap();
        assert!(Arc::ptr_eq(&normalized, &list));
    }

    #[test]
    fn test_normalize_fixed_size_list() {
        // FixedSizeList should be converted to List
        let fixed = create_fixed_size_list(vec![1, 2, 3, 4, 5, 6], 3);
        let normalized = normalize_list_array(fixed).unwrap();
        assert_eq!(
            normalized.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
        );
        // Verify it can be used as a ListArray
        let list = normalized.as_list::<i32>();
        assert_eq!(list.len(), 2); // Two rows with 3 elements each
        assert_eq!(list.value_lengths().values(), &[3, 3]);
    }

    #[test]
    fn test_normalize_list_error_for_non_list() {
        // Non-list types should error
        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = normalize_list_array(int_array);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expected a list array")
        );
    }

    #[test]
    fn test_extract_list_values_all_null() {
        // All-null array should return EarlyReturn with null scalar
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Some(NullBuffer::from(vec![false, false])),
        ));
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::EarlyReturn(ColumnarValue::Scalar(scalar)) => {
                assert!(scalar.is_null());
            }
            _ => panic!("Expected EarlyReturn with null scalar"),
        }
    }

    #[test]
    fn test_extract_list_values_empty_sublists() {
        // All sublists empty should return EarlyReturn with default empty list scalar
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        ));
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::EarlyReturn(ColumnarValue::Scalar(scalar)) => {
                // Should be an empty list
                assert!(!scalar.is_null());
            }
            _ => panic!("Expected EarlyReturn with default empty list"),
        }
    }

    #[test]
    fn test_extract_list_values_normal() {
        // Normal case should return Values
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::Values(values) => {
                assert_eq!(values.len(), 5);
                let ints = values.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(ints.values(), &[1, 2, 3, 4, 5]);
            }
            _ => panic!("Expected Values"),
        }
    }

    #[test]
    fn test_coerce_single_list_arg_errors() {
        // Wrong number of arguments
        let result = coerce_single_list_arg("filter", &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 1 value argument")
        );

        let result = coerce_single_list_arg("filter", &[DataType::Int32, DataType::Int32]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 1 value argument")
        );

        // Non-list type
        let result = coerce_single_list_arg("filter", &[DataType::Int32]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected a list"));
    }

    #[test]
    fn test_coerce_single_list_arg_fixed_size() {
        // FixedSizeList should be coerced to List
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::FixedSizeList(field.clone(), 3)]).unwrap();
        assert_eq!(result, vec![DataType::List(field)]);
    }

    #[test]
    fn test_coerce_single_list_arg_list_view() {
        // ListView should be coerced to List
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::ListView(field.clone())]).unwrap();
        assert_eq!(result, vec![DataType::List(field)]);
    }

    #[test]
    fn test_coerce_single_list_arg_large_list_view() {
        // LargeListView should be coerced to LargeList
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::LargeListView(field.clone())]).unwrap();
        assert_eq!(result, vec![DataType::LargeList(field)]);
    }

    #[test]
    fn test_index_array() {
        // Test that index_array generates correct indices
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let result = index_array("transform", &list).unwrap();
        let indices = result.as_any().downcast_ref::<Int32Array>().unwrap();
        // First row has 3 elements: indices 0, 1, 2
        // Second row has 2 elements: indices 0, 1
        assert_eq!(indices.values(), &[0, 1, 2, 0, 1]);
    }

    #[test]
    fn test_index_array_empty() {
        // Empty list should have no indices
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        ));
        let result = index_array("transform", &list).unwrap();
        let indices = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(indices.is_empty());
    }

    #[test]
    fn test_value_lambda_pair_error_cases() {
        use datafusion::arrow::datatypes::DataType;

        // Create test fields for ValueOrLambda
        let field1 = Arc::new(Field::new("f1", DataType::Int32, true));
        let field2 = Arc::new(Field::new("f2", DataType::Int32, true));

        // Wrong number of arguments - empty
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>("filter", &[]);
        assert!(result.is_err());

        // Wrong number of arguments - only one arg
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[ValueOrLambda::Value(Arc::clone(&field1))],
        );
        assert!(result.is_err());

        // Value in lambda position (two values instead of value + lambda)
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[
                ValueOrLambda::Value(Arc::clone(&field1)),
                ValueOrLambda::Value(Arc::clone(&field2)),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects a value followed by a lambda")
        );

        // Lambda in value position
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[
                ValueOrLambda::Lambda(Some(Arc::clone(&field1))),
                ValueOrLambda::Value(Arc::clone(&field2)),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects a value followed by a lambda")
        );
    }
}

#[cfg(any())]
mod tests {
    use datafusion::arrow::array::{
        BooleanArray, FixedSizeListArray, Int32Array, ListArray, NullArray,
    };
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::Field;
    use datafusion_expr::Expr;
    use datafusion_expr::expr::{LambdaVariable, Literal};

    use super::*;

    /// Creates a test list array with the given values and offsets
    fn create_test_list(values: Vec<i32>, offsets: Vec<i32>) -> ArrayRef {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        Arc::new(ListArray::new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(Int32Array::from(values)),
            None,
        ))
    }

    /// Creates a FixedSizeList array with the given values
    fn create_fixed_size_list(values: Vec<i32>, size: i32) -> ArrayRef {
        let values = Arc::new(Int32Array::from(values)) as ArrayRef;
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            size,
            values,
            None,
        ))
    }

    #[test]
    fn test_normalize_list_array_pass_through() {
        // Regular ListArray should pass through unchanged
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let normalized = normalize_list_array(list.clone()).unwrap();
        assert!(Arc::ptr_eq(&normalized, &list));
    }

    #[test]
    fn test_normalize_fixed_size_list() {
        // FixedSizeList should be converted to List
        let fixed = create_fixed_size_list(vec![1, 2, 3, 4, 5, 6], 3);
        let normalized = normalize_list_array(fixed).unwrap();
        assert_eq!(
            normalized.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
        );
        // Verify it can be used as a ListArray
        let list = normalized.as_list::<i32>();
        assert_eq!(list.len(), 2); // Two rows with 3 elements each
        assert_eq!(list.value_lengths().values(), &[3, 3]);
    }

    #[test]
    fn test_normalize_list_error_for_non_list() {
        // Non-list types should error
        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = normalize_list_array(int_array);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expected a list array")
        );
    }

    #[test]
    fn test_extract_list_values_all_null() {
        // All-null array should return EarlyReturn with null scalar
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Some(NullBuffer::from(vec![false, false])),
        ));
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::EarlyReturn(ColumnarValue::Scalar(scalar)) => {
                assert!(scalar.is_null());
            }
            _ => panic!("Expected EarlyReturn with null scalar"),
        }
    }

    #[test]
    fn test_extract_list_values_empty_sublists() {
        // All sublists empty should return EarlyReturn with default empty list scalar
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        ));
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::EarlyReturn(ColumnarValue::Scalar(scalar)) => {
                // Should be an empty list
                assert!(!scalar.is_null());
            }
            _ => panic!("Expected EarlyReturn with default empty list"),
        }
    }

    #[test]
    fn test_extract_list_values_normal() {
        // Normal case should return Values
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let return_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        match extract_list_values(&list, &return_type).unwrap() {
            ListValuesResult::Values(values) => {
                assert_eq!(values.len(), 5);
                let ints = values.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(ints.values(), &[1, 2, 3, 4, 5]);
            }
            _ => panic!("Expected Values"),
        }
    }

    #[test]
    fn test_coerce_single_list_arg_errors() {
        // Wrong number of arguments
        let result = coerce_single_list_arg("filter", &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 1 value argument")
        );

        let result = coerce_single_list_arg("filter", &[DataType::Int32, DataType::Int32]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 1 value argument")
        );

        // Non-list type
        let result = coerce_single_list_arg("filter", &[DataType::Int32]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected a list"));
    }

    #[test]
    fn test_coerce_single_list_arg_fixed_size() {
        // FixedSizeList should be coerced to List
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::FixedSizeList(field.clone(), 3)]).unwrap();
        assert_eq!(result, vec![DataType::List(field)]);
    }

    #[test]
    fn test_coerce_single_list_arg_list_view() {
        // ListView should be coerced to List
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::ListView(field.clone())]).unwrap();
        assert_eq!(result, vec![DataType::List(field)]);
    }

    #[test]
    fn test_coerce_single_list_arg_large_list_view() {
        // LargeListView should be coerced to LargeList
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let result =
            coerce_single_list_arg("filter", &[DataType::LargeListView(field.clone())]).unwrap();
        assert_eq!(result, vec![DataType::LargeList(field)]);
    }

    #[test]
    fn test_index_array() {
        // Test that index_array generates correct indices
        let list = create_test_list(vec![1, 2, 3, 4, 5], vec![0, 3, 5]);
        let result = index_array("transform", &list).unwrap();
        let indices = result.as_any().downcast_ref::<Int32Array>().unwrap();
        // First row has 3 elements: indices 0, 1, 2
        // Second row has 2 elements: indices 0, 1
        assert_eq!(indices.values(), &[0, 1, 2, 0, 1]);
    }

    #[test]
    fn test_index_array_empty() {
        // Empty list should have no indices
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            None,
        ));
        let result = index_array("transform", &list).unwrap();
        let indices = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(indices.is_empty());
    }

    #[test]
    fn test_value_lambda_pair_error_cases() {
        use datafusion::arrow::datatypes::DataType;

        // Create test fields for ValueOrLambda
        let field1 = Arc::new(Field::new("f1", DataType::Int32, true));
        let field2 = Arc::new(Field::new("f2", DataType::Int32, true));

        // Wrong number of arguments - empty
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>("filter", &[]);
        assert!(result.is_err());

        // Wrong number of arguments - only one arg
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[ValueOrLambda::Value(Arc::clone(&field1))],
        );
        assert!(result.is_err());

        // Value in lambda position (two values instead of value + lambda)
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[
                ValueOrLambda::Value(Arc::clone(&field1)),
                ValueOrLambda::Value(Arc::clone(&field2)),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects a value followed by a lambda")
        );

        // Lambda in value position
        let result = value_lambda_pair::<FieldRef, Option<FieldRef>>(
            "filter",
            &[
                ValueOrLambda::Lambda(Some(Arc::clone(&field1))),
                ValueOrLambda::Value(Arc::clone(&field2)),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects a value followed by a lambda")
        );
    }
}
