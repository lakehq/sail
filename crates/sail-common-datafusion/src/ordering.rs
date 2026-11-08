//! Spark's ordering of nested values.

use std::cmp::Ordering;

use arrow::array::{Array, make_comparator};
use arrow::compute::SortOptions;
use datafusion_common::{Result, ScalarValue};

/// Returns the Arrow sort options that order nested values (e.g. structs and arrays) like
/// Spark in the given direction.
///
/// In Spark, a NULL field or element inside a struct or an array is the smallest value, whatever
/// the direction and the NULL ordering of the sort (see `PhysicalDataType` in Spark). Arrow
/// (the comparator, the lexicographical sort, and the row format) applies the outer `nulls_first`
/// to nested fields and elements too, and reverses them with the outer direction, so
/// `nulls_first = !descending` gives Spark's nested order. The position of top-level NULL values
/// is not Spark's for all callers, so callers handle them separately when it matters.
pub const fn spark_nested_sort_options(descending: bool) -> SortOptions {
    SortOptions {
        descending,
        nulls_first: !descending,
    }
}

/// Returns the single-row array that a nested scalar holds, if any.
fn nested_scalar_array(value: &ScalarValue) -> Option<&dyn Array> {
    match value {
        ScalarValue::Struct(array) => Some(array.as_ref()),
        ScalarValue::List(array) => Some(array.as_ref()),
        ScalarValue::LargeList(array) => Some(array.as_ref()),
        ScalarValue::FixedSizeList(array) => Some(array.as_ref()),
        ScalarValue::ListView(array) => Some(array.as_ref()),
        ScalarValue::LargeListView(array) => Some(array.as_ref()),
        ScalarValue::Map(array) => Some(array.as_ref()),
        _ => None,
    }
}

/// Compares two scalars of the same nested type with Arrow's comparator and the given options,
/// which is the order of the rows of a nested array sorted with these options.
pub fn compare_nested_scalars_with_options(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    options: SortOptions,
) -> Result<Ordering> {
    match (nested_scalar_array(lhs), nested_scalar_array(rhs)) {
        (Some(lhs), Some(rhs)) => Ok(make_comparator(lhs, rhs, options)?(0, 0)),
        _ => Ok(make_comparator(
            lhs.to_array()?.as_ref(),
            rhs.to_array()?.as_ref(),
            options,
        )?(0, 0)),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, ListArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Int32Type};

    use super::*;

    fn list(value: Vec<Option<i32>>) -> ScalarValue {
        ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![Some(value)],
        )))
    }

    fn struct_scalar(a: Option<i32>) -> ScalarValue {
        ScalarValue::Struct(Arc::new(
            StructArray::try_new(
                Fields::from(vec![Field::new("a", DataType::Int32, true)]),
                vec![Arc::new(Int32Array::from(vec![a]))],
                None,
            )
            .unwrap(),
        ))
    }

    #[test]
    fn test_nested_nulls_are_smallest_in_both_directions() {
        let cases = [
            (list(vec![None, Some(5)]), list(vec![Some(1), Some(1)])),
            (list(vec![Some(1)]), list(vec![Some(1), None])),
            (list(vec![None]), list(vec![Some(i32::MIN)])),
            (struct_scalar(None), struct_scalar(Some(i32::MIN))),
        ];
        for (smaller, larger) in cases {
            let asc = spark_nested_sort_options(false);
            let desc = spark_nested_sort_options(true);
            assert_eq!(
                compare_nested_scalars_with_options(&smaller, &larger, asc).unwrap(),
                Ordering::Less
            );
            assert_eq!(
                compare_nested_scalars_with_options(&smaller, &larger, desc).unwrap(),
                Ordering::Greater
            );
            assert_eq!(
                compare_nested_scalars_with_options(&larger, &larger, desc).unwrap(),
                Ordering::Equal
            );
        }
    }
}
