use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Float32Type};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use super::is_float_vector;
use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorL2Distance {
    signature: Signature,
}

impl Default for VectorL2Distance {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorL2Distance {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VectorL2Distance {
    fn name(&self) -> &str {
        "vector_l2_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 || !arg_types.iter().all(is_float_vector) {
            return plan_err!(
                "vector_l2_distance expects two ARRAY<FLOAT> arguments, got {arg_types:?}"
            );
        }
        Ok(DataType::Float32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(vector_l2_distance_inner)(&args.args)
    }
}

fn vector_l2_distance_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("vector_l2_distance needs exactly two arguments");
    }
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::List(_), DataType::List(_)) => {
            compute_l2_distance(as_list_array(&args[0]), as_list_array(&args[1]))
        }
        (DataType::List(_), DataType::LargeList(_)) => {
            compute_l2_distance(as_list_array(&args[0]), as_large_list_array(&args[1]))
        }
        (DataType::LargeList(_), DataType::List(_)) => {
            compute_l2_distance(as_large_list_array(&args[0]), as_list_array(&args[1]))
        }
        (DataType::LargeList(_), DataType::LargeList(_)) => {
            compute_l2_distance(as_large_list_array(&args[0]), as_large_list_array(&args[1]))
        }
        (left, right) => exec_err!(
            "vector_l2_distance expects ARRAY<FLOAT> arguments, got {left:?} and {right:?}"
        ),
    }
}

/// Spark 4.2 accumulates squared differences eight at a time before processing the scalar tail.
fn l2_distance_spark(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    let mut sum_squared = 0.0f32;
    let mut index = 0;
    let simd_limit = (left.len() / 8) * 8;

    while index < simd_limit {
        let d0 = left[index] - right[index];
        let d1 = left[index + 1] - right[index + 1];
        let d2 = left[index + 2] - right[index + 2];
        let d3 = left[index + 3] - right[index + 3];
        let d4 = left[index + 4] - right[index + 4];
        let d5 = left[index + 5] - right[index + 5];
        let d6 = left[index + 6] - right[index + 6];
        let d7 = left[index + 7] - right[index + 7];

        sum_squared +=
            d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3 + d4 * d4 + d5 * d5 + d6 * d6 + d7 * d7;
        index += 8;
    }

    while index < left.len() {
        let difference = left[index] - right[index];
        sum_squared += difference * difference;
        index += 1;
    }

    (sum_squared as f64).sqrt() as f32
}

fn compute_l2_distance<L: OffsetSizeTrait, R: OffsetSizeTrait>(
    left: &GenericListArray<L>,
    right: &GenericListArray<R>,
) -> Result<ArrayRef> {
    let values = (0..left.len()).map(|row| {
        if left.is_null(row) || right.is_null(row) {
            return Ok(None);
        }
        let left = left.value(row);
        let right = right.value(row);
        if left.len() != right.len() {
            return exec_err!(
                "vector_l2_distance requires vectors with matching dimensions, got {} and {}",
                left.len(),
                right.len()
            );
        }
        let left = left.as_primitive::<Float32Type>();
        let right = right.as_primitive::<Float32Type>();
        if left.null_count() > 0 || right.null_count() > 0 {
            return Ok(None);
        }
        Ok(Some(l2_distance_spark(left.values(), right.values())))
    });
    let values = values.collect::<Result<Vec<Option<f32>>>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{LargeListArray, ListArray};

    use super::*;

    #[test]
    fn computes_spark_compatible_results() -> Result<()> {
        let left = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), None]),
            None,
        ]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(4.0), Some(5.0), Some(6.0)]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(1.0)]),
        ]);

        let actual = compute_l2_distance(&left, &right)?;
        let actual = actual.as_primitive::<Float32Type>();
        let expected = Float32Array::from(vec![Some(5.196152), Some(0.0), Some(0.0), None, None]);
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn rejects_dimension_mismatch() {
        let left = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![Some(1.0)])]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(1.0),
            Some(2.0),
        ])]);

        assert!(matches!(
            compute_l2_distance(&left, &right),
            Err(error) if error.to_string().contains("matching dimensions")
        ));
    }

    #[test]
    fn matches_spark_float_overflow_and_underflow() {
        assert_eq!(
            l2_distance_spark(&[3.0e19, 4.0e19], &[0.0, 0.0]),
            f32::INFINITY
        );
        assert_eq!(l2_distance_spark(&[1.0e-23, 0.0], &[0.0, 0.0]), 0.0);
    }

    #[test]
    fn uses_the_unrolled_accumulation_path() {
        let left = [
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        ];
        let right = [0.0; 16];

        assert_eq!(l2_distance_spark(&left, &right), 38.678158);
    }

    #[test]
    fn supports_mixed_list_array_widths() -> Result<()> {
        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(1.0), Some(2.0)]),
        ]));
        let large_list: ArrayRef =
            Arc::new(LargeListArray::from_iter_primitive::<Float32Type, _, _>(
                vec![Some(vec![Some(4.0), Some(6.0)])],
            ));
        let expected = Float32Array::from(vec![Some(5.0)]);

        let list_large = vector_l2_distance_inner(&[Arc::clone(&list), Arc::clone(&large_list)])?;
        assert_eq!(list_large.as_primitive::<Float32Type>(), &expected);

        let large_list = vector_l2_distance_inner(&[Arc::clone(&large_list), Arc::clone(&list)])?;
        assert_eq!(large_list.as_primitive::<Float32Type>(), &expected);
        Ok(())
    }
}
