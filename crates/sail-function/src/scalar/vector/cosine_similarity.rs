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
pub struct VectorCosineSimilarity {
    signature: Signature,
}

impl Default for VectorCosineSimilarity {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorCosineSimilarity {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VectorCosineSimilarity {
    fn name(&self) -> &str {
        "vector_cosine_similarity"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 || !arg_types.iter().all(is_float_vector) {
            return plan_err!(
                "vector_cosine_similarity expects two ARRAY<FLOAT> arguments, got {arg_types:?}"
            );
        }
        Ok(DataType::Float32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(vector_cosine_similarity_inner)(&args.args)
    }
}

fn vector_cosine_similarity_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("vector_cosine_similarity needs exactly two arguments");
    }
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::List(_), DataType::List(_)) => {
            compute_cosine_similarity(as_list_array(&args[0]), as_list_array(&args[1]))
        }
        (DataType::LargeList(_), DataType::LargeList(_)) => {
            compute_cosine_similarity(as_large_list_array(&args[0]), as_large_list_array(&args[1]))
        }
        (left, right) => exec_err!(
            "vector_cosine_similarity expects matching array types, got {left:?} and {right:?}"
        ),
    }
}

fn cosine_similarity_spark(left: &[f32], right: &[f32]) -> Option<f32> {
    debug_assert_eq!(left.len(), right.len());
    if left.is_empty() {
        return None;
    }

    let mut dot_product = 0.0f32;
    let mut left_norm_squared = 0.0f32;
    let mut right_norm_squared = 0.0f32;
    let mut index = 0;
    let simd_limit = (left.len() / 8) * 8;

    while index < simd_limit {
        let a0 = left[index];
        let a1 = left[index + 1];
        let a2 = left[index + 2];
        let a3 = left[index + 3];
        let a4 = left[index + 4];
        let a5 = left[index + 5];
        let a6 = left[index + 6];
        let a7 = left[index + 7];
        let b0 = right[index];
        let b1 = right[index + 1];
        let b2 = right[index + 2];
        let b3 = right[index + 3];
        let b4 = right[index + 4];
        let b5 = right[index + 5];
        let b6 = right[index + 6];
        let b7 = right[index + 7];

        dot_product +=
            a0 * b0 + a1 * b1 + a2 * b2 + a3 * b3 + a4 * b4 + a5 * b5 + a6 * b6 + a7 * b7;
        left_norm_squared +=
            a0 * a0 + a1 * a1 + a2 * a2 + a3 * a3 + a4 * a4 + a5 * a5 + a6 * a6 + a7 * a7;
        right_norm_squared +=
            b0 * b0 + b1 * b1 + b2 * b2 + b3 * b3 + b4 * b4 + b5 * b5 + b6 * b6 + b7 * b7;
        index += 8;
    }

    while index < left.len() {
        let left = left[index];
        let right = right[index];
        dot_product += left * right;
        left_norm_squared += left * left;
        right_norm_squared += right * right;
        index += 1;
    }

    let norm_product = (left_norm_squared * right_norm_squared).sqrt();
    if norm_product < f32::MIN_POSITIVE {
        None
    } else {
        Some(dot_product / norm_product)
    }
}

fn compute_cosine_similarity<O: OffsetSizeTrait>(
    left: &GenericListArray<O>,
    right: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let values = (0..left.len()).map(|row| {
        if left.is_null(row) || right.is_null(row) {
            return Ok(None);
        }
        let left = left.value(row);
        let right = right.value(row);
        if left.len() != right.len() {
            return exec_err!(
                "vector_cosine_similarity requires vectors with matching dimensions, got {} and {}",
                left.len(),
                right.len()
            );
        }
        let left = left.as_primitive::<Float32Type>();
        let right = right.as_primitive::<Float32Type>();
        if left.null_count() > 0 || right.null_count() > 0 {
            return Ok(None);
        }
        Ok(cosine_similarity_spark(left.values(), right.values()))
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
            Some(vec![Some(1.0), Some(0.0)]),
            Some(vec![Some(1.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(0.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), None]),
            None,
        ]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(4.0), Some(5.0), Some(6.0)]),
            Some(vec![Some(0.0), Some(1.0)]),
            Some(vec![Some(-1.0), Some(0.0)]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(1.0)]),
        ]);

        let actual = compute_cosine_similarity(&left, &right)?;
        let actual = actual.as_primitive::<Float32Type>();
        let expected = Float32Array::from(vec![
            Some(0.9746319),
            Some(0.0),
            Some(-1.0),
            None,
            None,
            None,
            None,
        ]);
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
            compute_cosine_similarity(&left, &right),
            Err(error) if error.to_string().contains("matching dimensions")
        ));
    }

    #[test]
    fn matches_spark_float_overflow_and_underflow() {
        assert!(
            cosine_similarity_spark(&[3.0e19, 4.0e19], &[3.0e19, 4.0e19]).is_some_and(f32::is_nan)
        );
        assert_eq!(
            cosine_similarity_spark(&[1.0e-23, 0.0], &[1.0e-23, 0.0]),
            None
        );
    }

    #[test]
    fn uses_the_unrolled_accumulation_path() {
        let left = [
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        ];
        let right = [
            16.0, 15.0, 14.0, 13.0, 12.0, 11.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0,
        ];

        assert_eq!(cosine_similarity_spark(&left, &right), Some(0.54545456));
    }

    #[test]
    fn supports_large_list_arrays() -> Result<()> {
        let left = LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(1.0),
            Some(2.0),
        ])]);
        let right = LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(1.0),
            Some(2.0),
        ])]);

        let actual = vector_cosine_similarity_inner(&[Arc::new(left), Arc::new(right)])?;
        assert_eq!(
            actual.as_primitive::<Float32Type>(),
            &Float32Array::from(vec![Some(1.0)])
        );
        Ok(())
    }

    #[test]
    fn propagates_infinite_values_as_nan() {
        let actual = cosine_similarity_spark(&[f32::INFINITY, 1.0], &[1.0, 1.0]);
        assert!(actual.is_some_and(f32::is_nan));
    }
}
