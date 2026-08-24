use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Float32Type};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

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

fn is_float_vector(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type() == &DataType::Float32
    )
}

fn vector_l2_distance_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("vector_l2_distance needs exactly two arguments");
    }
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::List(_), DataType::List(_)) => {
            compute_l2_distance(as_list_array(&args[0]), as_list_array(&args[1]))
        }
        (DataType::LargeList(_), DataType::LargeList(_)) => {
            compute_l2_distance(as_large_list_array(&args[0]), as_large_list_array(&args[1]))
        }
        (left, right) => {
            exec_err!("vector_l2_distance expects matching array types, got {left:?} and {right:?}")
        }
    }
}

/// Spark 4.x `vectorL2Distance` accumulates the squared differences eight at a
/// time in double precision (`sumSq += d0*d0 + ... + d7*d7`) then a scalar tail,
/// and returns `(float) sqrt(sumSq)`. The accumulator is widened to `f64` so a
/// sum of squares that is quadratic in the inputs does not overflow to infinity
/// for distances that are representable as an `f32`.
fn squared_distance_spark(left: &[f32], right: &[f32]) -> f64 {
    debug_assert_eq!(left.len(), right.len());
    let mut sum = 0.0f64;
    let mut i = 0;
    let simd_limit = (left.len() / 8) * 8;
    while i < simd_limit {
        let d0 = left[i] as f64 - right[i] as f64;
        let d1 = left[i + 1] as f64 - right[i + 1] as f64;
        let d2 = left[i + 2] as f64 - right[i + 2] as f64;
        let d3 = left[i + 3] as f64 - right[i + 3] as f64;
        let d4 = left[i + 4] as f64 - right[i + 4] as f64;
        let d5 = left[i + 5] as f64 - right[i + 5] as f64;
        let d6 = left[i + 6] as f64 - right[i + 6] as f64;
        let d7 = left[i + 7] as f64 - right[i + 7] as f64;
        sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3 + d4 * d4 + d5 * d5 + d6 * d6 + d7 * d7;
        i += 8;
    }
    while i < left.len() {
        let diff = left[i] as f64 - right[i] as f64;
        sum += diff * diff;
        i += 1;
    }
    sum
}

fn compute_l2_distance<O: OffsetSizeTrait>(
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
        // Match Spark 4.x: accumulate the squared differences in groups of eight,
        // in double precision, so the Euclidean distance stays Spark-compatible.
        Ok(Some(
            squared_distance_spark(left.values(), right.values()).sqrt() as f32,
        ))
    });
    let values = values.collect::<Result<Vec<Option<f32>>>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::ListArray;

    use super::*;

    #[test]
    fn computes_spark_compatible_results() -> Result<()> {
        let left = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            // sqrt((1-4)^2 + (2-5)^2 + (3-6)^2) = sqrt(27) = 5.196152
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            // empty vectors -> distance 0.0
            Some(vec![]),
            // identical vectors -> distance 0.0
            Some(vec![Some(3.0), Some(4.0)]),
            // null element -> NULL
            Some(vec![Some(1.0), None, Some(3.0)]),
            // null vector -> NULL
            None,
        ]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(4.0), Some(5.0), Some(6.0)]),
            Some(vec![]),
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            Some(vec![Some(1.0)]),
        ]);

        let actual = compute_l2_distance(&left, &right)?;
        let actual = actual.as_primitive::<Float32Type>();
        let expected = Float32Array::from(vec![Some(5.196152), Some(0.0), Some(0.0), None, None]);
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn distance_of_3_4_right_triangle_is_5() -> Result<()> {
        // sqrt((3-0)^2 + (4-0)^2) = 5.0
        let left = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(3.0),
            Some(4.0),
        ])]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(0.0),
            Some(0.0),
        ])]);

        let actual = compute_l2_distance(&left, &right)?;
        let actual = actual.as_primitive::<Float32Type>();
        assert_eq!(actual.value(0), 5.0);
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
    fn double_accumulation_avoids_f32_overflow() -> Result<()> {
        // The distance 2e20 is representable as an f32 (< f32::MAX ~3.4e38), but
        // its square 4e40 overflows f32 to +inf. Accumulating the sum of squares
        // in f64 (as Spark does) keeps the distance recoverable.
        let left =
            ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![Some(2.0e20)])]);
        let right =
            ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![Some(0.0)])]);

        let actual = compute_l2_distance(&left, &right)?;
        let actual = actual.as_primitive::<Float32Type>();
        // sqrt((2e20)^2) = 2e20, exactly recovered; an f32 sum-of-squares would
        // overflow to +inf and produce inf here.
        assert_eq!(actual.value(0), 2.0e20);
        assert!(actual.value(0).is_finite());
        Ok(())
    }
}
