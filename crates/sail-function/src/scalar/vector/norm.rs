use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Float32Type};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::is_float_vector;
use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorNorm {
    signature: Signature,
}

impl Default for VectorNorm {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorNorm {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for VectorNorm {
    fn name(&self) -> &str {
        "vector_norm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let valid = match arg_types {
            [vector] => is_float_vector(vector),
            [vector, degree] => is_float_vector(vector) && degree == &DataType::Float32,
            _ => false,
        };
        if !valid {
            return plan_err!(
                "vector_norm expects an ARRAY<FLOAT> argument and an optional FLOAT degree, got {arg_types:?}"
            );
        }
        Ok(DataType::Float32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(vector_norm_inner)(&args.args)
    }
}

fn vector_norm_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !(1..=2).contains(&args.len()) {
        return exec_err!("vector_norm needs one or two arguments");
    }
    match args[0].data_type() {
        DataType::List(_) => compute_norm(as_list_array(&args[0]), args.get(1)),
        DataType::LargeList(_) => compute_norm(as_large_list_array(&args[0]), args.get(1)),
        data_type => exec_err!("vector_norm expects an ARRAY<FLOAT> argument, got {data_type:?}"),
    }
}

fn vector_l1_norm_spark(vector: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let mut index = 0;
    let simd_limit = (vector.len() / 8) * 8;

    while index < simd_limit {
        sum += vector[index].abs()
            + vector[index + 1].abs()
            + vector[index + 2].abs()
            + vector[index + 3].abs()
            + vector[index + 4].abs()
            + vector[index + 5].abs()
            + vector[index + 6].abs()
            + vector[index + 7].abs();
        index += 8;
    }

    while index < vector.len() {
        sum += vector[index].abs();
        index += 1;
    }

    sum
}

fn vector_l2_norm_spark(vector: &[f32]) -> f32 {
    let mut sum_squared = 0.0f32;
    let mut index = 0;
    let simd_limit = (vector.len() / 8) * 8;

    while index < simd_limit {
        sum_squared += vector[index] * vector[index]
            + vector[index + 1] * vector[index + 1]
            + vector[index + 2] * vector[index + 2]
            + vector[index + 3] * vector[index + 3]
            + vector[index + 4] * vector[index + 4]
            + vector[index + 5] * vector[index + 5]
            + vector[index + 6] * vector[index + 6]
            + vector[index + 7] * vector[index + 7];
        index += 8;
    }

    while index < vector.len() {
        sum_squared += vector[index] * vector[index];
        index += 1;
    }

    (sum_squared as f64).sqrt() as f32
}

fn vector_infinity_norm_spark(vector: &[f32]) -> f32 {
    let mut maximum = 0.0f32;
    for value in vector {
        let absolute = value.abs();
        if absolute > maximum {
            maximum = absolute;
        }
    }
    maximum
}

fn vector_norm_spark(vector: &[f32], degree: f32) -> Result<f32> {
    if degree == 1.0 {
        Ok(vector_l1_norm_spark(vector))
    } else if degree == 2.0 {
        Ok(vector_l2_norm_spark(vector))
    } else if degree == f32::INFINITY {
        Ok(vector_infinity_norm_spark(vector))
    } else {
        exec_err!("vector_norm degree must be 1.0, 2.0, or float('inf'), got {degree}")
    }
}

fn compute_norm<O: OffsetSizeTrait>(
    vectors: &GenericListArray<O>,
    degrees: Option<&ArrayRef>,
) -> Result<ArrayRef> {
    let degrees = degrees.map(|array| array.as_primitive::<Float32Type>());
    let values = (0..vectors.len()).map(|row| {
        if vectors.is_null(row) || degrees.is_some_and(|array| array.is_null(row)) {
            return Ok(None);
        }
        let vector = vectors.value(row);
        let vector = vector.as_primitive::<Float32Type>();
        if vector.null_count() > 0 {
            return Ok(None);
        }
        let degree = degrees.map_or(2.0, |array| array.value(row));
        Ok(Some(vector_norm_spark(vector.values(), degree)?))
    });
    let values = values.collect::<Result<Vec<Option<f32>>>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{LargeListArray, ListArray};

    use super::*;

    #[test]
    fn computes_supported_norms() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(-3.0), Some(4.0)]),
            Some(vec![Some(-3.0), Some(4.0)]),
            Some(vec![Some(-3.0), Some(4.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), None]),
            None,
        ]);
        let degrees = Float32Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(f32::INFINITY),
            Some(2.0),
            Some(2.0),
            Some(2.0),
        ]);

        let actual = compute_norm(&vectors, Some(&(Arc::new(degrees) as ArrayRef)))?;
        let expected =
            Float32Array::from(vec![Some(7.0), Some(5.0), Some(4.0), Some(0.0), None, None]);
        assert_eq!(actual.as_primitive::<Float32Type>(), &expected);
        Ok(())
    }

    #[test]
    fn defaults_to_l2_norm() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(3.0),
            Some(4.0),
        ])]);

        let actual = compute_norm(&vectors, None)?;
        assert_eq!(
            actual.as_primitive::<Float32Type>(),
            &Float32Array::from(vec![Some(5.0)])
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_degree() {
        assert!(matches!(
            vector_norm_spark(&[1.0], 3.0),
            Err(error) if error.to_string().contains("degree must be")
        ));
        assert!(vector_norm_spark(&[1.0], f32::NAN).is_err());
        assert!(vector_norm_spark(&[1.0], f32::NEG_INFINITY).is_err());
    }

    #[test]
    fn matches_spark_float_edge_cases() -> Result<()> {
        assert_eq!(vector_norm_spark(&[3.0e38, 3.0e38], 1.0)?, f32::INFINITY);
        assert_eq!(vector_norm_spark(&[3.0e19, 4.0e19], 2.0)?, f32::INFINITY);
        assert_eq!(vector_norm_spark(&[1.0e-23, 0.0], 2.0)?, 0.0);
        assert_eq!(vector_norm_spark(&[f32::NAN, 1.0], f32::INFINITY)?, 1.0);
        Ok(())
    }

    #[test]
    fn supports_large_list_arrays() -> Result<()> {
        let vectors = LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(3.0),
            Some(4.0),
        ])]);

        let actual = compute_norm(&vectors, None)?;
        assert_eq!(
            actual.as_primitive::<Float32Type>(),
            &Float32Array::from(vec![Some(5.0)])
        );
        Ok(())
    }
}
