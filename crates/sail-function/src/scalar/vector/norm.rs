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
                "vector_norm expects ARRAY<FLOAT> and an optional FLOAT degree, got {arg_types:?}"
            );
        }
        Ok(DataType::Float32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(vector_norm_inner)(&args.args)
    }
}

fn is_float_vector(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type() == &DataType::Float32
    )
}

fn vector_norm_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !(1..=2).contains(&args.len()) {
        return exec_err!("vector_norm needs one vector and an optional degree argument");
    }
    let degree = args.get(1).map(|array| array.as_primitive::<Float32Type>());
    match args[0].data_type() {
        DataType::List(_) => compute_norm(as_list_array(&args[0]), degree),
        DataType::LargeList(_) => compute_norm(as_large_list_array(&args[0]), degree),
        data_type => exec_err!("vector_norm expects an ARRAY<FLOAT> argument, got {data_type:?}"),
    }
}

fn compute_norm<O: OffsetSizeTrait>(
    vectors: &GenericListArray<O>,
    degrees: Option<&Float32Array>,
) -> Result<ArrayRef> {
    let values = (0..vectors.len()).map(|row| {
        if vectors.is_null(row) || degrees.is_some_and(|degrees| degrees.is_null(row)) {
            return Ok(None);
        }
        let degree = degrees.map_or(2.0, |degrees| degrees.value(row));
        let norm_kind = NormKind::try_from(degree)?;
        let values = vectors.value(row);
        let vector = values.as_primitive::<Float32Type>();
        if vector.null_count() > 0 {
            return Ok(None);
        }
        Ok(Some(norm_kind.compute(vector.values())))
    });
    let values = values.collect::<Result<Vec<Option<f32>>>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

#[derive(Clone, Copy)]
enum NormKind {
    L1,
    L2,
    Infinity,
}

impl TryFrom<f32> for NormKind {
    type Error = datafusion_common::DataFusionError;

    fn try_from(degree: f32) -> Result<Self> {
        if degree == 1.0 {
            Ok(Self::L1)
        } else if degree == 2.0 {
            Ok(Self::L2)
        } else if degree.is_infinite() && degree.is_sign_positive() {
            Ok(Self::Infinity)
        } else {
            exec_err!(
                "INVALID_VECTOR_NORM_DEGREE: vector_norm degree must be 1.0, 2.0, or positive infinity, got {degree}"
            )
        }
    }
}

impl NormKind {
    fn compute(self, values: &[f32]) -> f32 {
        match self {
            Self::L1 => values.iter().map(|value| value.abs()).sum(),
            Self::L2 => {
                let squared_sum = values.iter().map(|value| value * value).sum::<f32>();
                if squared_sum == 0.0 {
                    0.0
                } else {
                    squared_sum.sqrt()
                }
            }
            Self::Infinity => values.iter().map(|value| value.abs()).fold(0.0, f32::max),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float32Array, ListArray};

    use super::*;

    #[test]
    fn computes_supported_norms_and_preserves_nulls() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![]),
            Some(vec![Some(3.0), None]),
            None,
        ]);
        let degrees = Float32Array::from(vec![Some(1.0), Some(2.0), Some(2.0), Some(2.0)]);

        let actual = compute_norm(&vectors, Some(&degrees))?;
        let actual = actual.as_primitive::<Float32Type>();
        let expected = Float32Array::from(vec![Some(7.0), Some(0.0), None, None]);
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn defaults_to_l2_and_supports_infinity_norm() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(-3.0),
            Some(4.0),
        ])]);

        let l2 = compute_norm(&vectors, None)?;
        assert_eq!(l2.as_primitive::<Float32Type>().value(0), 5.0);

        let infinity = Float32Array::from(vec![Some(f32::INFINITY)]);
        let max = compute_norm(&vectors, Some(&infinity))?;
        assert_eq!(max.as_primitive::<Float32Type>().value(0), 4.0);
        Ok(())
    }

    #[test]
    fn supports_large_list_arrays() -> Result<()> {
        let vectors =
            datafusion::arrow::array::LargeListArray::from_iter_primitive::<Float32Type, _, _>(
                vec![Some(vec![Some(1.0), Some(2.0), Some(2.0)])],
            );

        let actual = compute_norm(&vectors, None)?;
        let actual = actual.as_primitive::<Float32Type>();
        assert_eq!(actual.value(0), 3.0);
        Ok(())
    }

    #[test]
    fn rejects_non_vector_arguments_and_invalid_degrees() {
        let error = vector_norm_inner(&[Arc::new(Float32Array::from(vec![1.0]))]);
        assert!(matches!(error, Err(error) if error.to_string().contains("ARRAY<FLOAT>")));

        let invalid = NormKind::try_from(3.0);
        assert!(
            matches!(invalid, Err(error) if error.to_string().contains("INVALID_VECTOR_NORM_DEGREE"))
        );
    }
}
