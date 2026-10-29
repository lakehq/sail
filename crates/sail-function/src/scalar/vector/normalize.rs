use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, as_large_list_array, as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use super::is_float_vector;
use super::norm::vector_norm_spark;
use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorNormalize {
    signature: Signature,
}

impl Default for VectorNormalize {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorNormalize {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for VectorNormalize {
    fn name(&self) -> &str {
        "vector_normalize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Spark 4.2 only accepts `ARRAY<FLOAT>` with an optional `FLOAT` degree and does not
        // implicitly cast other numeric types, so wider element types are rejected here as well.
        let valid = match arg_types {
            [vector] => is_float_vector(vector),
            [vector, degree] => is_float_vector(vector) && degree == &DataType::Float32,
            _ => false,
        };
        if !valid {
            return plan_err!(
                "vector_normalize expects an ARRAY<FLOAT> argument and an optional FLOAT degree, got {arg_types:?}"
            );
        }
        let field = Arc::new(Field::new_list_field(DataType::Float32, true));
        match &arg_types[0] {
            DataType::LargeList(_) => Ok(DataType::LargeList(field)),
            _ => Ok(DataType::List(field)),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(vector_normalize_inner)(&args.args)
    }
}

fn vector_normalize_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !(1..=2).contains(&args.len()) {
        return exec_err!("vector_normalize needs one or two arguments");
    }
    match args[0].data_type() {
        DataType::List(_) => compute_normalize(as_list_array(&args[0]), args.get(1)),
        DataType::LargeList(_) => compute_normalize(as_large_list_array(&args[0]), args.get(1)),
        data_type => {
            exec_err!("vector_normalize expects an ARRAY<FLOAT> argument, got {data_type:?}")
        }
    }
}

/// Spark 4.2 returns empty vectors unchanged and NULL when the norm is below `Float.MIN_NORMAL`.
fn vector_normalize_spark(vector: Option<&[f32]>, degree: f32) -> Result<Option<Vec<f32>>> {
    let norm = vector_norm_spark(vector, degree, "vector_normalize")?;
    let (Some(vector), Some(norm)) = (vector, norm) else {
        return Ok(None);
    };
    if vector.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if norm < f32::MIN_POSITIVE {
        return Ok(None);
    }
    Ok(Some(vector.iter().map(|value| value / norm).collect()))
}

fn compute_normalize<O: OffsetSizeTrait>(
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
        let values = (vector.null_count() == 0).then(|| vector.values().as_ref());
        let degree = degrees.map_or(2.0, |array| array.value(row));
        let normalized = vector_normalize_spark(values, degree)?;
        Ok(normalized.map(|values| values.into_iter().map(Some).collect::<Vec<_>>()))
    });
    let values = values.collect::<Result<Vec<Option<Vec<Option<f32>>>>>>()?;
    Ok(Arc::new(GenericListArray::<O>::from_iter_primitive::<
        Float32Type,
        _,
        _,
    >(values)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float32Array, LargeListArray, ListArray};

    use super::*;

    fn list_of(values: Vec<Option<Vec<Option<f32>>>>) -> ListArray {
        ListArray::from_iter_primitive::<Float32Type, _, _>(values)
    }

    #[test]
    fn computes_supported_norms() -> Result<()> {
        let vectors = list_of(vec![
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(-3.0), Some(4.0)]),
        ]);
        let degrees: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(f32::INFINITY),
            Some(1.0),
        ]));

        let actual = compute_normalize(&vectors, Some(&degrees))?;
        let expected = list_of(vec![
            Some(vec![Some(0.42857143), Some(0.5714286)]),
            Some(vec![Some(0.6), Some(0.8)]),
            Some(vec![Some(0.75), Some(1.0)]),
            Some(vec![Some(-0.42857143), Some(0.5714286)]),
        ]);
        assert_eq!(actual.as_list::<i32>(), &expected);
        Ok(())
    }

    #[test]
    fn defaults_to_l2_norm() -> Result<()> {
        let vectors = list_of(vec![Some(vec![Some(3.0), Some(4.0)])]);

        let actual = compute_normalize(&vectors, None)?;
        let expected = list_of(vec![Some(vec![Some(0.6), Some(0.8)])]);
        assert_eq!(actual.as_list::<i32>(), &expected);
        Ok(())
    }

    #[test]
    fn handles_null_empty_and_zero_vectors() -> Result<()> {
        let vectors = list_of(vec![
            None,
            Some(vec![]),
            Some(vec![Some(0.0), Some(0.0)]),
            Some(vec![Some(1.0), None]),
            Some(vec![Some(3.0), Some(4.0)]),
        ]);
        let degrees: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(2.0),
            Some(2.0),
            Some(2.0),
            Some(2.0),
            None,
        ]));

        let actual = compute_normalize(&vectors, Some(&degrees))?;
        let expected = list_of(vec![None, Some(vec![]), None, None, None]);
        assert_eq!(actual.as_list::<i32>(), &expected);
        Ok(())
    }

    #[test]
    fn rejects_invalid_degree() {
        for vector in [Some([1.0].as_slice()), Some([].as_slice()), None] {
            for degree in [3.0, f32::NAN, f32::NEG_INFINITY] {
                assert!(matches!(
                    vector_normalize_spark(vector, degree),
                    Err(error) if error.to_string().contains("vector_normalize degree must be")
                ));
            }
        }
    }

    #[test]
    fn matches_spark_float_edge_cases() -> Result<()> {
        // The subnormal norm is treated as zero.
        assert_eq!(vector_normalize_spark(Some(&[1.0e-39, 0.0]), 2.0)?, None);
        // The L2 accumulation overflows to infinity, so every element becomes 0.0.
        assert_eq!(
            vector_normalize_spark(Some(&[3.0e19, 4.0e19]), 2.0)?,
            Some(vec![0.0, 0.0])
        );
        let actual = vector_normalize_spark(Some(&[f32::NAN, 1.0]), f32::INFINITY)?;
        assert!(actual.is_some_and(|values| values[0].is_nan() && values[1] == 1.0));
        assert_eq!(
            vector_normalize_spark(Some(&[f32::NAN]), f32::INFINITY)?,
            None
        );
        Ok(())
    }

    #[test]
    fn uses_the_unrolled_accumulation_path() -> Result<()> {
        let vector = [
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        ];

        let actual = vector_normalize_spark(Some(&vector), 2.0)?;
        assert_eq!(actual.map(|values| values[15]), Some(0.41367015));

        let actual = vector_normalize_spark(Some(&vector), 1.0)?;
        assert_eq!(actual.map(|values| values[15]), Some(0.11764706));
        Ok(())
    }

    #[test]
    fn supports_large_list_arrays() -> Result<()> {
        let vectors = LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(3.0),
            Some(4.0),
        ])]);

        let actual = compute_normalize(&vectors, None)?;
        let expected = LargeListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(0.6),
            Some(0.8),
        ])]);
        assert_eq!(actual.as_list::<i64>(), &expected);
        Ok(())
    }
}
