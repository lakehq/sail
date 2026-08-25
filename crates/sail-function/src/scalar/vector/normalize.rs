use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

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
        let vector_ty = match arg_types {
            [vector] if is_float_vector(vector) => vector,
            [vector, degree] if is_float_vector(vector) && degree == &DataType::Float32 => vector,
            _ => {
                return plan_err!(
                    "vector_normalize expects ARRAY<FLOAT> and an optional FLOAT degree, got {arg_types:?}"
                );
            }
        };
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        Ok(match vector_ty {
            DataType::LargeList(_) => DataType::LargeList(field),
            _ => DataType::List(field),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(vector_normalize_inner)(&args.args)
    }
}

fn is_float_vector(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type() == &DataType::Float32
    )
}

fn vector_normalize_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !(1..=2).contains(&args.len()) {
        return exec_err!("vector_normalize needs one vector and an optional degree argument");
    }
    let degree = args.get(1).map(|array| array.as_primitive::<Float32Type>());
    match args[0].data_type() {
        DataType::List(_) => {
            let rows = normalize_rows(as_list_array(&args[0]), degree)?;
            Ok(Arc::new(
                datafusion::arrow::array::ListArray::from_iter_primitive::<Float32Type, _, _>(rows),
            ))
        }
        DataType::LargeList(_) => {
            let rows = normalize_rows(as_large_list_array(&args[0]), degree)?;
            Ok(Arc::new(
                datafusion::arrow::array::LargeListArray::from_iter_primitive::<Float32Type, _, _>(
                    rows,
                ),
            ))
        }
        data_type => {
            exec_err!("vector_normalize expects an ARRAY<FLOAT> argument, got {data_type:?}")
        }
    }
}

fn normalize_rows<O: OffsetSizeTrait>(
    vectors: &GenericListArray<O>,
    degrees: Option<&Float32Array>,
) -> Result<Vec<Option<Vec<Option<f32>>>>> {
    (0..vectors.len())
        .map(|row| {
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
            let values = vector.values();
            let norm = norm_kind.compute(values);
            // Spark divides element-wise; a zero norm yields IEEE NaNs (0.0/0.0).
            let normalized: Vec<Option<f32>> =
                values.iter().map(|&value| Some(value / norm)).collect();
            Ok(Some(normalized))
        })
        .collect()
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
                "INVALID_VECTOR_NORM_DEGREE: vector_normalize degree must be 1.0, 2.0, or positive infinity, got {degree}"
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

    fn list_values(array: &ArrayRef) -> Vec<Option<Vec<Option<f32>>>> {
        let list = array.as_list::<i32>();
        (0..list.len())
            .map(|i| {
                if list.is_null(i) {
                    None
                } else {
                    let values = list.value(i);
                    let prim = values.as_primitive::<Float32Type>();
                    Some(
                        (0..prim.len())
                            .map(|j| {
                                if prim.is_null(j) {
                                    None
                                } else {
                                    Some(prim.value(j))
                                }
                            })
                            .collect(),
                    )
                }
            })
            .collect()
    }

    #[test]
    fn normalizes_with_supported_degrees() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
        ]);
        let degrees = Float32Array::from(vec![Some(2.0), Some(1.0), Some(f32::INFINITY)]);

        let rows_data = normalize_rows(&vectors, Some(&degrees))?;
        let actual: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(rows_data));
        let rows = list_values(&actual);
        assert_eq!(rows[0], Some(vec![Some(0.6), Some(0.8)]));
        assert_eq!(rows[1], Some(vec![Some(3.0 / 7.0), Some(4.0 / 7.0)]));
        assert_eq!(rows[2], Some(vec![Some(0.75), Some(1.0)]));
        Ok(())
    }

    #[test]
    fn defaults_to_l2_and_preserves_nulls() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(3.0), Some(4.0)]),
            Some(vec![Some(1.0), None]),
            None,
            Some(vec![]),
        ]);

        let rows_data = normalize_rows(&vectors, None)?;
        let actual: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(rows_data));
        let rows = list_values(&actual);
        assert_eq!(rows[0], Some(vec![Some(0.6), Some(0.8)]));
        assert_eq!(rows[1], None);
        assert_eq!(rows[2], None);
        assert_eq!(rows[3], Some(vec![]));
        Ok(())
    }

    #[test]
    fn zero_vector_yields_nans() -> Result<()> {
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(0.0),
            Some(0.0),
        ])]);
        let rows_data = normalize_rows(&vectors, None)?;
        let actual: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(rows_data));
        let rows = list_values(&actual);
        let row = rows[0].as_ref().unwrap();
        assert!(row[0].unwrap().is_nan());
        assert!(row[1].unwrap().is_nan());
        Ok(())
    }

    #[test]
    fn supports_large_list_arrays() -> Result<()> {
        let vectors =
            datafusion::arrow::array::LargeListArray::from_iter_primitive::<Float32Type, _, _>(
                vec![Some(vec![Some(3.0), Some(4.0)])],
            );
        let rows_data = normalize_rows(&vectors, None)?;
        let actual: ArrayRef = Arc::new(
            datafusion::arrow::array::LargeListArray::from_iter_primitive::<Float32Type, _, _>(
                rows_data,
            ),
        );
        let list = actual.as_list::<i64>();
        let prim = list.value(0).as_primitive::<Float32Type>();
        assert_eq!(prim.value(0), 0.6);
        assert_eq!(prim.value(1), 0.8);
        Ok(())
    }

    #[test]
    fn rejects_invalid_degrees() {
        let invalid = NormKind::try_from(3.0);
        assert!(
            matches!(invalid, Err(error) if error.to_string().contains("INVALID_VECTOR_NORM_DEGREE"))
        );
    }
}
