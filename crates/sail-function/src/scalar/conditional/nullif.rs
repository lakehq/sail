// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/core/nullif.rs

use arrow::compute::kernels::nullif::nullif;
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_physical_expr_common::datum::compare_with_eq;
use sail_common_datafusion::float_semantics::{has_float_leaf, normalize_floats};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NullIfFunc {
    signature: Signature,
}

impl Default for NullIfFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NullIfFunc {
    pub fn new() -> Self {
        Self {
            // Documentation mentioned in Postgres,
            // The result has the same type as the first argument — but there is a subtlety.
            // What is actually returned is the first argument of the implied = operator,
            // and in some cases that will have been promoted to match the second argument's type.
            // For example, NULLIF(1, 2.2) yields numeric, because there is no integer = numeric operator, only numeric = numeric
            //
            // We don't strictly follow Postgres or DuckDB for **simplicity**.
            // In this function, we will coerce arguments to the same data type for comparison need. Unlike DuckDB
            // we don't return the **original** first argument type but return the final coerced type.
            //
            // In Postgres, nullif('2', 2) returns Null but nullif('2::varchar', 2) returns error.
            // While in DuckDB both query returns Null. We follow DuckDB in this case since I think they are equivalent thing and should
            // have the same result as well.
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NullIfFunc {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "nullif"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        nullif_func(&args.args)
    }
}

/// Implements NULLIF(expr1, expr2)
/// Args: 0 - left expr is any array
///       1 - if the left is equal to this expr2, then the result is NULL, otherwise left value is passed.
fn nullif_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [lhs, rhs] = take_function_args("nullif", args)?;
    let is_nested = lhs.data_type().is_nested();

    match (lhs, rhs) {
        (ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)) => {
            let rhs = normalize_scalar(rhs)?.to_scalar()?;
            let eq_array = compare_with_eq(&normalize_floats(lhs), &rhs, is_nested)?;
            let array = nullif(lhs, &eq_array)?;

            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
            let eq_array =
                compare_with_eq(&normalize_floats(lhs), &normalize_floats(rhs), is_nested)?;
            let array = nullif(lhs, &eq_array)?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)) => {
            let lhs_s = normalize_scalar(lhs)?.to_scalar()?;
            let lhs_a = lhs.to_array_of_size(rhs.len())?;
            let eq_array = compare_with_eq(&lhs_s, &normalize_floats(rhs), is_nested)?;
            let array = nullif(
                // nullif in arrow-select does not support Datum, so we need to convert to array
                lhs_a.as_ref(),
                &eq_array,
            )?;
            Ok(ColumnarValue::Array(array))
        }
        (ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)) => {
            let val: ScalarValue = match normalize_scalar(lhs)? == normalize_scalar(rhs)? {
                true => lhs.data_type().try_into()?,
                false => lhs.clone(),
            };

            Ok(ColumnarValue::Scalar(val))
        }
    }
}

/// Normalizes `-0.0` to `0.0` in the floats of a value to compare, as in [`normalize_floats`],
/// since Spark compares floats with `-0.0` equal to `0.0`.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/SQLOrderingUtil.scala#L22-L40>
fn normalize_scalar(value: &ScalarValue) -> Result<ScalarValue> {
    if has_float_leaf(&value.data_type()) {
        ScalarValue::try_from_array(&normalize_floats(&value.to_array()?), 0)
    } else {
        Ok(value.clone())
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Fields, Float64Type, Int32Type, Int64Type};
    use datafusion_common::DataFusionError;

    use super::*;

    #[test]
    fn nullif_int32() -> Result<()> {
        let a = Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            None,
            None,
            Some(3),
            None,
            None,
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    // Ensure that arrays with no nulls can also invoke NULLIF() correctly
    fn nullif_int32_non_nulls() -> Result<()> {
        let a = Int32Array::from(vec![1, 3, 10, 7, 8, 1, 2, 4, 5]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(1i32)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            None,
            Some(3),
            Some(10),
            Some(7),
            Some(8),
            None,
            Some(2),
            Some(4),
            Some(5),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_boolean() -> Result<()> {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(BooleanArray::from(vec![Some(true), None, None])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_string() -> Result<()> {
        let a = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::from("bar"));

        let result = nullif_func(&[a, lit_array])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(StringArray::from(vec![
            Some("foo"),
            None,
            None,
            Some("baz"),
        ])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_struct() -> Result<()> {
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        let lhs_a = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let lhs_b = Arc::new(StringArray::from(vec![Some("1"), Some("2"), None]));
        let lhs_nulls = Some(NullBuffer::from(vec![true, true, false]));
        let lhs = ColumnarValue::Array(Arc::new(StructArray::new(
            fields.clone(),
            vec![lhs_a, lhs_b],
            lhs_nulls,
        )));

        let rhs_a = Arc::new(Int64Array::from(vec![Some(1), Some(9), None]));
        let rhs_b = Arc::new(StringArray::from(vec![Some("1"), Some("2"), None]));
        let rhs_nulls = Some(NullBuffer::from(vec![true, true, false]));
        let rhs = ColumnarValue::Array(Arc::new(StructArray::new(
            fields.clone(),
            vec![rhs_a, rhs_b],
            rhs_nulls,
        )));

        let result = nullif_func(&[lhs, rhs])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected_arrays = vec![
            Arc::new(Int64Array::from(vec![None, Some(2), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![None, Some("2"), None])) as ArrayRef,
        ];
        let expected_nulls = NullBuffer::from(vec![false, true, false]);

        let expected = Arc::new(StructArray::try_new(
            fields,
            expected_arrays,
            Some(expected_nulls),
        )?) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());

        Ok(())
    }

    #[test]
    fn nullif_list() -> Result<()> {
        let lhs = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            Some(vec![]),
            Some(vec![Some(5), Some(6), Some(7)]),
            None,
        ]));
        let lhs = ColumnarValue::Array(lhs);

        let rhs = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
        ]));
        let rhs = ColumnarValue::Scalar(ScalarValue::List(rhs));

        let result = nullif_func(&[lhs, rhs])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            None,
            Some(vec![Some(3)]),
            Some(vec![]),
            Some(vec![Some(5), Some(6), Some(7)]),
            None,
        ])) as ArrayRef;

        assert_eq!(expected.as_ref(), result.as_ref());

        Ok(())
    }

    #[test]
    fn nullif_compare_nested_to_unnested() -> Result<()> {
        let lhs = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            Some(vec![]),
            Some(vec![Some(5), Some(6), Some(7)]),
            None,
        ]));
        let lhs = ColumnarValue::Array(lhs);

        let rhs = Arc::new(Int64Array::from(vec![Some(1), Some(3), None, None, None]));
        let rhs = ColumnarValue::Array(rhs);

        let result = nullif_func(&[lhs, rhs]);

        assert!(matches!(result, Err(DataFusionError::ArrowError(_, _))));

        Ok(())
    }

    #[test]
    fn nullif_literal_first() -> Result<()> {
        let a = Int32Array::from(vec![Some(1), Some(2), None, None, Some(3), Some(4)]);
        let a = ColumnarValue::Array(Arc::new(a));

        let lit_array = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result = nullif_func(&[lit_array, a])?;
        let result = result.into_array(0).expect("Failed to convert to array");

        let expected = Arc::new(Int32Array::from(vec![
            Some(2),
            None,
            Some(2),
            Some(2),
            Some(2),
            Some(2),
        ])) as ArrayRef;
        assert_eq!(expected.as_ref(), result.as_ref());
        Ok(())
    }

    #[test]
    fn nullif_scalar() -> Result<()> {
        let a_eq = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let b_eq = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));

        let result_eq = nullif_func(&[a_eq, b_eq])?;
        let result_eq = result_eq.into_array(1).expect("Failed to convert to array");

        let expected_eq = Arc::new(Int32Array::from(vec![None])) as ArrayRef;

        assert_eq!(expected_eq.as_ref(), result_eq.as_ref());

        let a_neq = ColumnarValue::Scalar(ScalarValue::Int32(Some(2i32)));
        let b_neq = ColumnarValue::Scalar(ScalarValue::Int32(Some(1i32)));

        let result_neq = nullif_func(&[a_neq, b_neq])?;
        let result_neq = result_neq
            .into_array(1)
            .expect("Failed to convert to array");

        let expected_neq = Arc::new(Int32Array::from(vec![Some(2i32)])) as ArrayRef;
        assert_eq!(expected_neq.as_ref(), result_neq.as_ref());

        Ok(())
    }

    #[test]
    fn nullif_float_negative_zero() -> Result<()> {
        // The first values of `lhs` and `rhs` are equal in every case.
        let check = |lhs: ArrayRef, rhs: ArrayRef, valid: Vec<bool>| -> Result<()> {
            let result = nullif_func(&[
                ColumnarValue::Array(Arc::clone(&lhs)),
                ColumnarValue::Array(Arc::clone(&rhs)),
            ])?
            .into_array(lhs.len())?;
            assert_eq!(
                result.logical_nulls(),
                Some(NullBuffer::from(valid.clone()))
            );
            // The first argument is returned as it is, e.g. `-0.0` stays `-0.0`.
            let mask = valid.iter().map(|valid| !valid).collect::<Vec<_>>();
            let expected = nullif(&lhs, &BooleanArray::from(mask))?;
            assert_eq!(result.to_data(), expected.to_data());

            let lhs_scalar = ScalarValue::try_from_array(&lhs, 0)?;
            let rhs_scalar = ScalarValue::try_from_array(&rhs, 0)?;
            for args in [
                [
                    ColumnarValue::Array(Arc::clone(&lhs)),
                    ColumnarValue::Scalar(rhs_scalar.clone()),
                ],
                [
                    ColumnarValue::Scalar(lhs_scalar.clone()),
                    ColumnarValue::Array(Arc::clone(&rhs)),
                ],
                [
                    ColumnarValue::Scalar(lhs_scalar),
                    ColumnarValue::Scalar(rhs_scalar),
                ],
            ] {
                let result = nullif_func(&args)?.into_array(lhs.len())?;
                assert_eq!(result.data_type(), lhs.data_type());
                assert!(result.is_null(0));
            }
            Ok(())
        };
        check(
            Arc::new(Float64Array::from(vec![
                -0.0,
                0.0,
                -0.0,
                f64::NAN,
                1.0,
                -0.0,
            ])),
            Arc::new(Float64Array::from(vec![
                0.0,
                -0.0,
                -0.0,
                f64::NAN,
                -0.0,
                2.0,
            ])),
            vec![false, false, false, false, true, true],
        )?;
        check(
            Arc::new(Float32Array::from(vec![-0.0, 0.0, -1.0])),
            Arc::new(Float32Array::from(vec![0.0, -0.0, 1.0])),
            vec![false, false, true],
        )?;
        check(
            Arc::new(Float16Array::from(vec![
                half::f16::NEG_ZERO,
                half::f16::ONE,
            ])),
            Arc::new(Float16Array::from(vec![
                half::f16::ZERO,
                half::f16::NEG_ZERO,
            ])),
            vec![false, true],
        )?;
        check(
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(-0.0), Some(1.0)]),
                Some(vec![Some(-0.0)]),
            ])),
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(0.0), Some(1.0)]),
                Some(vec![Some(1.0)]),
            ])),
            vec![false, true],
        )?;
        let r#struct = |values: Vec<f64>| -> ArrayRef {
            Arc::new(StructArray::new(
                Fields::from(vec![Field::new("a", DataType::Float64, true)]),
                vec![Arc::new(Float64Array::from(values))],
                None,
            ))
        };
        check(
            r#struct(vec![-0.0, -0.0]),
            r#struct(vec![0.0, 2.0]),
            vec![false, true],
        )?;
        let dictionary = |values: Vec<f64>| -> ArrayRef {
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![0, 1]),
                Arc::new(Float64Array::from(values)),
            ))
        };
        check(
            dictionary(vec![-0.0, -0.0]),
            dictionary(vec![0.0, 2.0]),
            vec![false, true],
        )?;
        Ok(())
    }
}
