use std::sync::Arc;

use datafusion::arrow::array::builder::PrimitiveBuilder;
use datafusion::arrow::array::{Array, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, DecimalType, Field, FieldRef, Int32Type, Int64Type,
};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::utils::try_op::{binary_op_scalar_or_array, try_binary_op_primitive};

fn try_mod_return_type(arg_types: &[DataType]) -> Result<DataType> {
    if let [DataType::Decimal128(pl, sl), DataType::Decimal128(pr, sr)] = arg_types {
        let (result_scale, result_precision) = SparkTryMod::get_scale_and_precision(pl, sl, pr, sr);
        return Ok(DataType::Decimal128(result_precision, result_scale));
    }
    if arg_types.contains(&DataType::Int64) {
        Ok(DataType::Int64)
    } else {
        Ok(DataType::Int32)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryMod {
    signature: Signature,
}

impl Default for SparkTryMod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryMod {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn get_scale_and_precision(
        precision_left: &u8,
        scale_left: &i8,
        precision_right: &u8,
        scale_right: &i8,
    ) -> (i8, u8) {
        let result_scale: i8 = (*scale_left).max(*scale_right);
        let int_digits_l: i8 = *precision_left as i8 - *scale_left;
        let int_digits_r: i8 = *precision_right as i8 - *scale_right;
        let result_precision: u8 = (result_scale.saturating_add(int_digits_l.min(int_digits_r))
            as u8)
            .min(Decimal128Type::MAX_PRECISION);
        (result_scale, result_precision)
    }
}

impl ScalarUDFImpl for SparkTryMod {
    fn name(&self) -> &str {
        "try_mod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TryMod` (`TryEval.scala:154-163`) is `RuntimeReplaceable` and declares no
    /// `nullable` of its own, and neither does `InheritAnalysisRules`
    /// (`Expression.scala:470`), so the rule is `replacement.nullable`
    /// (`Expression.scala:446`). Both replacement branches land on `true`: the numeric branch
    /// is `Remainder(left, right, EvalMode.TRY)`, whose `nullable` short-circuits on
    /// `evalMode == EvalMode.TRY` (`arithmetic.scala:236`), and the fallback wraps the ANSI
    /// expression in `TryEval`, which declares `true` (`TryEval.scala:50`).
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = try_mod_return_type(&arg_types)?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_try_mod",
                (2, 2),
                args.len(),
            ));
        };

        let len = match (&left, &right) {
            (ColumnarValue::Array(arr), _) => arr.len(),
            (_, ColumnarValue::Array(arr)) => arr.len(),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => 1,
        };

        let left_arr = match left {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        let right_arr = match right {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        match (left_arr.data_type(), right_arr.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let l = left_arr.as_primitive::<Int32Type>();
                let r = right_arr.as_primitive::<Int32Type>();
                let result = try_binary_op_primitive::<Int32Type, _>(l, r, i32::checked_rem);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int64, DataType::Int64) => {
                let l = left_arr.as_primitive::<Int64Type>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_binary_op_primitive::<Int64Type, _>(l, r, i64::checked_rem);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                let (result_scale, result_precision) =
                    Self::get_scale_and_precision(p1, s1, p2, s2);

                let l_in = left_arr.as_primitive::<Decimal128Type>();
                let r_in = right_arr.as_primitive::<Decimal128Type>();

                let l_rescaled = rescale_decimal_up(l_in, *s1, result_scale, result_precision)?;
                let r_rescaled = rescale_decimal_up(r_in, *s2, result_scale, result_precision)?;

                let raw = try_binary_op_primitive::<Decimal128Type, _>(
                    &l_rescaled,
                    &r_rescaled,
                    i128::checked_rem,
                );

                let adjusted: PrimitiveArray<Decimal128Type> =
                    raw.with_precision_and_scale(result_precision, result_scale)?;
                binary_op_scalar_or_array(left, right, adjusted)
            }

            (l, r) => Err(unsupported_data_types_exec_err(
                "spark_try_mod",
                "Int32, Int64 o Decimal128",
                &[l.clone(), r.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "spark_try_mod",
                (2, 2),
                types.len(),
            ));
        }

        let left = &types[0];
        let right = &types[1];

        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }

        let is_int = |t: &DataType| matches!(t, DataType::Int32 | DataType::Int64);

        match (left, right) {
            (DataType::Decimal128(pl, sl), DataType::Decimal128(pr, sr)) => Ok(vec![
                DataType::Decimal128(*pl, *sl),
                DataType::Decimal128(*pr, *sr),
            ]),
            (DataType::Decimal128(p, s), r) if is_int(r) => Ok(vec![
                DataType::Decimal128(*p, *s),
                DataType::Decimal128(*p, *s),
            ]),
            (l, DataType::Decimal128(p, s)) if is_int(l) => Ok(vec![
                DataType::Decimal128(*p, *s),
                DataType::Decimal128(*p, *s),
            ]),
            (l, r) if is_int(l) && is_int(r) => {
                if *l == DataType::Int64 || *r == DataType::Int64 {
                    Ok(vec![DataType::Int64, DataType::Int64])
                } else {
                    Ok(vec![DataType::Int32, DataType::Int32])
                }
            }
            _ => Err(unsupported_data_types_exec_err(
                "spark_try_mod",
                "Int32, Int64 o Decimal128",
                types,
            )),
        }
    }
}

fn rescale_decimal_up(
    arr: &PrimitiveArray<Decimal128Type>,
    from_scale: i8,
    to_scale: i8,
    precision: u8,
) -> Result<PrimitiveArray<Decimal128Type>> {
    let delta: u32 = (to_scale - from_scale) as u32;

    let Some(mul): Option<i128> = 10i128.checked_pow(delta) else {
        let mut b = PrimitiveBuilder::<Decimal128Type>::with_capacity(arr.len());
        for _ in 0..arr.len() {
            b.append_null();
        }
        let out = b.finish().with_precision_and_scale(precision, to_scale)?;
        return Ok(out);
    };

    let mut b = PrimitiveBuilder::<Decimal128Type>::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            b.append_null();
        } else {
            let v = arr.value(i);
            match v.checked_mul(mul) {
                Some(scaled) => b.append_value(scaled),
                None => b.append_null(),
            }
        }
    }
    let out: PrimitiveArray<Decimal128Type> =
        b.finish().with_precision_and_scale(precision, to_scale)?;
    Ok(out)
}

#[cfg(test)]
mod return_field_tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ReturnFieldArgs, ScalarUDFImpl};

    use super::*;

    fn non_nullable(data_type: DataType) -> FieldRef {
        Arc::new(Field::new("c", data_type, false))
    }

    /// Spark declares this function's output nullable regardless of its children, so a
    /// non-nullable argument -- the case where the arity default would say `false` -- must
    /// still come back nullable.
    #[test]
    fn test_non_nullable_arguments_still_yield_a_nullable_field() -> Result<()> {
        let arg_fields = vec![non_nullable(DataType::Int32), non_nullable(DataType::Int32)];
        let scalar_arguments: Vec<Option<&ScalarValue>> = vec![None; arg_fields.len()];
        let field = SparkTryMod::new().return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());
        Ok(())
    }

    #[test]
    fn test_return_type_is_not_the_source_of_truth() {
        assert!(
            SparkTryMod::new()
                .return_type(&[DataType::Int32, DataType::Int32])
                .is_err()
        );
    }
}
