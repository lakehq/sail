//! Shared function typing, including literal-dependent result fields.

use std::sync::Arc;

use arrow_schema::{DECIMAL128_MAX_PRECISION, DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

pub fn get_scale_and_precision(
    precision_left: &u8,
    scale_left: &i8,
    precision_right: &u8,
    scale_right: &i8,
) -> (i8, u8) {
    let result_scale: i8 = (*scale_left).max(*scale_right);
    let int_digits_l: i8 = *precision_left as i8 - *scale_left;
    let int_digits_r: i8 = *precision_right as i8 - *scale_right;
    let result_precision: u8 = (result_scale.saturating_add(int_digits_l.min(int_digits_r)) as u8)
        .min(DECIMAL128_MAX_PRECISION);
    (result_scale, result_precision)
}

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let data_type = if let [DataType::Decimal128(pl, sl), DataType::Decimal128(pr, sr)] =
        arg_types.as_slice()
    {
        let (result_scale, result_precision) = get_scale_and_precision(pl, sl, pr, sr);
        DataType::Decimal128(result_precision, result_scale)
    } else if arg_types.contains(&DataType::Int64) {
        DataType::Int64
    } else {
        DataType::Int32
    };
    Ok(Arc::new(Field::new("spark_try_mod", data_type, true)))
}

pub fn coerce_types(types: &[DataType]) -> Result<Vec<DataType>> {
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
            "Int32, Int64 or Decimal128",
            types,
        )),
    }
}
