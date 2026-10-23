//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::IntervalUnit::{MonthDayNano, YearMonth};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let data_type = match arg_types.as_slice() {
        [DataType::Int32, DataType::Int32]
        | [DataType::Int64, DataType::Int64]
        | [DataType::Int32, DataType::Int64]
        | [DataType::Int64, DataType::Int32] => DataType::Float64,
        [DataType::Interval(YearMonth), DataType::Int32] => DataType::Interval(YearMonth),
        [DataType::Interval(MonthDayNano), DataType::Int32]
        | [DataType::Interval(MonthDayNano), DataType::Int64] => DataType::Interval(MonthDayNano),
        _ => {
            return Err(unsupported_data_types_exec_err(
                "try_divide",
                "Int32, Int64 or Interval(YearMonth|MonthDayNano)",
                &arg_types,
            ));
        }
    };
    Ok(Arc::new(Field::new("try_divide", data_type, true)))
}

pub fn coerce_types(types: &[DataType]) -> Result<Vec<DataType>> {
    if types.len() != 2 {
        return Err(invalid_arg_count_exec_err(
            "try_divide",
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

    if matches!(
        (left, right),
        (DataType::Interval(YearMonth), DataType::Int32)
            | (DataType::Int32, DataType::Interval(YearMonth))
    ) {
        return Ok(vec![DataType::Interval(YearMonth), DataType::Int32]);
    }

    if matches!(
        (left, right),
        (DataType::Interval(MonthDayNano), DataType::Int32)
            | (DataType::Int32, DataType::Interval(MonthDayNano))
    ) {
        return Ok(vec![DataType::Interval(MonthDayNano), DataType::Int32]);
    }
    if matches!(
        (left, right),
        (DataType::Interval(MonthDayNano), DataType::Int64)
            | (DataType::Int64, DataType::Interval(MonthDayNano))
    ) {
        return Ok(vec![DataType::Interval(MonthDayNano), DataType::Int64]);
    }
    if matches!(
        (left, right),
        (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::Int32, DataType::Int64)
            | (DataType::Int64, DataType::Int32)
    ) {
        if *left == DataType::Int64 || *right == DataType::Int64 {
            return Ok(vec![DataType::Int64, DataType::Int64]);
        } else {
            return Ok(vec![DataType::Int32, DataType::Int32]);
        }
    }

    Err(unsupported_data_types_exec_err(
        "try_divide",
        "Int32, Int64 or Interval(YearMonth) / Int32",
        types,
    ))
}
