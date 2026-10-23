//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::IntervalUnit::{MonthDayNano, YearMonth};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::unsupported_data_types_exec_err;

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
        [DataType::Int32, DataType::Int32] => DataType::Int32,
        [DataType::Int64, DataType::Int64]
        | [DataType::Int32, DataType::Int64]
        | [DataType::Int64, DataType::Int32] => DataType::Int64,
        [
            DataType::Interval(YearMonth),
            DataType::Int32 | DataType::Int64,
        ]
        | [
            DataType::Int32 | DataType::Int64,
            DataType::Interval(YearMonth),
        ] => DataType::Interval(YearMonth),
        [
            DataType::Interval(MonthDayNano),
            DataType::Int32 | DataType::Int64,
        ]
        | [
            DataType::Int32 | DataType::Int64,
            DataType::Interval(MonthDayNano),
        ] => DataType::Interval(MonthDayNano),
        _ => {
            return Err(unsupported_data_types_exec_err(
                "try_multiply",
                "Int32, Int64, Interval(YearMonth), Interval(MonthDayNano)",
                &arg_types,
            ));
        }
    };
    Ok(Arc::new(Field::new("try_multiply", data_type, true)))
}

pub fn coerce_types(types: &[DataType]) -> Result<Vec<DataType>> {
    let [left, right] = take_function_args("try_multiply", types)?;

    match (left, right) {
        (DataType::Null, _) => Ok(vec![right.clone(), right.clone()]),
        (_, DataType::Null) => Ok(vec![left.clone(), left.clone()]),
        (DataType::Int32, DataType::Int32)
        | (DataType::Int64, DataType::Int64)
        | (
            DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano),
            DataType::Int32 | DataType::Int64,
        )
        | (
            DataType::Int32 | DataType::Int64,
            DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano),
        ) => Ok(vec![left.clone(), right.clone()]),
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
            Ok(vec![DataType::Int64, DataType::Int64])
        }
        _ => Err(unsupported_data_types_exec_err(
            "try_multiply",
            "Int32, Int64, Interval(YearMonth), Interval(MonthDayNano)",
            types,
        )),
    }
}
