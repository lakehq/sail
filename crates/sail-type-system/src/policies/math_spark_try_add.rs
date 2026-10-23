//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::IntervalUnit::{MonthDayNano, YearMonth};
use arrow_schema::TimeUnit::Microsecond;
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
        [DataType::Int32, DataType::Int32] => DataType::Int32,
        [DataType::Int64, DataType::Int64]
        | [DataType::Int32, DataType::Int64]
        | [DataType::Int64, DataType::Int32] => DataType::Int64,
        [DataType::Date32, _] | [_, DataType::Date32] => DataType::Date32,
        [DataType::Interval(YearMonth), _] | [_, DataType::Interval(YearMonth)] => {
            DataType::Interval(YearMonth)
        }
        [DataType::Interval(MonthDayNano), DataType::Int32]
        | [DataType::Int32, DataType::Interval(MonthDayNano)]
        | [DataType::Interval(MonthDayNano), DataType::Int64]
        | [DataType::Int64, DataType::Interval(MonthDayNano)]
        | [
            DataType::Interval(MonthDayNano),
            DataType::Interval(MonthDayNano),
        ] => DataType::Interval(MonthDayNano),
        [
            DataType::Timestamp(Microsecond, _),
            DataType::Duration(Microsecond),
        ] => DataType::Timestamp(Microsecond, None),
        _ => {
            return Err(unsupported_data_types_exec_err(
                "try_add",
                "Int32, Int64, Interval(YearMonth), Interval(MonthDayNano)",
                &arg_types,
            ));
        }
    };
    Ok(Arc::new(Field::new("try_add", data_type, true)))
}

pub fn coerce_types(types: &[DataType]) -> Result<Vec<DataType>> {
    if types.len() != 2 {
        return Err(invalid_arg_count_exec_err(
            "spark_try_add",
            (2, 2),
            types.len(),
        ));
    }
    let left: &DataType = &types[0];
    let right: &DataType = &types[1];

    let valid_pair = matches!(
        (left, right),
        (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::Date32, DataType::Int32)
            | (DataType::Date32, DataType::Interval(YearMonth))
            | (DataType::Date32, DataType::Interval(MonthDayNano))
            | (DataType::Interval(YearMonth), DataType::Date32)
            | (DataType::Interval(YearMonth), DataType::Interval(YearMonth))
            | (
                DataType::Timestamp(Microsecond, _),
                DataType::Duration(Microsecond)
            )
            | (
                DataType::Interval(MonthDayNano),
                DataType::Interval(MonthDayNano)
            )
    );
    if *left == DataType::Null {
        return Ok(vec![right.clone(), right.clone()]);
    } else if *right == DataType::Null {
        return Ok(vec![left.clone(), left.clone()]);
    }

    if valid_pair {
        Ok(vec![left.clone(), right.clone()])
    } else {
        Err(unsupported_data_types_exec_err(
            "spark_try_add",
            "Int32, Int64, Date32 or Interval(YearMonth)",
            types,
        ))
    }
}
