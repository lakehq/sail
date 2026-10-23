//! Shared function type policies.

use arrow_schema::{DataType, IntervalUnit};
use datafusion_common::DataFusionError;

pub fn try_avg_function_return_type(arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
    use DataType::*;

    let dt = &arg_types[0];
    let result_type = match *dt {
        Null => Float64,
        Decimal128(p, s) => {
            let p2 = std::cmp::min(p + 10, 38);
            Decimal128(p2, s)
        }
        Interval(IntervalUnit::YearMonth) => Interval(IntervalUnit::YearMonth),
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Float64,
        Float16 | Float32 | Float64 => Float64,
        ref other => {
            return Err(DataFusionError::Plan(format!(
                "try_avg: unsupported type: {other:?}"
            )));
        }
    };

    Ok(result_type)
}

pub fn try_avg_function_coerce_types(
    arg_types: &[DataType],
) -> datafusion_common::Result<Vec<DataType>> {
    use DataType::*;
    if arg_types.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "try_avg: exactly 1 argument expected, got {}",
            arg_types.len()
        )));
    }

    let dt = &arg_types[0];
    let coerced = match dt {
        Null => Float64,
        Decimal128(p, s) => Decimal128(*p, *s),
        Interval(IntervalUnit::YearMonth) => Interval(IntervalUnit::YearMonth),
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Int64,
        Float16 | Float32 | Float64 => Float64,
        other => {
            return Err(DataFusionError::Plan(format!(
                "try_avg: unsupported type: {other:?}"
            )));
        }
    };
    Ok(vec![coerced])
}
