use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::compute::{binary, cast};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Time64NanosecondType, TimeUnit,
};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::invalid_arg_count_exec_err;

const NANOS_PER_MICRO: i128 = 1_000;
const NANOS_PER_DAY: i128 = 86_400_000_000_000;

/// `TimeAddInterval` (`timeExpressions.scala:584`): a TIME shifted by a day-time interval, which
/// Sail spells `Duration(Microsecond)`; `-` passes the negated interval.
///
/// A UDF rather than `time + CAST(interval AS INTERVAL MONTH DAY NANO)`: DataFusion's interval bound
/// propagation has no extreme value for a TIME and panics in `handle_overflow` whenever the planner
/// knows the bounds of the TIME operand -- a TIME projected out of a CTE is enough.
///
/// TODO: Spark raises `[DATETIME_OVERFLOW]` in both ANSI modes when the result leaves the day
/// (`DateTimeUtils.scala:1098-1104`). This wraps within the 24-hour clock, as the arithmetic it
/// replaces did; `arithmetic_time_subtraction.feature` pins the gap.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimeAddDtInterval {
    signature: Signature,
}

impl Default for SparkTimeAddDtInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimeAddDtInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTimeAddDtInterval {
    fn name(&self) -> &str {
        "spark_time_add_dt_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [time @ (DataType::Time32(_) | DataType::Time64(_)), _] => Ok(time.clone()),
            _ => plan_err!("Spark `TimeAddInterval` expects a TIME and a day-time interval"),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [time, interval] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "TimeAddInterval",
                (2, 2),
                arg_types.len(),
            ));
        };
        if !matches!(time, DataType::Time32(_) | DataType::Time64(_))
            || !matches!(interval, DataType::Duration(_) | DataType::Null)
        {
            return plan_err!("Spark `TimeAddInterval` expects a TIME and a day-time interval");
        }
        Ok(vec![
            time.clone(),
            DataType::Duration(TimeUnit::Microsecond),
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            number_rows,
            return_field,
            ..
        } = args;
        let [time, interval] = <[ColumnarValue; 2]>::try_from(args)
            .map_err(|args| invalid_arg_count_exec_err("TimeAddInterval", (2, 2), args.len()))?;
        let time = cast(
            &time.to_array(number_rows)?,
            &DataType::Time64(TimeUnit::Nanosecond),
        )?;
        let interval = interval.to_array(number_rows)?;
        let shifted: PrimitiveArray<Time64NanosecondType> = binary(
            time.as_primitive::<Time64NanosecondType>(),
            interval.as_primitive::<DurationMicrosecondType>(),
            |time, micros| {
                (i128::from(time) + i128::from(micros) * NANOS_PER_MICRO).rem_euclid(NANOS_PER_DAY)
                    as i64
            },
        )?;
        let shifted: ArrayRef = cast(&shifted, return_field.data_type())?;
        Ok(ColumnarValue::Array(shifted))
    }
}
