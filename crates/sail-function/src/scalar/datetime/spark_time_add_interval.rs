use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::compute::{cast, try_binary};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Time64NanosecondType, TimeUnit,
};
use datafusion::arrow::error::ArrowError;
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
/// Spark rejects results outside the day in both ANSI modes
/// (`DateTimeUtils.scala:1098-1104`).
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

    /// `TimeAddInterval` answers `TimeType(max(p, 6))` when the interval reaches SECOND
    /// (`timeExpressions.scala:596-606`). Sail's `Duration` does not carry the interval's end field,
    /// so the result is TIME(6), or TIME(9) for a nanosecond input; the input type cut the fraction.
    /// TODO: `TIME(0) + INTERVAL '1' HOUR` is `time(0)` in Spark and `time(6)` here, until the
    ///  day-time interval keeps its end field.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Time64(TimeUnit::Nanosecond), _] => {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            }
            [DataType::Time32(_) | DataType::Time64(_), _] => {
                Ok(DataType::Time64(TimeUnit::Microsecond))
            }
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
        let shifted: PrimitiveArray<Time64NanosecondType> = try_binary(
            time.as_primitive::<Time64NanosecondType>(),
            interval.as_primitive::<DurationMicrosecondType>(),
            |time, micros| {
                let shifted = i128::from(time) + i128::from(micros) * NANOS_PER_MICRO;
                if !(0..NANOS_PER_DAY).contains(&shifted) {
                    return Err(ArrowError::ComputeError(
                        "[DATETIME_OVERFLOW] Datetime operation overflow: time plus interval is outside [00:00, 24:00). SQLSTATE: 22008".to_string(),
                    ));
                }
                Ok(shifted as i64)
            },
        )?;
        let shifted: ArrayRef = cast(&shifted, return_field.data_type())?;
        Ok(ColumnarValue::Array(shifted))
    }
}
