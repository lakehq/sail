use std::sync::Arc;

use chrono::{DateTime, TimeZone};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, UInt64Array};
use datafusion::arrow::compute::kernels::{cast, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type, TimeUnit};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Volatility,
};
use datafusion_expr_common::signature::Signature;
use datafusion_functions::utils::make_scalar_function;
use sail_common_datafusion::utils::datetime::{
    SparkTimeZone, localize_with_fallback, parse_spark_timezone,
};

/// A helper scalar UDF for converting time zones for timestamps.
/// The timestamp must be NTZ timestamp, which should have [`None`] time zone
/// in the Arrow data type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTz {
    /// Whether to use the "classic" algorithm to convert time zone.
    /// The "classic" algorithm is used by the `convert_timezone` function in Spark,
    /// while the "non-classic" algorithm is used by the `from_utc_timestamp` and
    /// `to_utc_timestamp` functions in Spark.
    classic: bool,
    signature: Signature,
}

impl ConvertTz {
    pub fn new(classic: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            classic,
        }
    }

    pub fn classic(&self) -> bool {
        self.classic
    }
}

impl ScalarUDFImpl for ConvertTz {
    fn name(&self) -> &str {
        "convert_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, _, ts] = arg_types else {
            return plan_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
        };
        match ts {
            DataType::Timestamp(unit, None) => Ok(DataType::Timestamp(*unit, None)),
            _ => plan_err!("`convert_tz` expects NTZ timestamp but got {ts:?}"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&arg_types)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| convert_tz_inner(args, self.classic),
            [Hint::AcceptsSingular].repeat(args.args.len()),
        )(args.args.as_slice())
    }
}

fn convert_tz_inner(args: &[ArrayRef], classic: bool) -> Result<ArrayRef> {
    let parse_tz = |input: Option<&str>| input.map(parse_spark_timezone).transpose();

    let convert = if classic {
        convert_tz_classic
    } else {
        convert_tz_non_classic
    };

    let from_to_utc_timestamp_func = |inputs: (
        Option<i64>,
        Result<Option<SparkTimeZone>>,
        Result<Option<SparkTimeZone>>,
    )| match inputs {
        (Some(ts_micros), Ok(Some(from_tz)), Ok(Some(to_tz))) => {
            Ok(convert(ts_micros, &from_tz, &to_tz))
        }
        (_, Err(e), _) | (_, _, Err(e)) => Err(e),
        _ => Ok(None),
    };

    let from_tz_strs_arr = cast::cast(&args[0], &DataType::Utf8)?;
    let to_tz_strs_arr = cast::cast(&args[1], &DataType::Utf8)?;
    let ts_arr = &args[2];

    let results: Int64Array = {
        let (from_tz_strs, to_tz_strs) = match (
            from_tz_strs_arr.as_string_opt::<i32>(),
            to_tz_strs_arr.as_string_opt::<i32>(),
        ) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                return exec_err!(
                    "`convert_timezone` first and second arguments must be string literal or array, received {:?}, {:?}",
                    args[0],
                    args[1]
                );
            }
        };

        let arr_lens = args.iter().map(|a| a.len()).collect::<Vec<_>>();
        let max_len = *arr_lens.iter().max().map_or_else(
            || exec_err!("`convert_timezone`: could not get array lengths max"),
            Ok,
        )?;

        let ts_arr = if ts_arr.len() != max_len && ts_arr.len() == 1 {
            let indices = (0..max_len).map(|_| 0u64).collect::<UInt64Array>();
            take::take(&ts_arr, &indices, None)?
        } else {
            ts_arr.clone()
        };

        let micros_arr = timestamp_to_microseconds(ts_arr.as_ref())?;

        let first = |iter: &mut dyn Iterator<Item = Result<Option<SparkTimeZone>>>| {
            iter.next().transpose().map(|opt| opt.flatten())
        };
        // lazy evaluated iterators
        let mut from_tzs = from_tz_strs.iter().map(parse_tz);
        let mut to_tzs = to_tz_strs.iter().map(parse_tz);

        match (arr_lens[0] == 1, arr_lens[1] == 1) {
            (true, true) => {
                let from_tz = first(&mut from_tzs)?;
                let to_tz = first(&mut to_tzs)?;

                micros_arr
                    .iter()
                    .map(|ts| from_to_utc_timestamp_func((ts, Ok(from_tz), Ok(to_tz))))
                    .collect::<Result<Int64Array>>()
            }
            (true, false) => {
                let from_tz = first(&mut from_tzs)?;
                micros_arr
                    .iter()
                    .zip(to_tzs)
                    .map(|(ts, to_tz)| from_to_utc_timestamp_func((ts, Ok(from_tz), to_tz)))
                    .collect::<Result<Int64Array>>()
            }
            (false, true) => {
                let to_tz = first(&mut to_tzs)?;

                micros_arr
                    .iter()
                    .zip(from_tzs)
                    .map(|(ts, from_tz)| from_to_utc_timestamp_func((ts, from_tz, Ok(to_tz))))
                    .collect::<Result<Int64Array>>()
            }
            (false, false) => micros_arr
                .iter()
                .zip(from_tzs.zip(to_tzs))
                .map(|(a, (b, c))| (a, b, c))
                .map(|(ts, from_tz, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                .collect::<Result<Int64Array>>(),
        }
    }?;

    let time_unit = match args[2].data_type() {
        DataType::Timestamp(unit, None) => *unit,
        x => return exec_err!("invalid timestamp type for `convert_tz`: {x:?}"),
    };

    microseconds_to_timestamp(results, time_unit)
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#convertTimestampNtzToAnotherTz`
fn convert_tz_classic(
    ts_micros: i64,
    from_zone: &SparkTimeZone,
    to_zone: &SparkTimeZone,
) -> Option<i64> {
    let local = DateTime::from_timestamp_micros(ts_micros)?.naive_utc();
    let datetime = localize_with_fallback(from_zone, &local).ok()?;
    Some(
        datetime
            .with_timezone(to_zone)
            .naive_local()
            .and_utc()
            .timestamp_micros(),
    )
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.SparkDateTimeUtils#convertTz`
fn convert_tz_non_classic(
    ts_micros: i64,
    from_zone: &SparkTimeZone,
    to_zone: &SparkTimeZone,
) -> Option<i64> {
    let local = to_zone.timestamp_micros(ts_micros).single()?.naive_local();
    let datetime = localize_with_fallback(from_zone, &local).ok()?;
    Some(datetime.timestamp_micros())
}

fn timestamp_to_microseconds(array: &dyn Array) -> Result<Int64Array> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let values = cast::cast(array, &DataType::Int64)?;
            let values = values.as_primitive::<Int64Type>();
            // Spark normalizes an instant to microseconds with floor division. Arrow's timestamp
            // cast truncates toward zero, which is incorrect for negative sub-microsecond values.
            Ok(values
                .iter()
                .map(|value| value.map(|value| value.div_euclid(1_000)))
                .collect())
        }
        DataType::Timestamp(_, None) => {
            let timestamp = cast::cast(array, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
            let values = cast::cast(timestamp.as_ref(), &DataType::Int64)?;
            Ok(values.as_primitive::<Int64Type>().clone())
        }
        _ => {
            exec_err!(
                "`convert_timezone`: third argument type must coerce to NTZ timestamp, received {:?}",
                array.data_type()
            )
        }
    }
}

fn microseconds_to_timestamp(array: Int64Array, time_unit: TimeUnit) -> Result<ArrayRef> {
    let timestamp = cast::cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
    Ok(cast::cast(
        timestamp.as_ref(),
        &DataType::Timestamp(time_unit, None),
    )?)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::TimestampNanosecondArray;
    use datafusion::arrow::datatypes::TimestampNanosecondType;

    use super::*;

    #[test]
    fn nanoseconds_are_normalized_to_spark_microseconds() -> Result<()> {
        let input = TimestampNanosecondArray::from(vec![Some(-999), Some(999), None]);
        let micros = timestamp_to_microseconds(&input)?;
        assert_eq!(micros, Int64Array::from(vec![Some(-1), Some(0), None]));

        let output = microseconds_to_timestamp(micros, TimeUnit::Nanosecond)?;
        assert_eq!(
            output.as_primitive::<TimestampNanosecondType>(),
            &TimestampNanosecondArray::from(vec![Some(-1_000), Some(0), None]),
        );
        Ok(())
    }
}
