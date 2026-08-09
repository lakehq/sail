use std::collections::HashMap;
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
    /// Whether conversion failures should produce null instead of an error.
    safe: bool,
    signature: Signature,
}

impl ConvertTz {
    pub fn new(classic: bool) -> Self {
        Self::new_with_safe(classic, false)
    }

    pub fn new_with_safe(classic: bool, safe: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            classic,
            safe,
        }
    }

    pub fn classic(&self) -> bool {
        self.classic
    }

    pub fn safe(&self) -> bool {
        self.safe
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
        let nullable = self.safe || args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| convert_tz_inner(args, self.classic, self.safe),
            [Hint::AcceptsSingular].repeat(args.args.len()),
        )(args.args.as_slice())
    }
}

pub(super) fn convert_tz_inner(args: &[ArrayRef], classic: bool, safe: bool) -> Result<ArrayRef> {
    let convert = if classic {
        convert_tz_classic
    } else {
        convert_tz_non_classic
    };

    let mut timezone_cache = HashMap::<String, SparkTimeZone>::new();
    let mut parse_tz = |value: &str| -> Result<SparkTimeZone> {
        if let Some(timezone) = timezone_cache.get(value) {
            return Ok(*timezone);
        }
        let timezone = parse_spark_timezone(value)?;
        timezone_cache.insert(value.to_string(), timezone);
        Ok(timezone)
    };
    let mut convert_row = |ts_micros: Option<i64>, from_tz: Option<&str>, to_tz: Option<&str>| {
        let (Some(ts_micros), Some(from_tz), Some(to_tz)) = (ts_micros, from_tz, to_tz) else {
            return Ok(None);
        };
        let from_tz = parse_tz(from_tz)?;
        let to_tz = parse_tz(to_tz)?;
        match convert(ts_micros, &from_tz, &to_tz) {
            Err(_) if safe => Ok(None),
            result => result,
        }
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

        match (arr_lens[0] == 1, arr_lens[1] == 1) {
            (true, true) => {
                let from_tz = from_tz_strs.iter().next().flatten();
                let to_tz = to_tz_strs.iter().next().flatten();
                micros_arr
                    .iter()
                    .map(|ts| convert_row(ts, from_tz, to_tz))
                    .collect::<Result<Int64Array>>()
            }
            (true, false) => {
                let from_tz = from_tz_strs.iter().next().flatten();
                micros_arr
                    .iter()
                    .zip(to_tz_strs.iter())
                    .map(|(ts, to_tz)| convert_row(ts, from_tz, to_tz))
                    .collect::<Result<Int64Array>>()
            }
            (false, true) => {
                let to_tz = to_tz_strs.iter().next().flatten();
                micros_arr
                    .iter()
                    .zip(from_tz_strs.iter())
                    .map(|(ts, from_tz)| convert_row(ts, from_tz, to_tz))
                    .collect::<Result<Int64Array>>()
            }
            (false, false) => micros_arr
                .iter()
                .zip(from_tz_strs.iter().zip(to_tz_strs.iter()))
                .map(|(ts, (from_tz, to_tz))| convert_row(ts, from_tz, to_tz))
                .collect::<Result<Int64Array>>(),
        }
    }?;

    let time_unit = match args[2].data_type() {
        DataType::Timestamp(unit, None) => *unit,
        x => return exec_err!("invalid timestamp type for `convert_tz`: {x:?}"),
    };

    microseconds_to_timestamp(results, time_unit)
}

fn convert_fixed_offsets(
    ts_micros: i64,
    from_zone: &chrono::FixedOffset,
    to_zone: &chrono::FixedOffset,
) -> Result<Option<i64>> {
    let offset_micros =
        i64::from(to_zone.local_minus_utc() - from_zone.local_minus_utc()) * 1_000_000;
    let Some(value) = ts_micros.checked_add(offset_micros) else {
        return exec_err!("long overflow");
    };
    Ok(Some(value))
}

// FIXME: Named zones still use Chrono's bounded civil datetime and chrono-tz's finite
// transition table. Supporting Spark's full i64 microsecond domain requires a wide
// proleptic-Gregorian conversion and deterministic IANA rules that preserve recurring
// transitions, gap/overlap resolution, and initial local-mean-time offsets. Intermediate
// values must remain wide enough that only the final Spark timestamp is narrowed to i64.

/// Reference:
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#convertTimestampNtzToAnotherTz`
fn convert_tz_classic(
    ts_micros: i64,
    from_zone: &SparkTimeZone,
    to_zone: &SparkTimeZone,
) -> Result<Option<i64>> {
    if let (SparkTimeZone::Fixed(from), SparkTimeZone::Fixed(to)) = (from_zone, to_zone) {
        return convert_fixed_offsets(ts_micros, from, to);
    }
    let Some(local) = DateTime::from_timestamp_micros(ts_micros).map(|value| value.naive_utc())
    else {
        return Ok(None);
    };
    let Some(datetime) = localize_with_fallback(from_zone, &local).ok() else {
        return Ok(None);
    };
    Ok(Some(
        datetime
            .with_timezone(to_zone)
            .naive_local()
            .and_utc()
            .timestamp_micros(),
    ))
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.SparkDateTimeUtils#convertTz`
fn convert_tz_non_classic(
    ts_micros: i64,
    from_zone: &SparkTimeZone,
    to_zone: &SparkTimeZone,
) -> Result<Option<i64>> {
    if let (SparkTimeZone::Fixed(from), SparkTimeZone::Fixed(to)) = (from_zone, to_zone) {
        return convert_fixed_offsets(ts_micros, from, to);
    }
    let Some(local) = to_zone
        .timestamp_micros(ts_micros)
        .single()
        .map(|value| value.naive_local())
    else {
        return Ok(None);
    };
    let Some(datetime) = localize_with_fallback(from_zone, &local).ok() else {
        return Ok(None);
    };
    Ok(Some(datetime.timestamp_micros()))
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
    use datafusion::arrow::array::{
        StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{TimestampMicrosecondType, TimestampNanosecondType};

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

    #[test]
    fn safe_conversion_returns_null_on_overflow() -> Result<()> {
        let args: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["UTC"])),
            Arc::new(StringArray::from(vec!["+01:00"])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX)])),
        ];

        assert!(convert_tz_inner(&args, false, false).is_err());
        let output = convert_tz_inner(&args, false, true)?;
        assert_eq!(
            output.as_primitive::<TimestampMicrosecondType>(),
            &TimestampMicrosecondArray::from(vec![None]),
        );
        Ok(())
    }
}
