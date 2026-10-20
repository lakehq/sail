// [CREDIT]: https://github.com/apache/datafusion/blob/b6f6990ed5f926fd04fe04c46410f8c6ddfb7674/datafusion/functions/src/datetime/date_trunc.rs

use std::collections::BTreeMap;
use std::num::NonZeroI64;
use std::ops::{Add, Sub};
use std::sync::Arc;

use arrow::array::temporal_conversions::{
    MICROSECONDS, MILLISECONDS, NANOSECONDS, as_datetime_with_timezone,
};
use arrow::array::timezone::Tz;
use arrow::array::types::{
    ArrowTimestampType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow::array::{Array, ArrayRef, PrimitiveArray, UInt64Array, new_null_array};
use arrow::compute::{DatePart, interleave, take};
use arrow::datatypes::DataType::{self, Time32, Time64, Timestamp};
use arrow::datatypes::TimeUnit::{self, Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{Field, FieldRef};
use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDateTime, Offset, TimeDelta, TimeZone, Timelike,
};
use chrono_tz::GapInfo;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::types::{NativeType, logical_date, logical_string};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, internal_err,
};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common::utils::datetime::{is_fixed_zero_offset_timezone, time_unit_to_multiplier};

use crate::scalar::datetime::utils::string_array_iter;

pub fn parse_granularity(value: &str) -> Result<DatePart> {
    // Extraction aliases are not truncation units; the planner normalizes Spark's aliases.
    match value.to_lowercase().as_str() {
        "microsecond" => Ok(DatePart::Microsecond),
        "millisecond" => Ok(DatePart::Millisecond),
        "second" => Ok(DatePart::Second),
        "minute" => Ok(DatePart::Minute),
        "hour" => Ok(DatePart::Hour),
        "day" => Ok(DatePart::Day),
        "week" => Ok(DatePart::Week),
        "month" => Ok(DatePart::Month),
        "quarter" => Ok(DatePart::Quarter),
        "year" => Ok(DatePart::Year),
        _ => {
            exec_err!(
                "Unsupported date_trunc granularity: '{value}'. Supported granularities are: microsecond, millisecond, second, minute, hour, day, week, month, quarter, year"
            )
        }
    }
}

trait DateTruncGranularityExt {
    fn is_fine_granularity(&self) -> bool;
    fn is_fine_granularity_utc(&self) -> bool;
    fn valid_for_time(&self) -> bool;
}

impl DateTruncGranularityExt for DatePart {
    /// Returns true if this granularity can be handled with simple arithmetic
    /// (fine granularity: second, millisecond, microsecond)
    fn is_fine_granularity(&self) -> bool {
        matches!(
            self,
            DatePart::Second | DatePart::Millisecond | DatePart::Microsecond
        )
    }

    /// Returns true if this granularity can be handled with simple arithmetic in UTC
    /// (minute, hour and day in addition to fine granularities)
    fn is_fine_granularity_utc(&self) -> bool {
        self.is_fine_granularity()
            || matches!(self, DatePart::Minute | DatePart::Hour | DatePart::Day)
    }

    /// Returns true if this granularity is valid for Time types
    /// Time types don't have date components, so day/week/month/quarter/year are not valid
    fn valid_for_time(&self) -> bool {
        matches!(
            self,
            DatePart::Hour
                | DatePart::Minute
                | DatePart::Second
                | DatePart::Millisecond
                | DatePart::Microsecond
        )
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Timestamp,
                            // Allow implicit cast from string and date to timestamp for backward compatibility
                            vec![
                                TypeSignatureClass::Native(logical_string()),
                                TypeSignatureClass::Native(logical_date()),
                            ],
                            NativeType::Timestamp(Microsecond, None),
                        ),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Time),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("date_trunc"), String::from("datetrunc")],
        }
    }
    fn invoke_dynamic(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ColumnarValue::Array(units) = &args.args[0] else {
            return internal_err!("dynamic truncation requires an array of units");
        };
        let mut groups = BTreeMap::<&str, Vec<usize>>::new();
        for (row, unit) in string_array_iter(units.as_ref())?.enumerate() {
            let Some(unit) = unit else { continue };
            let unit = match parse_granularity(unit) {
                Ok(DatePart::Year) => "year",
                Ok(DatePart::Quarter) => "quarter",
                Ok(DatePart::Month) => "month",
                Ok(DatePart::Week) => "week",
                Ok(DatePart::Day) => "day",
                Ok(DatePart::Hour) => "hour",
                Ok(DatePart::Minute) => "minute",
                Ok(DatePart::Second) => "second",
                Ok(DatePart::Millisecond) => "millisecond",
                Ok(DatePart::Microsecond) => "microsecond",
                _ => continue,
            };
            groups.entry(unit).or_default().push(row);
        }
        let mut results = vec![new_null_array(args.return_type(), 1)];
        let mut positions = vec![(0, 0); units.len()];
        for (unit, rows) in groups {
            let value = match &args.args[1] {
                ColumnarValue::Array(array) if rows.len() != units.len() => {
                    let indices = UInt64Array::from_iter_values(rows.iter().map(|row| *row as u64));
                    ColumnarValue::Array(take(array, &indices, None)?)
                }
                value => value.clone(),
            };
            let result = self
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(unit.to_owned()))),
                        value,
                    ],
                    arg_fields: args.arg_fields.clone(),
                    number_rows: rows.len(),
                    return_field: Arc::clone(&args.return_field),
                    config_options: Arc::clone(&args.config_options),
                })?
                .into_array(rows.len())?;
            if rows.len() == units.len() {
                return Ok(ColumnarValue::Array(result));
            }
            for (position, row) in rows.into_iter().enumerate() {
                positions[row] = (results.len(), position);
            }
            results.push(result);
        }
        let arrays = results
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        Ok(ColumnarValue::Array(interleave(&arrays, &positions)?))
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        "spark_date_trunc"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = &args.arg_fields[1];
        let return_type = if field.data_type().is_null() {
            Timestamp(Nanosecond, None)
        } else {
            field.data_type().clone()
        };
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if matches!(args.args.first(), Some(ColumnarValue::Array(_))) {
            return self.invoke_dynamic(args);
        }
        let return_field = args.return_field;
        let args = args.args;
        let (granularity, array) = (&args[0], &args[1]);

        if matches!(granularity, ColumnarValue::Scalar(value) if value.is_null()) {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                return_field.data_type(),
            )?));
        }

        let granularity_str = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = granularity
        {
            v.to_lowercase()
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) = granularity {
            v.to_lowercase()
        } else if let ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) = granularity {
            v.to_lowercase()
        } else {
            return exec_err!("Granularity of `date_trunc` must be non-null scalar Utf8");
        };

        let granularity = match parse_granularity(&granularity_str) {
            Ok(granularity) => granularity,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                    return_field.data_type(),
                )?));
            }
        };

        invoke_value(granularity, &granularity_str, array, false)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let precision = &input[0];
        let date_value = &input[1];

        let order_safe_input = matches!(
            date_value.range.data_type(),
            Timestamp(_, None) | Time32(_) | Time64(_)
        );

        if precision.sort_properties == SortProperties::Singleton && order_safe_input {
            Ok(date_value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }
}

fn invoke_value(
    granularity: DatePart,
    granularity_str: &str,
    array: &ColumnarValue,
    start_of_day: bool,
) -> Result<ColumnarValue> {
    // Check upfront if granularity is valid for Time types
    let is_time_type = matches!(array.data_type(), Time64(_) | Time32(_));
    if is_time_type && !granularity.valid_for_time() {
        return exec_err!(
            "date_trunc does not support '{}' granularity for Time types. Valid values are: hour, minute, second, millisecond, microsecond",
            granularity_str
        );
    }

    fn process_array<T: ArrowTimestampType>(
        array: &dyn Array,
        granularity: DatePart,
        tz_opt: Option<&Arc<str>>,
        start_of_day: bool,
    ) -> Result<ColumnarValue> {
        let parsed_tz = parse_tz(tz_opt)?;
        let array = as_primitive_array::<T>(array)?;

        if truncates_in_input_unit(granularity, parsed_tz.as_ref()) {
            let result = general_date_trunc_array_fine_granularity(
                T::UNIT,
                array,
                granularity,
                tz_opt.cloned(),
            )?;
            return Ok(ColumnarValue::Array(result));
        }

        let array: PrimitiveArray<T> = array
            .try_unary(|x| general_date_trunc(T::UNIT, x, parsed_tz, granularity, start_of_day))?
            .with_timezone_opt(tz_opt.cloned());
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn process_scalar<T: ArrowTimestampType>(
        v: Option<&i64>,
        granularity: DatePart,
        tz_opt: Option<&Arc<str>>,
        start_of_day: bool,
    ) -> Result<ColumnarValue> {
        let parsed_tz = parse_tz(tz_opt)?;
        let value = match v {
            // Truncate in the input's own unit, as `process_array` does, so a
            // scalar accepts every timestamp a column accepts.
            Some(v) if truncates_in_input_unit(granularity, parsed_tz.as_ref()) => {
                match fine_granularity_unit(T::UNIT, granularity) {
                    Some(unit) => Some(truncate_to_unit(*v, unit.get(), granularity)?),
                    // `granularity` is no coarser than the input's unit
                    None => Some(*v),
                }
            }
            Some(v) => Some(general_date_trunc(
                T::UNIT,
                *v,
                parsed_tz,
                granularity,
                start_of_day,
            )?),
            None => None,
        };
        let value = ScalarValue::new_timestamp::<T>(value, tz_opt.cloned());
        Ok(ColumnarValue::Scalar(value))
    }

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::Null) => {
            // NULL input returns NULL timestamp
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None, None))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            process_scalar::<TimestampNanosecondType>(
                v.as_ref(),
                granularity,
                tz_opt.as_ref(),
                start_of_day,
            )?
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, tz_opt)) => {
            process_scalar::<TimestampMicrosecondType>(
                v.as_ref(),
                granularity,
                tz_opt.as_ref(),
                start_of_day,
            )?
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, tz_opt)) => {
            process_scalar::<TimestampMillisecondType>(
                v.as_ref(),
                granularity,
                tz_opt.as_ref(),
                start_of_day,
            )?
        }
        ColumnarValue::Scalar(ScalarValue::TimestampSecond(v, tz_opt)) => {
            process_scalar::<TimestampSecondType>(
                v.as_ref(),
                granularity,
                tz_opt.as_ref(),
                start_of_day,
            )?
        }
        ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(v)) => {
            let truncated = v.map(|val| truncate_time_nanos(val, granularity));
            ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(truncated))
        }
        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(v)) => {
            let truncated = v.map(|val| truncate_time_micros(val, granularity));
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(truncated))
        }
        ColumnarValue::Scalar(ScalarValue::Time32Millisecond(v)) => {
            let truncated = v.map(|val| truncate_time_millis(val, granularity));
            ColumnarValue::Scalar(ScalarValue::Time32Millisecond(truncated))
        }
        ColumnarValue::Scalar(ScalarValue::Time32Second(v)) => {
            let truncated = v.map(|val| truncate_time_secs(val, granularity));
            ColumnarValue::Scalar(ScalarValue::Time32Second(truncated))
        }
        ColumnarValue::Array(array) => {
            let array_type = array.data_type();
            match array_type {
                Timestamp(Second, tz_opt) => process_array::<TimestampSecondType>(
                    array,
                    granularity,
                    tz_opt.as_ref(),
                    start_of_day,
                )?,
                Timestamp(Millisecond, tz_opt) => process_array::<TimestampMillisecondType>(
                    array,
                    granularity,
                    tz_opt.as_ref(),
                    start_of_day,
                )?,
                Timestamp(Microsecond, tz_opt) => process_array::<TimestampMicrosecondType>(
                    array,
                    granularity,
                    tz_opt.as_ref(),
                    start_of_day,
                )?,
                Timestamp(Nanosecond, tz_opt) => process_array::<TimestampNanosecondType>(
                    array,
                    granularity,
                    tz_opt.as_ref(),
                    start_of_day,
                )?,
                Time64(Nanosecond) => {
                    let arr = as_primitive_array::<Time64NanosecondType>(array)?;
                    let result: PrimitiveArray<Time64NanosecondType> =
                        arr.unary(|v| truncate_time_nanos(v, granularity));
                    ColumnarValue::Array(Arc::new(result))
                }
                Time64(Microsecond) => {
                    let arr = as_primitive_array::<Time64MicrosecondType>(array)?;
                    let result: PrimitiveArray<Time64MicrosecondType> =
                        arr.unary(|v| truncate_time_micros(v, granularity));
                    ColumnarValue::Array(Arc::new(result))
                }
                Time32(Millisecond) => {
                    let arr = as_primitive_array::<Time32MillisecondType>(array)?;
                    let result: PrimitiveArray<Time32MillisecondType> =
                        arr.unary(|v| truncate_time_millis(v, granularity));
                    ColumnarValue::Array(Arc::new(result))
                }
                Time32(Second) => {
                    let arr = as_primitive_array::<Time32SecondType>(array)?;
                    let result: PrimitiveArray<Time32SecondType> =
                        arr.unary(|v| truncate_time_secs(v, granularity));
                    ColumnarValue::Array(Arc::new(result))
                }
                _ => {
                    return exec_err!(
                        "second argument of `date_trunc` is an unsupported array type: {array_type}"
                    );
                }
            }
        }
        _ => {
            return exec_err!(
                "second argument of `date_trunc` must be timestamp, time scalar or array"
            );
        }
    })
}

const NANOS_PER_MICROSECOND: i64 = NANOSECONDS / MICROSECONDS;
const NANOS_PER_MILLISECOND: i64 = NANOSECONDS / MILLISECONDS;
const NANOS_PER_SECOND: i64 = NANOSECONDS;
const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;

const MICROS_PER_MILLISECOND: i64 = MICROSECONDS / MILLISECONDS;
const MICROS_PER_SECOND: i64 = MICROSECONDS;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;

const MILLIS_PER_SECOND: i32 = MILLISECONDS as i32;
const MILLIS_PER_MINUTE: i32 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i32 = 60 * MILLIS_PER_MINUTE;

const SECS_PER_MINUTE: i32 = 60;
const SECS_PER_HOUR: i32 = 60 * SECS_PER_MINUTE;

/// Truncate time in nanoseconds to the specified granularity
fn truncate_time_nanos(value: i64, granularity: DatePart) -> i64 {
    match granularity {
        DatePart::Hour => value - (value % NANOS_PER_HOUR),
        DatePart::Minute => value - (value % NANOS_PER_MINUTE),
        DatePart::Second => value - (value % NANOS_PER_SECOND),
        DatePart::Millisecond => value - (value % NANOS_PER_MILLISECOND),
        DatePart::Microsecond => value - (value % NANOS_PER_MICROSECOND),
        // Other granularities are not valid for time - should be caught earlier
        _ => value,
    }
}

/// Truncate time in microseconds to the specified granularity
fn truncate_time_micros(value: i64, granularity: DatePart) -> i64 {
    match granularity {
        DatePart::Hour => value - (value % MICROS_PER_HOUR),
        DatePart::Minute => value - (value % MICROS_PER_MINUTE),
        DatePart::Second => value - (value % MICROS_PER_SECOND),
        DatePart::Millisecond => value - (value % MICROS_PER_MILLISECOND),
        DatePart::Microsecond => value, // Already at microsecond precision
        // Other granularities are not valid for time
        _ => value,
    }
}

/// Truncate time in milliseconds to the specified granularity
fn truncate_time_millis(value: i32, granularity: DatePart) -> i32 {
    match granularity {
        DatePart::Hour => value - (value % MILLIS_PER_HOUR),
        DatePart::Minute => value - (value % MILLIS_PER_MINUTE),
        DatePart::Second => value - (value % MILLIS_PER_SECOND),
        DatePart::Millisecond => value, // Already at millisecond precision
        DatePart::Microsecond => value, // Can't truncate to finer precision
        // Other granularities are not valid for time
        _ => value,
    }
}

/// Truncate time in seconds to the specified granularity
fn truncate_time_secs(value: i32, granularity: DatePart) -> i32 {
    match granularity {
        DatePart::Hour => value - (value % SECS_PER_HOUR),
        DatePart::Minute => value - (value % SECS_PER_MINUTE),
        DatePart::Second => value,      // Already at second precision
        DatePart::Millisecond => value, // Can't truncate to finer precision
        DatePart::Microsecond => value, // Can't truncate to finer precision
        // Other granularities are not valid for time
        _ => value,
    }
}

fn _date_trunc_coarse<T>(granularity: DatePart, value: Option<T>) -> Result<Option<T>>
where
    T: Datelike + Timelike + Sub<Duration, Output = T> + Copy,
{
    let value = match granularity {
        DatePart::Millisecond => value,
        DatePart::Microsecond => value,
        DatePart::Second => value.and_then(|d| d.with_nanosecond(0)),
        DatePart::Minute => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0)),
        DatePart::Hour => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        DatePart::Day => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0)),
        DatePart::Week => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| {
                TimeDelta::try_seconds(60 * 60 * 24 * d.weekday() as i64).map(|delta| d - delta)
            }),
        DatePart::Month => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0)),
        DatePart::Quarter => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month(quarter_month(&d))),
        DatePart::Year => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month0(0)),
        _ => unreachable!("unsupported date_trunc granularity"),
    };
    Ok(value)
}

fn quarter_month<T>(date: &T) -> u32
where
    T: Datelike,
{
    1 + 3 * ((date.month() - 1) / 3)
}

fn _date_trunc_coarse_with_tz(
    granularity: DatePart,
    value: DateTime<Tz>,
    unit: TimeUnit,
    start_of_day: bool,
) -> Result<Option<i64>> {
    let calendar = start_of_day
        || matches!(
            granularity,
            DatePart::Week | DatePart::Month | DatePart::Quarter | DatePart::Year
        );
    let local = value.naive_local();
    let truncated = _date_trunc_coarse::<NaiveDateTime>(granularity, Some(local))?;
    let truncated = truncated.and_then(|truncated| {
        match truncated.and_local_timezone(value.timezone()) {
            LocalResult::None => {
                if calendar {
                    // Calendar boundaries use the first valid time of the date.
                    let named = value.timezone().to_string().parse::<chrono_tz::Tz>().ok()?;
                    return GapInfo::new(&truncated, &named)?
                        .end
                        .map(|end| end.with_timezone(&value.timezone()));
                }
                if granularity == DatePart::Minute {
                    // Spark shifts a boundary in a gap forward by the gap's exact duration.
                    let named = value.timezone().to_string().parse::<chrono_tz::Tz>().ok()?;
                    let before = GapInfo::new(&truncated, &named)?.begin?.1.fix();
                    return truncated
                        .and_local_timezone(before)
                        .single()
                        .map(|datetime| datetime.with_timezone(&value.timezone()));
                }
                // This can happen if the date_trunc operation moves the time into
                // an hour that doesn't exist due to daylight savings. On known example where
                // this can happen is with historic dates in the America/Sao_Paulo time zone.
                // To account for this adjust the time by a few hours, convert to local time,
                // and then adjust the time back.
                let delta = TimeDelta::try_hours(3)?;
                truncated
                    .sub(delta)
                    .and_local_timezone(value.timezone())
                    .single()
                    .map(|v| v.add(delta))
            }
            LocalResult::Single(datetime) => Some(datetime),
            LocalResult::Ambiguous(datetime1, datetime2) => {
                // Calendar boundaries choose the earliest instant; MINUTE, HOUR and DAY
                // retain the original offset when the local time repeats.
                if calendar || datetime1.offset().fix() == value.offset().fix() {
                    Some(datetime1)
                } else {
                    Some(datetime2)
                }
            }
        }
    });
    Ok(truncated.and_then(|value| match unit {
        Second => Some(value.timestamp()),
        Millisecond => Some(value.timestamp_millis()),
        Microsecond => Some(value.timestamp_micros()),
        Nanosecond => value.timestamp_nanos_opt(),
    }))
}

// The two helpers below duplicate `chrono::NaiveDate::{from_epoch_days,
// to_epoch_days}`. They are kept separate because chrono's versions round trip
// through a validated `NaiveDate`: `from_epoch_days` computes year flags and
// returns an `Option`, and reading the year/month/day back out decodes them from
// its packed representation. These helpers stay in plain integers, which is all
// the truncation below needs.

/// Days from the Unix epoch to 0000-03-01, the epoch used by the civil calendar
/// conversions below.
const DAYS_EPOCH_SHIFT: i64 = 719_468;

/// Days in a 400 year era of the proleptic Gregorian calendar.
const DAYS_PER_ERA: i64 = 146_097;

/// Splits a day count relative to the Unix epoch into a proleptic Gregorian
/// year, month (1-12) and day of month (1-31).
///
/// This is a port of Howard Hinnant's `civil_from_days`, which documents the
/// derivation of the constants and the March-based year used below:
/// <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + DAYS_EPOCH_SHIFT;
    let era = z.div_euclid(DAYS_PER_ERA);
    let day_of_era = z.rem_euclid(DAYS_PER_ERA);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    // Month index with March as 0, so that the leap day falls at the end of the year.
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Inverse of [`civil_from_days`]: the day count relative to the Unix epoch for
/// the given proleptic Gregorian date.
///
/// This is a port of Howard Hinnant's `days_from_civil`, which documents the
/// derivation of the constants and the March-based year used below:
/// <https://howardhinnant.github.io/date_algorithms.html#days_from_civil>
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_index = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * DAYS_PER_ERA + day_of_era - DAYS_EPOCH_SHIFT
}

/// Truncates a UTC timestamp in its input unit with integer arithmetic. Truncating on
/// the calendar directly avoids converting every value to a `NaiveDateTime` and
/// rebuilding it field by field.
///
/// Returns `None` when the truncated timestamp is no longer representable as
/// units since the epoch, which the caller reports as an out of range
/// error.
fn _date_trunc_coarse_without_tz(granularity: DatePart, value: i64, unit: TimeUnit) -> Option<i64> {
    let units_per_second = match unit {
        Second => 1,
        Millisecond => 1_000,
        Microsecond => 1_000_000,
        Nanosecond => NANOSECONDS,
    };
    let units_per_day = 86_400 * units_per_second;
    let truncate_to = |unit: i64| value.checked_sub(value.rem_euclid(unit));
    let days = || value.div_euclid(units_per_day);
    let from_days = |days: i64| days.checked_mul(units_per_day);

    match granularity {
        // Sub-second granularities are applied by the caller in the input unit.
        DatePart::Millisecond | DatePart::Microsecond => Some(value),
        DatePart::Second => truncate_to(units_per_second),
        DatePart::Minute => truncate_to(60 * units_per_second),
        DatePart::Hour => truncate_to(3_600 * units_per_second),
        DatePart::Day => from_days(days()),
        DatePart::Week => {
            let days = days();
            // `Weekday::num_days_from_monday` for the epoch (a Thursday) is 3.
            from_days(days - (days + 3).rem_euclid(7))
        }
        DatePart::Month => {
            let days = days();
            let (_, _, day_of_month) = civil_from_days(days);
            from_days(days - (day_of_month - 1))
        }
        DatePart::Quarter => {
            let (year, month, _) = civil_from_days(days());
            from_days(days_from_civil(year, 1 + 3 * ((month - 1) / 3), 1))
        }
        DatePart::Year => {
            let (year, _, _) = civil_from_days(days());
            from_days(days_from_civil(year, 1, 1))
        }
        _ => unreachable!("unsupported date_trunc granularity"),
    }
}

/// Truncates the single `value`, expressed in its input unit since the
/// epoch, for granularities greater than 1 second, in taking into
/// account that some granularities are not uniform durations of time
/// (e.g. months are not always the same lengths, leap seconds, etc)
fn date_trunc_coarse(
    granularity: DatePart,
    value: i64,
    tz: Option<Tz>,
    unit: TimeUnit,
    start_of_day: bool,
) -> Result<i64> {
    let input = value;
    let value = match tz {
        Some(tz) => {
            // Use chrono DateTime<Tz> to clear the various fields because need to clear per timezone,
            // and NaiveDateTime (ISO 8601) has no concept of timezones
            let datetime = match unit {
                Second => as_datetime_with_timezone::<TimestampSecondType>(value, tz),
                Millisecond => as_datetime_with_timezone::<TimestampMillisecondType>(value, tz),
                Microsecond => as_datetime_with_timezone::<TimestampMicrosecondType>(value, tz),
                Nanosecond => as_datetime_with_timezone::<TimestampNanosecondType>(value, tz),
            }
            .ok_or(exec_datafusion_err!("Timestamp {value} out of range"))?;
            _date_trunc_coarse_with_tz(granularity, datetime, unit, start_of_day)?
        }
        None => _date_trunc_coarse_without_tz(granularity, value, unit),
    };

    value.ok_or_else(|| {
        let granularity = granularity.to_string().to_lowercase();
        exec_datafusion_err!("Timestamp {input} out of range after truncating to {granularity}")
    })
}

/// Fast path for fine granularities (hour and smaller) that can be handled
/// with simple arithmetic operations without calendar complexity.
///
/// This function is timezone-agnostic and should only be used when:
/// - No timezone is specified in the input, OR
/// - The granularity is less than minute, since offsets can include seconds
fn general_date_trunc_array_fine_granularity<T: ArrowTimestampType>(
    tu: TimeUnit,
    array: &PrimitiveArray<T>,
    granularity: DatePart,
    tz_opt: Option<Arc<str>>,
) -> Result<ArrayRef> {
    let unit = fine_granularity_unit(tu, granularity);

    if let Some(unit) = unit {
        let unit = unit.get();
        // Truncation can only underflow within one `unit` of `i64::MIN`.
        // Track that possibility while computing the common case so the loop
        // remains infallible and can be vectorized.
        let underflow_bound = i64::MIN + unit;
        let mut maybe_underflow = false;
        let values: Vec<i64> = array
            .values()
            .iter()
            .map(|value| {
                maybe_underflow |= *value < underflow_bound;
                value.wrapping_sub(value.rem_euclid(unit))
            })
            .collect();
        let array: PrimitiveArray<T> = if maybe_underflow {
            array.try_unary(|value| truncate_to_unit(value, unit, granularity))?
        } else {
            PrimitiveArray::new(values.into(), array.nulls().cloned())
        }
        .with_timezone_opt(tz_opt);
        Ok(Arc::new(array))
    } else {
        // truncate to the same or smaller unit
        Ok(Arc::new(array.clone()))
    }
}

/// Whether truncating to `granularity` is plain arithmetic in the input's own
/// time unit: rounding down to a multiple of [`fine_granularity_unit`].
///
/// In UTC, minute, hour and day also have uniform, UTC-aligned boundaries.
fn truncates_in_input_unit(granularity: DatePart, tz: Option<&Tz>) -> bool {
    granularity.is_fine_granularity() || (tz.is_none() && granularity.is_fine_granularity_utc())
}

/// The length of `granularity` in `tu`, or `None` when `granularity` is no
/// coarser than `tu`, so a value in `tu` is already truncated to it.
fn fine_granularity_unit(tu: TimeUnit, granularity: DatePart) -> Option<NonZeroI64> {
    match (tu, granularity) {
        (Second, DatePart::Minute) => NonZeroI64::new(60),
        (Second, DatePart::Hour) => NonZeroI64::new(3600),
        (Second, DatePart::Day) => NonZeroI64::new(86400),

        (Millisecond, DatePart::Second) => NonZeroI64::new(1_000),
        (Millisecond, DatePart::Minute) => NonZeroI64::new(60_000),
        (Millisecond, DatePart::Hour) => NonZeroI64::new(3_600_000),
        (Millisecond, DatePart::Day) => NonZeroI64::new(86_400_000),

        (Microsecond, DatePart::Millisecond) => NonZeroI64::new(1_000),
        (Microsecond, DatePart::Second) => NonZeroI64::new(1_000_000),
        (Microsecond, DatePart::Minute) => NonZeroI64::new(60_000_000),
        (Microsecond, DatePart::Hour) => NonZeroI64::new(3_600_000_000),
        (Microsecond, DatePart::Day) => NonZeroI64::new(86_400_000_000),

        (Nanosecond, DatePart::Microsecond) => NonZeroI64::new(1_000),
        (Nanosecond, DatePart::Millisecond) => NonZeroI64::new(1_000_000),
        (Nanosecond, DatePart::Second) => NonZeroI64::new(1_000_000_000),
        (Nanosecond, DatePart::Minute) => NonZeroI64::new(60_000_000_000),
        (Nanosecond, DatePart::Hour) => NonZeroI64::new(3_600_000_000_000),
        (Nanosecond, DatePart::Day) => NonZeroI64::new(86_400_000_000_000),
        _ => None,
    }
}

/// Rounds `value` down to a multiple of `unit`, the length of `granularity` in
/// the value's time unit, or errors if that falls below `i64::MIN`.
fn truncate_to_unit(value: i64, unit: i64, granularity: DatePart) -> Result<i64> {
    value.checked_sub(value.rem_euclid(unit)).ok_or_else(|| {
        exec_datafusion_err!("Timestamp {value} out of range after truncating to {granularity}")
    })
}

// truncates a single value with the given timeunit to the specified granularity
fn general_date_trunc(
    tu: TimeUnit,
    value: i64,
    tz: Option<Tz>,
    granularity: DatePart,
    start_of_day: bool,
) -> Result<i64, DataFusionError> {
    if truncates_in_input_unit(granularity, tz.as_ref()) {
        return match fine_granularity_unit(tu, granularity) {
            Some(unit) => truncate_to_unit(value, unit.get(), granularity),
            None => Ok(value),
        };
    }
    if granularity == DatePart::Minute
        && let Some(tz) = tz
    {
        // Match Spark's truncToUnitFast: round in local time, then verify the offset.
        let units_per_second = time_unit_to_multiplier(&tu);
        let unit = 60 * units_per_second;
        let offset_at = |value: i64| {
            let seconds = value.div_euclid(units_per_second);
            // Chrono-tz's first and last offset periods extend beyond Chrono's range.
            let utc = DateTime::from_timestamp(seconds, 0)
                .map(|datetime| datetime.naive_utc())
                .unwrap_or(if seconds < 0 {
                    NaiveDateTime::MIN
                } else {
                    NaiveDateTime::MAX
                });
            i64::from(tz.offset_from_utc_datetime(&utc).fix().local_minus_utc())
        };
        let offset = offset_at(value);
        // Reduce before adding so a valid timestamp near i64's bounds cannot overflow.
        let remainder =
            (value.rem_euclid(unit) + offset.rem_euclid(60) * units_per_second).rem_euclid(unit);
        if let Some(candidate) = value.checked_sub(remainder)
            && offset_at(candidate) == offset
        {
            return Ok(candidate);
        }
        // A transition (or arithmetic overflow) needs Spark's local-time resolution.
    }
    date_trunc_coarse(granularity, value, tz, tu, start_of_day)
}

// [CREDIT]: https://github.com/apache/datafusion/blob/b6f6990ed5f926fd04fe04c46410f8c6ddfb7674/datafusion/functions/src/datetime/common.rs
fn parse_tz(tz: Option<&Arc<str>>) -> Result<Option<Tz>> {
    // Sail keeps fixed-zero zones on the integer path, including timestamps beyond Chrono's range.
    if tz.is_some_and(|tz| is_fixed_zero_offset_timezone(tz)) {
        return Ok(None);
    }
    tz.map(|tz| {
        tz.parse::<Tz>()
            .map_err(|op| exec_datafusion_err!("failed on timezone {tz}: {op:?}"))
    })
    .transpose()
}

/// The earliest valid local midnight used by Spark's daysToMicros.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStartOfDay {
    signature: Signature,
}

impl Default for SparkStartOfDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStartOfDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Timestamp)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkStartOfDay {
    fn name(&self) -> &str {
        "spark_start_of_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = &args.arg_fields[0];
        Ok(Arc::new(Field::new(
            self.name(),
            field.data_type().clone(),
            field.is_nullable(),
        )))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_value(DatePart::Day, "day", &args.args[0], true)
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::panic)]

    use std::sync::Arc;

    use arrow::array::cast::as_primitive_array;
    use arrow::array::types::{ArrowTimestampType, TimestampNanosecondType};
    use arrow::array::{
        Array, PrimitiveArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::compute::{DatePart, SortOptions};
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::interval_arithmetic::Interval;
    use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use super::{
        NANOS_PER_MICROSECOND, SparkDateTrunc, date_trunc_coarse, general_date_trunc,
        general_date_trunc_array_fine_granularity, parse_granularity,
    };

    #[test]
    fn output_ordering_respects_timestamp_timezone() {
        let precision_value = ScalarValue::Utf8(Some("hour".into()));
        let precision = ExprProperties::new_unknown()
            .with_order(SortProperties::Singleton)
            .with_range(Interval::try_new(precision_value.clone(), precision_value).unwrap());
        let ordered = SortProperties::Ordered(SortOptions::default());
        let date_value = |data_type| {
            ExprProperties::new_unknown()
                .with_order(ordered)
                .with_range(Interval::make_unbounded(&data_type).unwrap())
        };
        let function = SparkDateTrunc::new();

        let timestamp = date_value(DataType::Timestamp(TimeUnit::Second, None));
        assert_eq!(
            function
                .output_ordering(&[precision.clone(), timestamp])
                .unwrap(),
            ordered
        );
        let timestamp_with_timezone = date_value(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/Goose_Bay".into()),
        ));
        assert_eq!(
            function
                .output_ordering(&[precision.clone(), timestamp_with_timezone])
                .unwrap(),
            SortProperties::Unordered
        );
        let unknown = ExprProperties::new_unknown().with_order(ordered);
        assert_eq!(
            function.output_ordering(&[precision, unknown]).unwrap(),
            SortProperties::Unordered
        );
    }

    #[test]
    fn date_trunc_test() {
        let cases = vec![
            (
                "2020-09-08T13:42:29.190855Z",
                "second",
                "2020-09-08T13:42:29.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "minute",
                "2020-09-08T13:42:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "hour",
                "2020-09-08T13:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "day",
                "2020-09-08T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "week",
                "2020-09-07T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "month",
                "2020-09-01T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "year",
                "2020-01-01T00:00:00.000000Z",
            ),
            // week
            (
                "2021-01-01T13:42:29.190855Z",
                "week",
                "2020-12-28T00:00:00.000000Z",
            ),
            (
                "2020-01-01T13:42:29.190855Z",
                "week",
                "2019-12-30T00:00:00.000000Z",
            ),
            // quarter
            (
                "2020-01-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-02-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-03-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-04-01T13:42:29.190855Z",
                "quarter",
                "2020-04-01T00:00:00.000000Z",
            ),
            (
                "2020-08-01T13:42:29.190855Z",
                "quarter",
                "2020-07-01T00:00:00.000000Z",
            ),
            (
                "2020-11-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
            (
                "2020-12-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
        ];

        for (original, granularity, expected) in &cases {
            let left = string_to_timestamp_nanos(original).unwrap();
            let right = string_to_timestamp_nanos(expected).unwrap();
            let granularity_enum = parse_granularity(granularity).unwrap();
            let result =
                date_trunc_coarse(granularity_enum, left, None, TimeUnit::Nanosecond, false)
                    .unwrap();
            assert_eq!(result, right, "{original} = {expected}");
        }
    }

    #[test]
    fn date_trunc_out_of_range_lower_bound_returns_error() {
        let timestamp = string_to_timestamp_nanos("1677-09-22T00:00:00Z").unwrap();
        let err = date_trunc_coarse(DatePart::Year, timestamp, None, TimeUnit::Nanosecond, false)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("out of range after truncating to year"),
            "{err}"
        );
    }

    #[test]
    fn test_date_trunc_timezones() {
        let cases = [
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("+00".into()),
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                None,
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("-02".into()),
                vec![
                    "2020-09-07T02:00:00Z",
                    "2020-09-07T02:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T02:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T01:00:00+05",
                    "2020-09-08T02:00:00+05",
                    "2020-09-08T03:00:00+05",
                    "2020-09-08T04:00:00+05",
                ],
                Some("+05".into()),
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T01:00:00+08",
                    "2020-09-08T02:00:00+08",
                    "2020-09-08T03:00:00+08",
                    "2020-09-08T04:00:00+08",
                ],
                Some("+08".into()),
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                ],
            ),
            (
                vec![
                    "2024-10-26T23:00:00Z",
                    "2024-10-27T00:00:00Z",
                    "2024-10-27T01:00:00Z",
                    "2024-10-27T02:00:00Z",
                ],
                Some("Europe/Berlin".into()),
                vec![
                    "2024-10-27T00:00:00+02",
                    "2024-10-27T00:00:00+02",
                    "2024-10-27T00:00:00+02",
                    "2024-10-27T00:00:00+02",
                ],
            ),
            (
                vec![
                    "2018-02-18T00:00:00Z",
                    "2018-02-18T01:00:00Z",
                    "2018-02-18T02:00:00Z",
                    "2018-02-18T03:00:00Z",
                    "2018-11-04T01:00:00Z",
                    "2018-11-04T02:00:00Z",
                    "2018-11-04T03:00:00Z",
                    "2018-11-04T04:00:00Z",
                ],
                Some("America/Sao_Paulo".into()),
                vec![
                    "2018-02-17T00:00:00-02",
                    "2018-02-17T00:00:00-02",
                    "2018-02-17T00:00:00-02",
                    "2018-02-18T00:00:00-03",
                    "2018-11-03T00:00:00-03",
                    "2018-11-03T00:00:00-03",
                    "2018-11-04T01:00:00-02",
                    "2018-11-04T01:00:00-02",
                ],
            ),
        ];

        for (original, tz_opt, expected) in &cases {
            let input = original
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let right = expected
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let batch_len = input.len();
            let arg_fields = vec![
                Field::new("a", DataType::Utf8, false).into(),
                Field::new("b", input.data_type().clone(), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::from("day")),
                    ColumnarValue::Array(Arc::new(input)),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new(
                    "f",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                    true,
                )
                .into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = SparkDateTrunc::new().invoke_with_args(args).unwrap();
            if let ColumnarValue::Array(result) = result {
                assert_eq!(
                    result.data_type(),
                    &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
                );
                let left = as_primitive_array::<TimestampNanosecondType>(&result);
                assert_eq!(left, &right);
            } else {
                panic!("unexpected column type");
            }
        }
    }

    #[test]
    fn test_date_trunc_hour_timezones() {
        let cases = [
            (
                vec![
                    "2020-09-08T00:30:00Z",
                    "2020-09-08T01:30:00Z",
                    "2020-09-08T02:30:00Z",
                    "2020-09-08T03:30:00Z",
                    "2020-09-08T04:30:00Z",
                ],
                Some("+00".into()),
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00Z",
                    "2020-09-08T01:30:00Z",
                    "2020-09-08T02:30:00Z",
                    "2020-09-08T03:30:00Z",
                    "2020-09-08T04:30:00Z",
                ],
                None,
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00Z",
                    "2020-09-08T01:30:00Z",
                    "2020-09-08T02:30:00Z",
                    "2020-09-08T03:30:00Z",
                    "2020-09-08T04:30:00Z",
                ],
                Some("-02".into()),
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00+05",
                    "2020-09-08T01:30:00+05",
                    "2020-09-08T02:30:00+05",
                    "2020-09-08T03:30:00+05",
                    "2020-09-08T04:30:00+05",
                ],
                Some("+05".into()),
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T01:00:00+05",
                    "2020-09-08T02:00:00+05",
                    "2020-09-08T03:00:00+05",
                    "2020-09-08T04:00:00+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00+08",
                    "2020-09-08T01:30:00+08",
                    "2020-09-08T02:30:00+08",
                    "2020-09-08T03:30:00+08",
                    "2020-09-08T04:30:00+08",
                ],
                Some("+08".into()),
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T01:00:00+08",
                    "2020-09-08T02:00:00+08",
                    "2020-09-08T03:00:00+08",
                    "2020-09-08T04:00:00+08",
                ],
            ),
            (
                vec![
                    "2024-10-26T23:30:00Z",
                    "2024-10-27T00:30:00Z",
                    "2024-10-27T01:30:00Z",
                    "2024-10-27T02:30:00Z",
                ],
                Some("Europe/Berlin".into()),
                vec![
                    "2024-10-27T01:00:00+02",
                    "2024-10-27T02:00:00+02",
                    "2024-10-27T02:00:00+01",
                    "2024-10-27T03:00:00+01",
                ],
            ),
            (
                vec![
                    "2018-02-18T00:30:00Z",
                    "2018-02-18T01:30:00Z",
                    "2018-02-18T02:30:00Z",
                    "2018-02-18T03:30:00Z",
                    "2018-11-04T01:00:00Z",
                    "2018-11-04T02:00:00Z",
                    "2018-11-04T03:00:00Z",
                    "2018-11-04T04:00:00Z",
                ],
                Some("America/Sao_Paulo".into()),
                vec![
                    "2018-02-17T22:00:00-02",
                    "2018-02-17T23:00:00-02",
                    "2018-02-17T23:00:00-03",
                    "2018-02-18T00:00:00-03",
                    "2018-11-03T22:00:00-03",
                    "2018-11-03T23:00:00-03",
                    "2018-11-04T01:00:00-02",
                    "2018-11-04T02:00:00-02",
                ],
            ),
            (
                vec![
                    "2024-10-26T23:30:00Z",
                    "2024-10-27T00:30:00Z",
                    "2024-10-27T01:30:00Z",
                    "2024-10-27T02:30:00Z",
                ],
                Some("Asia/Kathmandu".into()), // UTC+5:45
                vec![
                    "2024-10-27T05:00:00+05:45",
                    "2024-10-27T06:00:00+05:45",
                    "2024-10-27T07:00:00+05:45",
                    "2024-10-27T08:00:00+05:45",
                ],
            ),
        ];

        for (original, tz_opt, expected) in &cases {
            let input = original
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let right = expected
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let batch_len = input.len();
            let arg_fields = vec![
                Field::new("a", DataType::Utf8, false).into(),
                Field::new("b", input.data_type().clone(), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::from("hour")),
                    ColumnarValue::Array(Arc::new(input)),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new(
                    "f",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                    true,
                )
                .into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = SparkDateTrunc::new().invoke_with_args(args).unwrap();
            if let ColumnarValue::Array(result) = result {
                assert_eq!(
                    result.data_type(),
                    &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
                );
                let left = as_primitive_array::<TimestampNanosecondType>(&result);
                assert_eq!(left, &right);
            } else {
                panic!("unexpected column type");
            }
        }
    }

    #[test]
    fn scalar_fine_granularity_floors_negative_timestamps() {
        for (unit, value, granularity, expected) in [
            (TimeUnit::Microsecond, -999, DatePart::Millisecond, -1_000),
            (TimeUnit::Nanosecond, -999, DatePart::Microsecond, -1_000),
            (
                TimeUnit::Nanosecond,
                -999_999,
                DatePart::Millisecond,
                -1_000_000,
            ),
        ] {
            assert_eq!(
                general_date_trunc(unit, value, None, granularity, false).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn scalar_fine_granularity_rejects_underflow() {
        for granularity in [DatePart::Microsecond, DatePart::Millisecond] {
            assert!(
                general_date_trunc(TimeUnit::Nanosecond, i64::MIN, None, granularity, false)
                    .is_err()
            );
        }
    }

    /// Evaluates `date_trunc(granularity, value)` once on a scalar and once on a
    /// one-row column holding the same value.
    fn date_trunc_scalar_and_array(
        granularity: &str,
        value: ScalarValue,
    ) -> (
        datafusion_common::Result<ScalarValue>,
        datafusion_common::Result<ScalarValue>,
    ) {
        let invoke = |arg: ColumnarValue| {
            let data_type = value.data_type();
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::from(granularity)), arg],
                arg_fields: vec![
                    Field::new("a", DataType::Utf8, false).into(),
                    Field::new("b", data_type.clone(), true).into(),
                ],
                number_rows: 1,
                return_field: Field::new("f", data_type, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            match SparkDateTrunc::new().invoke_with_args(args)? {
                ColumnarValue::Scalar(result) => Ok(result),
                ColumnarValue::Array(result) => ScalarValue::try_from_array(&result, 0),
            }
        };
        (
            invoke(ColumnarValue::Scalar(value.clone())),
            invoke(ColumnarValue::Array(value.to_array().unwrap())),
        )
    }

    /// A timestamp beyond the nanosecond range is truncated the same way as a
    /// scalar and as a column: neither converts it to nanoseconds first.
    #[test]
    fn scalar_and_array_accept_timestamps_beyond_nanosecond_range() {
        // 2286-11-20T17:46:40, after the last nanosecond timestamp in 2262
        let seconds = 10_000_000_000;
        let utc: Option<Arc<str>> = Some("UTC".into());
        let cases = [
            // Seconds, truncated to a minute, an hour and a day
            (
                ScalarValue::TimestampSecond(Some(seconds + 59), None),
                "minute",
            ),
            (
                ScalarValue::TimestampSecond(Some(seconds + 1), None),
                "hour",
            ),
            (ScalarValue::TimestampSecond(Some(seconds + 1), None), "day"),
            // The granularity is the input's own unit
            (ScalarValue::TimestampSecond(Some(seconds), None), "second"),
            // With a time zone
            (
                ScalarValue::TimestampSecond(Some(seconds + 59), utc.clone()),
                "minute",
            ),
            // Milliseconds and microseconds, truncated to a coarser unit
            (
                ScalarValue::TimestampMillisecond(Some(seconds * 1_000 + 999), None),
                "second",
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(seconds * 1_000_000 + 999), None),
                "millisecond",
            ),
            // Before the epoch, with a time zone
            (
                ScalarValue::TimestampMicrosecond(Some(-seconds * 1_000_000 - 1), utc),
                "second",
            ),
        ];
        for (value, granularity) in cases {
            let (scalar, array) = date_trunc_scalar_and_array(granularity, value.clone());
            let scalar = scalar.unwrap_or_else(|e| {
                panic!("scalar date_trunc('{granularity}', {value:?}) failed: {e}")
            });
            assert_eq!(
                scalar,
                array.unwrap(),
                "date_trunc('{granularity}', {value:?})"
            );
        }

        // `date_trunc('second', to_timestamp_seconds(10000000000))` returns its input
        // rather than an out of range error.
        // See <https://github.com/apache/datafusion/issues/25432>.
        let (scalar, _) = date_trunc_scalar_and_array(
            "second",
            ScalarValue::TimestampSecond(Some(seconds), None),
        );
        assert_eq!(
            scalar.unwrap(),
            ScalarValue::TimestampSecond(Some(seconds), None)
        );
    }

    /// The scalar fast path shares the same underflow check as the array
    /// path: truncating a value within one unit of `i64::MIN` to a coarser
    /// granularity in its own unit must error rather than wrap around.
    #[test]
    fn scalar_and_array_reject_fine_granularity_underflow() {
        let (scalar, array) = date_trunc_scalar_and_array(
            "minute",
            ScalarValue::TimestampSecond(Some(i64::MIN), None),
        );
        assert!(scalar.is_err(), "expected scalar path to reject underflow");
        assert!(array.is_err(), "expected array path to reject underflow");
    }

    fn assert_fine_granularity_underflow<T: ArrowTimestampType>(
        array: PrimitiveArray<T>,
        granularity: DatePart,
    ) {
        let error = general_date_trunc_array_fine_granularity(T::UNIT, &array, granularity, None)
            .unwrap_err();
        assert!(
            error
                .strip_backtrace()
                .contains("Timestamp -9223372036854775808 out of range"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_date_trunc_fine_granularity_underflow_for_each_timestamp_unit() {
        assert_fine_granularity_underflow(
            TimestampSecondArray::from(vec![i64::MIN]),
            DatePart::Minute,
        );
        assert_fine_granularity_underflow(
            TimestampMillisecondArray::from(vec![i64::MIN]),
            DatePart::Second,
        );
        assert_fine_granularity_underflow(
            TimestampMicrosecondArray::from(vec![i64::MIN]),
            DatePart::Millisecond,
        );
        assert_fine_granularity_underflow(
            TimestampNanosecondArray::from(vec![i64::MIN]),
            DatePart::Microsecond,
        );
    }

    #[test]
    fn test_date_trunc_fine_granularity_minimum_safe_input() {
        // The boundary below i64::MIN is unrepresentable. Inputs become safe
        // at the next microsecond boundary.
        let distance_to_next_boundary =
            NANOS_PER_MICROSECOND - i64::MIN.rem_euclid(NANOS_PER_MICROSECOND);
        let minimum_safe_input = i64::MIN + distance_to_next_boundary;
        let unsafe_input = TimestampNanosecondArray::from(vec![minimum_safe_input - 1]);
        assert!(
            general_date_trunc_array_fine_granularity(
                TimeUnit::Nanosecond,
                &unsafe_input,
                DatePart::Microsecond,
                None,
            )
            .is_err()
        );

        let input = TimestampNanosecondArray::from(vec![
            Some(minimum_safe_input),
            Some(minimum_safe_input + 1),
            None,
        ]);
        let result = general_date_trunc_array_fine_granularity(
            TimeUnit::Nanosecond,
            &input,
            DatePart::Microsecond,
            None,
        )
        .unwrap();
        let result = as_primitive_array::<TimestampNanosecondType>(&result);
        let expected = TimestampNanosecondArray::from(vec![
            Some(minimum_safe_input),
            Some(minimum_safe_input),
            None,
        ]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_date_trunc_fine_granularity_ignores_null_slot_values() {
        // Physical values behind null slots are arbitrary. An unsafe value in
        // a null slot may trigger the checked fallback, but must not cause an
        // error or alter the null bitmap.
        let input = TimestampNanosecondArray::new(
            vec![i64::MIN, 1].into(),
            Some(NullBuffer::from(vec![false, true])),
        );
        let result = general_date_trunc_array_fine_granularity(
            TimeUnit::Nanosecond,
            &input,
            DatePart::Microsecond,
            None,
        )
        .unwrap();
        let result = as_primitive_array::<TimestampNanosecondType>(&result);
        let expected = TimestampNanosecondArray::from(vec![None, Some(0)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_date_trunc_fine_granularity_timezones() {
        let cases = [
            // Test "second" granularity
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:42:30.500000Z",
                    "2020-09-08T13:42:31.999999Z",
                ],
                Some("+00".into()),
                "second",
                vec![
                    "2020-09-08T13:42:29.000000Z",
                    "2020-09-08T13:42:30.000000Z",
                    "2020-09-08T13:42:31.000000Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855+05",
                    "2020-09-08T13:42:30.500000+05",
                    "2020-09-08T13:42:31.999999+05",
                ],
                Some("+05".into()),
                "second",
                vec![
                    "2020-09-08T13:42:29.000000+05",
                    "2020-09-08T13:42:30.000000+05",
                    "2020-09-08T13:42:31.000000+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:42:30.500000Z",
                    "2020-09-08T13:42:31.999999Z",
                ],
                Some("Europe/Berlin".into()),
                "second",
                vec![
                    "2020-09-08T13:42:29.000000Z",
                    "2020-09-08T13:42:30.000000Z",
                    "2020-09-08T13:42:31.000000Z",
                ],
            ),
            // Test "minute" granularity
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:43:30.500000Z",
                    "2020-09-08T13:44:31.999999Z",
                ],
                Some("+00".into()),
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000Z",
                    "2020-09-08T13:43:00.000000Z",
                    "2020-09-08T13:44:00.000000Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855+08",
                    "2020-09-08T13:43:30.500000+08",
                    "2020-09-08T13:44:31.999999+08",
                ],
                Some("+08".into()),
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000+08",
                    "2020-09-08T13:43:00.000000+08",
                    "2020-09-08T13:44:00.000000+08",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:43:30.500000Z",
                    "2020-09-08T13:44:31.999999Z",
                ],
                Some("America/Sao_Paulo".into()),
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000Z",
                    "2020-09-08T13:43:00.000000Z",
                    "2020-09-08T13:44:00.000000Z",
                ],
            ),
            // Test with None (no timezone)
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:43:30.500000Z",
                    "2020-09-08T13:44:31.999999Z",
                ],
                None,
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000Z",
                    "2020-09-08T13:43:00.000000Z",
                    "2020-09-08T13:44:00.000000Z",
                ],
            ),
            // Test millisecond granularity
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:42:29.191999Z",
                    "2020-09-08T13:42:29.192500Z",
                ],
                Some("Asia/Kolkata".into()),
                "millisecond",
                vec![
                    "2020-09-08T19:12:29.190000+05:30",
                    "2020-09-08T19:12:29.191000+05:30",
                    "2020-09-08T19:12:29.192000+05:30",
                ],
            ),
        ];

        for (original, tz_opt, granularity, expected) in &cases {
            let input = original
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let right = expected
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let batch_len = input.len();
            let arg_fields = vec![
                Field::new("a", DataType::Utf8, false).into(),
                Field::new("b", input.data_type().clone(), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::from(*granularity)),
                    ColumnarValue::Array(Arc::new(input)),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new(
                    "f",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                    true,
                )
                .into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = SparkDateTrunc::new().invoke_with_args(args).unwrap();
            if let ColumnarValue::Array(result) = result {
                assert_eq!(
                    result.data_type(),
                    &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                    "Failed for granularity: {granularity}, timezone: {tz_opt:?}"
                );
                let left = as_primitive_array::<TimestampNanosecondType>(&result);
                assert_eq!(
                    left, &right,
                    "Failed for granularity: {granularity}, timezone: {tz_opt:?}"
                );
            } else {
                panic!("unexpected column type");
            }
        }
    }
}
