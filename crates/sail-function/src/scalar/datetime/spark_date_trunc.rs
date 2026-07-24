use std::sync::Arc;

use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike,
    Utc,
};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, TimeUnit, TimestampMicrosecondType};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_trunc::DateTruncFunc;
use sail_common_datafusion::utils::datetime::localize_with_fallback;

use crate::error::invalid_arg_count_exec_err;

/// DataFusion's `date_trunc`, with Spark's nullability.
///
/// The unit is resolved in the plan builder, which enumerates the units and calls this function
/// with a literal one per branch (see `truncate_by_unit`), so an unrecognized, NULL, or columnar
/// unit never reaches the inner function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    inner: DateTruncFunc,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            inner: DateTruncFunc::new(),
        }
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// `DateTruncFunc::return_type` always fails -- it defers to `return_field_from_args` -- so
    /// declaring the type here is what makes this function usable through the plain `return_type`
    /// path. The result carries the value's own timestamp type, and a NULL value truncates to a
    /// timestamp rather than to NULL.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(value) = arg_types.get(1) else {
            return Err(invalid_arg_count_exec_err(
                "date_trunc",
                (2, 2),
                arg_types.len(),
            ));
        };
        if value.is_null() {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        } else {
            Ok(value.clone())
        }
    }

    /// Spark truncates a NULL timestamp to NULL, so the result is nullable even when the input is
    /// not, which is what DataFusion would infer.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    /// Spark truncates entirely in microseconds ([`DateTimeUtils.truncTimestamp`]), while
    /// DataFusion converts the value to nanoseconds first and fails for anything past the year
    /// 2262 -- a range Spark's own timestamp type covers. Truncate microsecond timestamps here
    /// and leave every other input type to the inner function.
    ///
    /// [`DateTimeUtils.truncTimestamp`]: https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala#L533-L553
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "date_trunc",
                (2, 2),
                args.args.len(),
            ));
        }
        // Decide before touching the arguments, so nothing is still borrowed from `args` if the
        // inner function has to take them.
        let microsecond_timestamp = match args.args[1].data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => Some(timezone),
            _ => None,
        };
        let granularity = scalar_granularity(&args.args[0])
            .as_deref()
            .and_then(Granularity::parse);
        let Some((granularity, timezone)) = granularity.zip(microsecond_timestamp) else {
            return self.inner.invoke_with_args(args);
        };
        let data_type = args.args[1].data_type();
        let timezone = parse_timezone(timezone.as_deref())?;
        match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, tz)) => {
                let truncated = value
                    .map(|value| granularity.truncate(value, &timezone))
                    .transpose()?;
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    truncated,
                    tz.clone(),
                )))
            }
            ColumnarValue::Array(array) => {
                // Iterate rather than `try_unary`, which runs the closure over null slots too:
                // truncating rejects a value outside chrono's range, and a null slot may hold any
                // payload at all, so a garbage byte pattern under a null bit would fail the batch.
                let array: PrimitiveArray<TimestampMicrosecondType> = array
                    .as_primitive::<TimestampMicrosecondType>()
                    .iter()
                    .map(|value| {
                        value
                            .map(|value| granularity.truncate(value, &timezone))
                            .transpose()
                    })
                    .collect::<Result<_>>()?;
                Ok(ColumnarValue::Array(Arc::new(
                    array.with_data_type(data_type),
                )))
            }
            other => Err(exec_datafusion_err!(
                "`date_trunc` expected a microsecond timestamp, got {:?}",
                other.data_type()
            )),
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(input)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

const MICROS_PER_MILLI: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// The granularities Spark truncates a timestamp to. A unit and its aliases are resolved by
/// `SparkTruncLevel`, and the plan builder dispatches on the result, so only the canonical names
/// reach this function -- always as a literal, one per branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Granularity {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Granularity {
    fn parse(granularity: &str) -> Option<Self> {
        match granularity.to_uppercase().as_str() {
            "MICROSECOND" => Some(Self::Microsecond),
            "MILLISECOND" => Some(Self::Millisecond),
            "SECOND" => Some(Self::Second),
            "MINUTE" => Some(Self::Minute),
            "HOUR" => Some(Self::Hour),
            "DAY" => Some(Self::Day),
            "WEEK" => Some(Self::Week),
            "MONTH" => Some(Self::Month),
            "QUARTER" => Some(Self::Quarter),
            "YEAR" => Some(Self::Year),
            _ => None,
        }
    }

    /// Time zone offsets are whole seconds, so microsecond, millisecond and second truncate
    /// without consulting the zone at all -- which is why Spark does them first.
    fn truncate(self, micros: i64, timezone: &Tz) -> Result<i64> {
        match self {
            Self::Microsecond => Ok(micros),
            Self::Millisecond => Ok(micros - micros.rem_euclid(MICROS_PER_MILLI)),
            Self::Second => Ok(micros - micros.rem_euclid(MICROS_PER_SECOND)),
            Self::Minute => truncate_to_unit(micros, timezone, MICROS_PER_MINUTE),
            Self::Hour => truncate_to_unit(micros, timezone, MICROS_PER_HOUR),
            Self::Day => truncate_to_unit(micros, timezone, MICROS_PER_DAY),
            Self::Week | Self::Month | Self::Quarter | Self::Year => {
                let date = local_date(micros, timezone)?;
                days_to_micros(self.truncate_date(date)?, timezone)
            }
        }
    }

    fn truncate_date(self, date: NaiveDate) -> Result<NaiveDate> {
        let truncated = match self {
            // Spark's week starts on Monday.
            Self::Week => date
                .checked_sub_signed(Duration::days(date.weekday().num_days_from_monday() as i64)),
            Self::Month => date.with_day(1),
            Self::Quarter => date
                .with_day(1)
                .and_then(|date| date.with_month((date.month0() / 3) * 3 + 1)),
            Self::Year => date.with_day(1).and_then(|date| date.with_month(1)),
            _ => Some(date),
        }
        .ok_or_else(|| exec_datafusion_err!("cannot truncate date {date} to {self:?}"))?;
        Ok(truncated)
    }
}

/// The offset in microseconds that the time zone applies at the given instant.
fn offset_micros(micros: i64, timezone: &Tz) -> Result<i64> {
    let seconds = micros.div_euclid(MICROS_PER_SECOND);
    let instant = DateTime::from_timestamp(seconds, 0)
        .ok_or_else(|| exec_datafusion_err!("timestamp out of range: {micros}"))?;
    let offset = timezone
        .offset_from_utc_datetime(&instant.naive_utc())
        .fix()
        .local_minus_utc() as i64;
    Ok(offset * MICROS_PER_SECOND)
}

/// Truncate to a unit that divides the local day, by offset arithmetic rather than by building a
/// zoned datetime per row. Mirrors Spark's `truncToUnitFast`: when truncating moves the value
/// across an offset transition the arithmetic no longer holds, and the local wall clock decides.
fn truncate_to_unit(micros: i64, timezone: &Tz, unit_micros: i64) -> Result<i64> {
    let offset = offset_micros(micros, timezone)?;
    let local = micros
        .checked_add(offset)
        .ok_or_else(|| exec_datafusion_err!("timestamp out of range: {micros}"))?;
    let truncated = local - local.rem_euclid(unit_micros);
    let candidate = truncated
        .checked_sub(offset)
        .ok_or_else(|| exec_datafusion_err!("timestamp out of range: {micros}"))?;
    if offset_micros(candidate, timezone)? == offset {
        return Ok(candidate);
    }
    let local = local_datetime(micros, timezone)?;
    let time = local.time().num_seconds_from_midnight() as i64 * MICROS_PER_SECOND
        + local.time().nanosecond() as i64 / 1_000;
    let truncated = local.date().and_time(NaiveTime::MIN)
        + Duration::microseconds(time - time.rem_euclid(unit_micros));
    Ok(instant_micros(&localize_with_fallback(
        timezone, &truncated,
    )?))
}

fn local_datetime(micros: i64, timezone: &Tz) -> Result<NaiveDateTime> {
    let offset = offset_micros(micros, timezone)?;
    let local = micros
        .checked_add(offset)
        .ok_or_else(|| exec_datafusion_err!("timestamp out of range: {micros}"))?;
    DateTime::from_timestamp_micros(local)
        .map(|datetime| datetime.naive_utc())
        .ok_or_else(|| exec_datafusion_err!("timestamp out of range: {micros}"))
}

fn local_date(micros: i64, timezone: &Tz) -> Result<NaiveDate> {
    Ok(local_datetime(micros, timezone)?.date())
}

/// Midnight of the local date, as microseconds since the epoch. A local midnight that a DST
/// transition skips does not exist, and Spark's `atStartOfDay` moves it forward to the first
/// valid instant, which is what the fallback does.
fn days_to_micros(date: NaiveDate, timezone: &Tz) -> Result<i64> {
    let midnight = date.and_time(NaiveTime::MIN);
    Ok(instant_micros(&localize_with_fallback(
        timezone, &midnight,
    )?))
}

fn instant_micros(instant: &DateTime<Utc>) -> i64 {
    instant.timestamp_micros()
}

/// The granularity only ever reaches this function as a literal, because the plan builder
/// enumerates the units and calls it once per branch.
fn scalar_granularity(granularity: &ColumnarValue) -> Option<String> {
    match granularity {
        ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(granularity))
            | ScalarValue::LargeUtf8(Some(granularity))
            | ScalarValue::Utf8View(Some(granularity)),
        ) => Some(granularity.clone()),
        _ => None,
    }
}

fn parse_timezone(timezone: Option<&str>) -> Result<Tz> {
    timezone
        .unwrap_or("UTC")
        .parse()
        .map_err(|_| exec_datafusion_err!("invalid timezone: {timezone:?}"))
}
