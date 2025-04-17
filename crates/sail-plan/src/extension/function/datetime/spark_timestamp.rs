use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;

use chrono::format::{parse_and_remainder, Fixed, Item, Numeric, Pad, Parsed};
use chrono::{
    DateTime, Days, FixedOffset, MappedLocalTime, NaiveDateTime, ParseError, TimeZone, Utc,
};
use chrono_tz::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkTimestamp {
    timezone: Arc<str>,
    signature: Signature,
}

impl SparkTimestamp {
    pub fn new(timezone: Arc<str>) -> Self {
        Self {
            timezone,
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }

    fn string_to_timestamp_microseconds<T: AsRef<str>>(&self, value: T) -> Result<i64> {
        let timestamp = parse_timestamp(value.as_ref())?;
        let datetime = match timestamp {
            TimestampValue::WithTimeZone(x) => x,
            TimestampValue::WithoutTimeZone(x) => {
                // FIXME: avoid expensive time zone parsing
                let tz = parse_timezone(&self.timezone)?;
                tz.localize_with_fallback(&x)?
            }
        };
        Ok(datetime.timestamp_micros())
    }
}

impl ScalarUDFImpl for SparkTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(&self.timezone)),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        match arg {
            ColumnarValue::Array(array) => {
                let array: PrimitiveArray<TimestampMicrosecondType> = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|x| self.string_to_timestamp_microseconds(x))
                                .transpose()
                        })
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|x| self.string_to_timestamp_microseconds(x))
                                .transpose()
                        })
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|x| self.string_to_timestamp_microseconds(x))
                                .transpose()
                        })
                        .collect::<Result<_>>()?,
                    _ => return exec_err!("expected string array for `timestamp`"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|x| self.string_to_timestamp_microseconds(x))
                        .transpose()?,
                    _ => {
                        return exec_err!("expected string scalar for `timestamp`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    value,
                    Some(Arc::from("UTC")),
                )))
            }
        }
    }
}

pub enum TimestampValue {
    WithTimeZone(DateTime<Utc>),
    WithoutTimeZone(NaiveDateTime),
}

pub enum TimeZoneValue {
    Fixed(FixedOffset),
    Tz(Tz),
}

impl TimeZoneValue {
    pub fn localize(&self, datetime: &NaiveDateTime) -> MappedLocalTime<DateTime<Utc>> {
        match self {
            TimeZoneValue::Fixed(offset) => {
                offset.from_local_datetime(datetime).map(|x| x.to_utc())
            }
            TimeZoneValue::Tz(tz) => tz.from_local_datetime(datetime).map(|x| x.to_utc()),
        }
    }

    pub fn localize_with_fallback(&self, datetime: &NaiveDateTime) -> Result<DateTime<Utc>> {
        match self.localize(datetime).earliest() {
            Some(x) => Ok(x),
            None => datetime
                .checked_sub_days(Days::new(1))
                .and_then(|x| self.localize(&x).earliest())
                .and_then(|x| x.checked_add_days(Days::new(1)))
                .ok_or_else(|| exec_datafusion_err!("cannot localize datetime: {datetime}")),
        }
    }
}

pub fn parse_timezone(s: &str) -> Result<TimeZoneValue> {
    if let Ok(offset) = FixedOffset::from_str(s) {
        Ok(TimeZoneValue::Fixed(offset))
    } else {
        let tz = Tz::from_str(s).map_err(|_| exec_datafusion_err!("invalid time zone: {s}"))?;
        Ok(TimeZoneValue::Tz(tz))
    }
}

pub fn parse_timestamp(s: &str) -> Result<TimestampValue> {
    const DATE_ITEMS: &[Item<'static>] = &[
        Item::Numeric(Numeric::Year, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Month, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Day, Pad::Zero),
    ];
    const TIME_ITEMS: &[Item<'static>] = &[
        Item::Numeric(Numeric::Hour, Pad::Zero),
        Item::Space(""),
        Item::Literal(":"),
        Item::Numeric(Numeric::Minute, Pad::Zero),
        Item::Space(""),
        Item::Literal(":"),
        Item::Numeric(Numeric::Second, Pad::Zero),
        Item::Fixed(Fixed::Nanosecond),
        Item::Space(""),
    ];

    let error = |e: ParseError| exec_datafusion_err!("invalid timestamp: {e}: {s}");
    let mut parsed = Parsed::new();
    let suffix = parse_and_remainder(&mut parsed, s, DATE_ITEMS.iter()).map_err(error)?;
    let suffix = match suffix.as_bytes().first() {
        Some(b' ' | b'T' | b't') => &suffix[1..],
        Some(_) => return exec_err!("invalid time part in timestamp: {s}"),
        None => return exec_err!("missing time part in timestamp: {s}"),
    };
    let suffix = parse_and_remainder(&mut parsed, suffix, TIME_ITEMS.iter()).map_err(error)?;
    let tz = if suffix.is_empty() {
        None
    } else {
        Some(parse_timezone(suffix)?)
    };
    if let Some(tz) = tz {
        let datetime = parsed.to_naive_datetime_with_offset(0).map_err(error)?;
        let datetime = tz.localize_with_fallback(&datetime)?;
        Ok(TimestampValue::WithTimeZone(datetime))
    } else {
        let datetime = parsed.to_naive_datetime_with_offset(0).map_err(error)?;
        Ok(TimestampValue::WithoutTimeZone(datetime))
    }
}
