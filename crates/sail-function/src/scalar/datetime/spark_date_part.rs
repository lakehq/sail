use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, Int32Array};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Field, FieldRef, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_part::DatePartFunc;
use sail_common::utils::datetime::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND, civil_from_days,
    is_fixed_zero_offset_timezone,
};
use sail_common_datafusion::utils::datetime::SparkZone;

/// Fields from Spark's `DatePart.parseExtractField` and `ExtractIntervalPart`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SparkDatePartField {
    Year,
    YearOfWeek,
    Quarter,
    Month,
    Week,
    Day,
    DayOfWeek,
    DayOfWeekIso,
    DayOfYear,
    Hour,
    Minute,
    Second,
}

impl SparkDatePartField {
    fn parse(part: &str) -> Option<Self> {
        Some(match part.to_ascii_uppercase().as_str() {
            "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => Self::Year,
            "YEAROFWEEK" => Self::YearOfWeek,
            "QUARTER" | "QTR" => Self::Quarter,
            "MONTH" | "MON" | "MONS" | "MONTHS" => Self::Month,
            "WEEK" | "W" | "WEEKS" => Self::Week,
            "DAY" | "D" | "DAYS" => Self::Day,
            "DAYOFWEEK" | "DOW" => Self::DayOfWeek,
            "DAYOFWEEK_ISO" | "DOW_ISO" => Self::DayOfWeekIso,
            "DOY" => Self::DayOfYear,
            "HOUR" | "H" | "HOURS" | "HR" | "HRS" => Self::Hour,
            "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => Self::Minute,
            "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => Self::Second,
            _ => return None,
        })
    }

    fn is_time(self) -> bool {
        matches!(self, Self::Hour | Self::Minute | Self::Second)
    }

    fn arrow_name(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::YearOfWeek => "isoyear",
            Self::Quarter => "quarter",
            Self::Month => "month",
            Self::Week => "week",
            Self::Day => "day",
            Self::DayOfWeek => "dow1",
            Self::DayOfWeekIso => "isodow",
            Self::DayOfYear => "doy",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
        }
    }

    fn data_type(self, source: &DataType) -> DataType {
        match (self, source) {
            (Self::Second, _) => DataType::Decimal128(8, 6),
            (Self::Month, DataType::Interval(IntervalUnit::YearMonth))
            | (Self::Hour | Self::Minute, DataType::Duration(TimeUnit::Microsecond)) => {
                DataType::Int8
            }
            _ => DataType::Int32,
        }
    }
}

/// Paths where every non-null input has a representable result, including values outside Chrono.
/// Other Arrow representations retain DataFusion's conservative nullability.
enum WideExtraction {
    Date,
    Timestamp { offset_seconds: i32 },
    DayTimeInterval,
    YearMonthInterval,
}

impl WideExtraction {
    fn new(part: SparkDatePartField, source: &DataType) -> Result<Option<Self>> {
        use SparkDatePartField as P;
        Ok(match source {
            DataType::Date32 if !part.is_time() => Some(Self::Date),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                match timezone.as_deref() {
                    None => Some(Self::Timestamp { offset_seconds: 0 }),
                    Some(tz) if is_fixed_zero_offset_timezone(tz) => {
                        Some(Self::Timestamp { offset_seconds: 0 })
                    }
                    Some(tz) => match SparkZone::parse(tz)? {
                        SparkZone::Fixed(offset) => Some(Self::Timestamp {
                            offset_seconds: offset.local_minus_utc(),
                        }),
                        // Named zones still need Chrono and can yield NULL for wide timestamps.
                        SparkZone::Named(_) => None,
                    },
                }
            }
            DataType::Duration(TimeUnit::Microsecond) => match part {
                P::Day | P::Hour | P::Minute | P::Second => Some(Self::DayTimeInterval),
                _ => return exec_err!("Cannot extract {} from {source}", part.arrow_name()),
            },
            DataType::Interval(IntervalUnit::YearMonth) => match part {
                P::Year | P::Month => Some(Self::YearMonthInterval),
                _ => return exec_err!("Cannot extract {} from {source}", part.arrow_name()),
            },
            _ => None,
        })
    }

    fn extract(&self, part: SparkDatePartField, array: &dyn Array) -> Int32Array {
        use SparkDatePartField as P;
        match self {
            Self::Date => array
                .as_primitive::<Date32Type>()
                .unary(|days| calendar_part(part, i64::from(days))),
            Self::Timestamp { offset_seconds } => array
                .as_primitive::<TimestampMicrosecondType>()
                .unary(|micros| {
                    // Local wall-clock microseconds can exceed i64 at either instant endpoint.
                    let local = i128::from(micros)
                        + i128::from(*offset_seconds) * i128::from(MICROS_PER_SECOND);
                    let time = local.rem_euclid(i128::from(MICROS_PER_DAY)) as i64;
                    match part {
                        P::Hour => (time / MICROS_PER_HOUR) as i32,
                        P::Minute => (time % MICROS_PER_HOUR / MICROS_PER_MINUTE) as i32,
                        P::Second => (time % MICROS_PER_MINUTE) as i32,
                        _ => {
                            calendar_part(part, local.div_euclid(i128::from(MICROS_PER_DAY)) as i64)
                        }
                    }
                }),
            Self::DayTimeInterval => {
                array
                    .as_primitive::<DurationMicrosecondType>()
                    .unary(|micros| match part {
                        // Spark IntervalUtils uses signed remainders, unlike timestamp fields.
                        P::Day => (micros / MICROS_PER_DAY) as i32,
                        P::Hour => (micros % MICROS_PER_DAY / MICROS_PER_HOUR) as i32,
                        P::Minute => (micros % MICROS_PER_HOUR / MICROS_PER_MINUTE) as i32,
                        P::Second => (micros % MICROS_PER_MINUTE) as i32,
                        _ => unreachable!("validated interval part"),
                    })
            }
            Self::YearMonthInterval => {
                array
                    .as_primitive::<IntervalYearMonthType>()
                    .unary(|months| match part {
                        P::Year => months / 12,
                        P::Month => months % 12,
                        _ => unreachable!("validated interval part"),
                    })
            }
        }
    }
}

fn ordinal(year: i64, month: u32, day: u32) -> i32 {
    let preceding = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    preceding[(month - 1) as usize] + day as i32 + i32::from(leap && month > 2)
}

fn calendar_part(part: SparkDatePartField, days: i64) -> i32 {
    use SparkDatePartField as P;
    let (year, month, day) = civil_from_days(days);
    match part {
        P::Year => year as i32,
        P::Quarter => (month as i32 - 1) / 3 + 1,
        P::Month => month as i32,
        P::Day => day as i32,
        P::DayOfWeek => (days + 4).rem_euclid(7) as i32 + 1,
        P::DayOfWeekIso => (days + 3).rem_euclid(7) as i32 + 1,
        P::DayOfYear => ordinal(year, month, day),
        P::YearOfWeek | P::Week => {
            // ISO weeks belong to the year of their Thursday, including BCE and year zero.
            let thursday = days + 3 - (days + 3).rem_euclid(7);
            let (year, month, day) = civil_from_days(thursday);
            if part == P::YearOfWeek {
                year as i32
            } else {
                (ordinal(year, month, day) - 1) / 7 + 1
            }
        }
        _ => unreachable!("calendar field"),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDatePart {
    inner: DatePartFunc,
}

impl Default for SparkDatePart {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDatePart {
    pub fn new() -> Self {
        Self {
            inner: DatePartFunc::new(),
        }
    }
}

impl ScalarUDFImpl for SparkDatePart {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;
        let [_, source] = take_function_args(self.name(), args.arg_fields)?;
        if field.is_some_and(ScalarValue::is_null) {
            return Ok(Arc::new(Field::new(self.name(), DataType::Float64, true)));
        }
        if let Some(part) = field
            .and_then(|sv| sv.try_as_str().flatten())
            .and_then(SparkDatePartField::parse)
        {
            let wide = WideExtraction::new(part, source.data_type())?;
            // Spark's calendar fields implicitly cast NTZ to DATE. Cast.forceNullable marks
            // that cast nullable even though every microsecond value has a valid date.
            let ntz_calendar =
                !part.is_time() && matches!(source.data_type(), DataType::Timestamp(_, None));
            return Ok(Arc::new(Field::new(
                self.name(),
                part.data_type(source.data_type()),
                source.is_nullable() || wide.is_none() || ntz_calendar,
            )));
        }
        self.inner.return_field_from_args(args)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [field, source] = take_function_args(self.name(), args.args.as_slice())?;
        let ColumnarValue::Scalar(field) = field else {
            return exec_err!("date_part requires a constant string field");
        };
        if field.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
        }
        let Some(part) = field
            .try_as_str()
            .flatten()
            .and_then(SparkDatePartField::parse)
        else {
            return self.inner.invoke_with_args(args);
        };
        if let Some(wide) = WideExtraction::new(part, &source.data_type())? {
            let scalar = matches!(source, ColumnarValue::Scalar(_));
            let array = source.to_array(1)?;
            let array: ArrayRef = Arc::new(wide.extract(part, array.as_ref()));
            let value = if scalar {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&array, 0)?)
            } else {
                ColumnarValue::Array(array)
            };
            return if part == SparkDatePartField::Second {
                seconds_decimal(value)
            } else {
                value.cast_to(&part.data_type(&source.data_type()), None)
            };
        }

        // Keep DF's range/overflow handling for representations not covered above. Normalize
        // Spark's aliases, notably Sunday=1 and the ISO week-year, before delegating.
        let name = if part == SparkDatePartField::Second {
            "microseconds"
        } else {
            part.arrow_name()
        };
        args.args[0] = ColumnarValue::Scalar(ScalarValue::from(name));
        if part == SparkDatePartField::Second {
            args.return_field = Arc::new(Field::new(self.name(), DataType::Int32, true));
            seconds_decimal(self.inner.invoke_with_args(args)?)
        } else {
            self.inner.invoke_with_args(args)
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

fn seconds_decimal(value: ColumnarValue) -> Result<ColumnarValue> {
    let value = value.cast_to(&DataType::Decimal128(8, 0), None)?;
    let scalar = matches!(value, ColumnarValue::Scalar(_));
    let array = value.to_array(1)?;
    let array = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| exec_datafusion_err!("date_part: expected decimal seconds"))?
        .clone()
        .with_precision_and_scale(8, 6)?;
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &array, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, NaiveDate};
    use datafusion::arrow::array::{
        Date32Array, DurationMicrosecondArray, TimestampMicrosecondArray,
    };
    use datafusion_common::config::ConfigOptions;

    use super::*;

    fn field(part: &ScalarValue, source: DataType, nullable: bool) -> Result<FieldRef> {
        SparkDatePart::new().return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                Arc::new(Field::new("part", part.data_type(), part.is_null())),
                Arc::new(Field::new("source", source, nullable)),
            ],
            scalar_arguments: &[Some(part), None],
        })
    }

    fn invoke(part: &str, source: ColumnarValue) -> Result<ColumnarValue> {
        let part = ScalarValue::from(part);
        let return_field = field(&part, source.data_type(), true)?;
        SparkDatePart::new().invoke_with_args(ScalarFunctionArgs {
            arg_fields: vec![
                Arc::new(Field::new("part", part.data_type(), false)),
                Arc::new(Field::new("source", source.data_type(), true)),
            ],
            number_rows: match &source {
                ColumnarValue::Scalar(_) => 1,
                ColumnarValue::Array(a) => a.len(),
            },
            args: vec![ColumnarValue::Scalar(part), source],
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn scalar(part: &str, source: ScalarValue) -> Result<ScalarValue> {
        let ColumnarValue::Scalar(value) = invoke(part, ColumnarValue::Scalar(source))? else {
            panic!("scalar input must remain scalar")
        };
        Ok(value)
    }

    const CALENDAR_PARTS: [&str; 9] = [
        "YEAR",
        "YEAROFWEEK",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
        "DOW",
        "DOW_ISO",
        "DOY",
    ];

    #[test]
    fn date32_full_domain_and_iso_rollover() -> Result<()> {
        // Spark 4.2 extract.sql semantics, including DateTimeUtils' Java LocalDate range.
        for (days, expected) in [
            (i32::MIN, [-5877641, -5877641, 2, 6, 26, 23, 3, 2, 174]),
            (i32::MAX, [5881580, 5881580, 3, 7, 28, 11, 6, 5, 193]),
            (-141438, [1582, 1582, 4, 10, 40, 4, 2, 1, 277]),
            (-1, [1969, 1970, 4, 12, 1, 31, 4, 3, 365]),
            (0, [1970, 1970, 1, 1, 1, 1, 5, 4, 1]),
            (13149, [2006, 2005, 1, 1, 52, 1, 1, 7, 1]),
            (13150, [2006, 2006, 1, 1, 1, 2, 2, 1, 2]),
        ] {
            for (part, expected) in CALENDAR_PARTS.into_iter().zip(expected) {
                assert_eq!(
                    scalar(part, ScalarValue::Date32(Some(days)))?,
                    ScalarValue::Int32(Some(expected)),
                    "{part} at {days}"
                );
                assert!(!field(&ScalarValue::from(part), DataType::Date32, false)?.is_nullable());
            }
        }
        Ok(())
    }

    #[test]
    fn gregorian_cycle_matches_independent_calendar() {
        // A full cycle across year zero exercises BCE leap years and every ISO week boundary.
        let start = Date32Type::from_naive_date(NaiveDate::from_ymd_opt(-200, 1, 1).unwrap());
        for days in start..start + 146097 {
            let date = Date32Type::to_naive_date_opt(days).unwrap();
            let expected = [
                date.year(),
                date.iso_week().year(),
                (date.month() as i32 - 1) / 3 + 1,
                date.month() as i32,
                date.iso_week().week() as i32,
                date.day() as i32,
                date.weekday().num_days_from_sunday() as i32 + 1,
                date.weekday().number_from_monday() as i32,
                date.ordinal() as i32,
            ];
            for (part, expected) in CALENDAR_PARTS.into_iter().zip(expected) {
                assert_eq!(
                    calendar_part(SparkDatePartField::parse(part).unwrap(), i64::from(days)),
                    expected
                );
            }
        }
    }

    #[test]
    fn timestamp_microsecond_limits_and_fixed_offsets() -> Result<()> {
        // Spark DateTimeUtilsSuite SPARK-35664, extended with real Spark fixed-zone probes.
        for (zone, micros, expected) in [
            (None, i64::MIN, [-290308, 12, 21, 19, 59, 5_224_192]),
            (Some("UTC"), i64::MAX, [294247, 1, 10, 4, 0, 54_775_807]),
            (
                Some("+05:30"),
                i64::MIN,
                [-290308, 12, 22, 1, 29, 5_224_192],
            ),
            (Some("+05:30"), i64::MAX, [294247, 1, 10, 9, 30, 54_775_807]),
            (
                Some("-12:00"),
                i64::MIN,
                [-290308, 12, 21, 7, 59, 5_224_192],
            ),
            (Some("-12:00"), i64::MAX, [294247, 1, 9, 16, 0, 54_775_807]),
            (None, -1, [1969, 12, 31, 23, 59, 59_999_999]),
        ] {
            let input = ScalarValue::TimestampMicrosecond(Some(micros), zone.map(Arc::from));
            for (part, value) in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]
                .into_iter()
                .zip(expected)
            {
                let expected = if part == "SECOND" {
                    ScalarValue::Decimal128(Some(i128::from(value)), 8, 6)
                } else {
                    ScalarValue::Int32(Some(value))
                };
                assert_eq!(
                    scalar(part, input.clone())?,
                    expected,
                    "{part}: {zone:?}/{micros}"
                );
                assert_eq!(
                    field(&ScalarValue::from(part), input.data_type(), false)?.is_nullable(),
                    zone.is_none() && matches!(part, "YEAR" | "MONTH" | "DAY")
                );
            }
        }
        Ok(())
    }

    #[test]
    fn arrays_preserve_nulls_and_shape() -> Result<()> {
        let input = ColumnarValue::Array(Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(i32::MAX),
        ])));
        let result = invoke("YEAR", input)?.to_array(3)?;
        assert_eq!(
            result.as_ref(),
            &Int32Array::from(vec![Some(-5877641), None, Some(5881580)])
        );
        let input = ColumnarValue::Array(Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1),
            None,
            Some(1),
        ])));
        let result = invoke("SECOND", input)?.to_array(3)?;
        assert_eq!(
            result.as_ref(),
            &Decimal128Array::from(vec![Some(59_999_999), None, Some(1)])
                .with_precision_and_scale(8, 6)?
        );
        let input =
            ColumnarValue::Array(Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())));
        assert_eq!(invoke("SECOND", input)?.to_array(0)?.len(), 0);
        Ok(())
    }

    #[test]
    fn intervals_use_signed_components_without_overflow() -> Result<()> {
        for (micros, expected) in [
            (i64::MIN, [-106751991, -4, 0, -54_775_808]),
            (i64::MAX, [106751991, 4, 0, 54_775_807]),
            (-1, [0, 0, 0, -1]),
            (-10672496789123, [-123, -12, -34, -56_789_123]),
        ] {
            let input = ScalarValue::DurationMicrosecond(Some(micros));
            for (part, expected) in ["DAY", "HOUR", "MINUTE", "SECOND"]
                .into_iter()
                .zip(expected)
            {
                let expected = match part {
                    "SECOND" => ScalarValue::Decimal128(Some(i128::from(expected)), 8, 6),
                    "HOUR" | "MINUTE" => ScalarValue::Int8(Some(expected as i8)),
                    _ => ScalarValue::Int32(Some(expected)),
                };
                assert_eq!(scalar(part, input.clone())?, expected);
            }
        }
        for months in [i32::MIN, -25, 0, 25, i32::MAX] {
            let input = ScalarValue::IntervalYearMonth(Some(months));
            assert_eq!(
                scalar("YEAR", input.clone())?,
                ScalarValue::Int32(Some(months / 12))
            );
            assert_eq!(
                scalar("MONTH", input)?,
                ScalarValue::Int8(Some((months % 12) as i8))
            );
        }
        let input = ColumnarValue::Array(Arc::new(DurationMicrosecondArray::from(vec![
            Some(i64::MIN),
            None,
            Some(-1),
        ])));
        let result = invoke("SECOND", input)?.to_array(3)?;
        assert_eq!(
            result.as_ref(),
            &Decimal128Array::from(vec![Some(-54_775_808), None, Some(-1)])
                .with_precision_and_scale(8, 6)?
        );
        Ok(())
    }

    #[test]
    fn supported_paths_propagate_nullable_but_delegates_stay_conservative() -> Result<()> {
        for (part, source, data_type, total) in [
            ("YEAR", DataType::Date32, DataType::Int32, true),
            (
                "SECOND",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Decimal128(8, 6),
                true,
            ),
            (
                "SECOND",
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Decimal128(8, 6),
                true,
            ),
            (
                "HOUR",
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Int8,
                true,
            ),
            (
                "MONTH",
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Int8,
                true,
            ),
            ("YEAR", DataType::Date64, DataType::Int32, false),
            (
                "YEAR",
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Int32,
                false,
            ),
            (
                "SECOND",
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/Los_Angeles".into())),
                DataType::Decimal128(8, 6),
                false,
            ),
            (
                "YEAR",
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/Los_Angeles".into())),
                DataType::Int32,
                false,
            ),
            (
                "MICROSECOND",
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Int32,
                false,
            ),
        ] {
            for nullable in [false, true] {
                let result = field(&ScalarValue::from(part), source.clone(), nullable)?;
                assert_eq!(result.data_type(), &data_type);
                assert_eq!(result.is_nullable(), nullable || !total, "{part}/{source}");
            }
        }
        assert_eq!(
            field(&ScalarValue::Utf8(None), DataType::Date32, false)?.data_type(),
            &DataType::Float64
        );
        Ok(())
    }

    #[test]
    fn ntz_calendar_nullability_matches_spark_without_changing_other_fields() -> Result<()> {
        // Spark Cast.forceNullable(NTZ, DATE) is true; LTZ-to-DATE is false. Time fields do
        // not make this implicit cast. This schema distinction does not alter wide values.
        for nullable in [false, true] {
            for part in CALENDAR_PARTS {
                let result = field(&ScalarValue::from(part), DataType::Date32, nullable)?;
                assert_eq!(result.data_type(), &DataType::Int32);
                assert_eq!(result.is_nullable(), nullable, "DATE/{part}");
            }
            for zone in [None, Some("UTC"), Some("+05:30")] {
                let source = DataType::Timestamp(TimeUnit::Microsecond, zone.map(Arc::from));
                for part in CALENDAR_PARTS {
                    let result = field(&ScalarValue::from(part), source.clone(), nullable)?;
                    assert_eq!(result.data_type(), &DataType::Int32);
                    assert_eq!(
                        result.is_nullable(),
                        nullable || zone.is_none(),
                        "{source}/{part}"
                    );
                }
                for part in ["HOUR", "MINUTE", "SECOND"] {
                    let result = field(&ScalarValue::from(part), source.clone(), nullable)?;
                    let expected_type = if part == "SECOND" {
                        DataType::Decimal128(8, 6)
                    } else {
                        DataType::Int32
                    };
                    assert_eq!(result.data_type(), &expected_type);
                    assert_eq!(result.is_nullable(), nullable, "{source}/{part}");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn named_zones_delegate_dst_and_historical_offset_seconds() -> Result<()> {
        for (zone, micros, hour, seconds) in [
            ("America/Los_Angeles", -1, 15, 59_999_999),
            ("America/Los_Angeles", 1710064799999999, 1, 59_999_999),
            ("America/Los_Angeles", 1710064800000000, 3, 0),
            ("America/Los_Angeles", 1730624399999999, 1, 59_999_999),
            ("America/Los_Angeles", 1730624400000000, 1, 0),
            ("Africa/Monrovia", -1, 23, 29_999_999),
            ("Asia/Kathmandu", -2208988800000000, 5, 16_000_000),
        ] {
            let input = ScalarValue::TimestampMicrosecond(Some(micros), Some(zone.into()));
            assert_eq!(
                scalar("HOUR", input.clone())?,
                ScalarValue::Int32(Some(hour))
            );
            assert_eq!(
                scalar("SECOND", input)?,
                ScalarValue::Decimal128(Some(seconds), 8, 6)
            );
        }
        // A delegated path really can add NULLs: never describe it as input-null-only.
        let input =
            ScalarValue::TimestampMicrosecond(Some(i64::MAX), Some("America/Los_Angeles".into()));
        assert_eq!(scalar("YEAR", input.clone())?, ScalarValue::Int32(None));
        assert_eq!(
            scalar("SECOND", input)?,
            ScalarValue::Decimal128(None, 8, 6)
        );
        Ok(())
    }

    #[test]
    fn spark_aliases_and_errors() -> Result<()> {
        for (aliases, expected) in [
            ("YEAR Y YEARS YR YRS", 1969),
            ("YEAROFWEEK", 1970),
            ("QUARTER QTR", 4),
            ("MONTH MON MONS MONTHS", 12),
            ("WEEK W WEEKS", 1),
            ("DAY D DAYS", 31),
            ("DAYOFWEEK DOW", 4),
            ("DAYOFWEEK_ISO DOW_ISO", 3),
            ("DOY", 365),
            ("HOUR H HOURS HR HRS", 23),
            ("MINUTE M MIN MINS MINUTES", 59),
            ("SECOND S SEC SECONDS SECS", 59_999_999),
        ] {
            for part in aliases.split_whitespace() {
                let expected =
                    if SparkDatePartField::parse(part) == Some(SparkDatePartField::Second) {
                        ScalarValue::Decimal128(Some(i128::from(expected)), 8, 6)
                    } else {
                        ScalarValue::Int32(Some(expected))
                    };
                assert_eq!(
                    scalar(
                        &part.to_lowercase(),
                        ScalarValue::TimestampMicrosecond(Some(-1), None)
                    )?,
                    expected
                );
            }
        }
        assert!(scalar("not_supported", ScalarValue::Date32(Some(0))).is_err());
        assert!(scalar("", ScalarValue::Date32(Some(0))).is_err());
        assert!(scalar("YEAR", ScalarValue::DurationMicrosecond(Some(0))).is_err());
        assert!(scalar("DAY", ScalarValue::IntervalYearMonth(Some(0))).is_err());
        assert!(
            scalar(
                "YEAR",
                ScalarValue::TimestampMicrosecond(Some(0), Some("Invalid/Zone".into()))
            )
            .is_err()
        );
        Ok(())
    }
}
