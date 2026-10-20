use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write};

use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion_common::{DataFusionError, Result};
use sail_common::spec::{
    self, IntervalFieldType, SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata,
};

const MICROS_PER_SECOND: i128 = 1_000_000;
const MICROS_PER_MINUTE: i128 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i128 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i128 = 24 * MICROS_PER_HOUR;

/// The leading and trailing fields of a Spark interval type.
///
/// Arrow interval types have no notion of these fields, so Sail carries them in the field
/// metadata under [`SAIL_SPARK_INTERVAL_METADATA_KEY`]. A field without the metadata has
/// Spark's default range, which is also what Spark assumes when it reads an Arrow interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IntervalRange {
    pub start: IntervalFieldType,
    pub end: IntervalFieldType,
}

impl IntervalRange {
    /// `DayTimeIntervalType.DEFAULT`
    pub const DAY_TIME: Self = Self::new(IntervalFieldType::Day, IntervalFieldType::Second);
    /// `YearMonthIntervalType.DEFAULT`
    pub const YEAR_MONTH: Self = Self::new(IntervalFieldType::Year, IntervalFieldType::Month);

    pub const fn new(start: IntervalFieldType, end: IntervalFieldType) -> Self {
        Self { start, end }
    }

    pub const fn single(field: IntervalFieldType) -> Self {
        Self::new(field, field)
    }

    /// The default range of the Spark interval type that the Arrow type stands for.
    pub fn default_for(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Duration(TimeUnit::Microsecond)
            | DataType::Interval(IntervalUnit::DayTime) => Some(Self::DAY_TIME),
            DataType::Interval(IntervalUnit::YearMonth) => Some(Self::YEAR_MONTH),
            _ => None,
        }
    }

    /// The range of a declared Spark interval type, or `None` for a calendar interval.
    /// A leading field alone is a single-field range, and a trailing field without a leading one
    /// is ignored in favor of the default range, as Spark does.
    pub fn from_declared(
        unit: spec::IntervalUnit,
        start: Option<IntervalFieldType>,
        end: Option<IntervalFieldType>,
    ) -> Option<Self> {
        let default = match unit {
            spec::IntervalUnit::YearMonth => Self::YEAR_MONTH,
            spec::IntervalUnit::DayTime => Self::DAY_TIME,
            spec::IntervalUnit::MonthDayNano => return None,
        };
        let declared = match (start, end) {
            (Some(start), Some(end)) => Self::new(start, end),
            (Some(start), None) => Self::single(start),
            (None, _) => default,
        };
        Some(if declared.is_valid_for(default) {
            declared
        } else {
            default
        })
    }

    /// The microseconds in one unit of the trailing field of a day-time range.
    pub fn end_unit_micros(self) -> i64 {
        match self.end {
            IntervalFieldType::Day => 86_400_000_000,
            IntervalFieldType::Hour => 3_600_000_000,
            IntervalFieldType::Minute => 60_000_000,
            _ => 1_000_000,
        }
    }

    /// The range of an interval field, or `None` if the field is not a Spark interval.
    pub fn from_field(field: &Field) -> Option<Self> {
        Self::from_metadata(field.data_type(), field.metadata())
    }

    /// The range declared by the field metadata for a value of the given type, or `None` if the
    /// type is not a Spark interval. Metadata of the wrong family is ignored.
    pub fn from_metadata(data_type: &DataType, metadata: &HashMap<String, String>) -> Option<Self> {
        let default = Self::default_for(data_type)?;
        let declared = metadata
            .get(SAIL_SPARK_INTERVAL_METADATA_KEY)
            .and_then(|x| SparkIntervalMetadata::from_json(x).ok())
            .map(|x| Self::new(x.start_field(), x.end_field()))
            .filter(|x| x.is_valid_for(default));
        Some(declared.unwrap_or(default))
    }

    fn is_valid_for(&self, default: Self) -> bool {
        self.start <= self.end && default.start <= self.start && self.end <= default.end
    }

    /// The default range of the family this range belongs to, which is what Spark declares for a
    /// result that does not keep the range of its input.
    pub fn family_default(self) -> Self {
        match self.start {
            IntervalFieldType::Year | IntervalFieldType::Month => Self::YEAR_MONTH,
            _ => Self::DAY_TIME,
        }
    }

    /// The range of the result of adding or subtracting two intervals of the same family
    /// (`TypeCoercion.scala`, `findTightestCommonType`).
    pub fn widen(self, other: Self) -> Self {
        Self::new(self.start.min(other.start), self.end.max(other.end))
    }

    /// The interval family the range belongs to, which the metadata is tagged with.
    fn interval_unit(self) -> spec::IntervalUnit {
        match self.start {
            IntervalFieldType::Year | IntervalFieldType::Month => spec::IntervalUnit::YearMonth,
            _ => spec::IntervalUnit::DayTime,
        }
    }

    pub fn to_metadata(self) -> Result<HashMap<String, String>> {
        let internal = |e: sail_common::error::CommonError| {
            DataFusionError::Internal(format!("interval metadata: {e}"))
        };
        let value =
            SparkIntervalMetadata::try_new(self.interval_unit(), Some(self.start), Some(self.end))
                .map_err(internal)?
                .ok_or_else(|| {
                    DataFusionError::Internal("interval metadata: no range".to_string())
                })?
                .to_json()
                .map_err(internal)?;
        Ok(HashMap::from([(
            SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
            value,
        )]))
    }

    /// The Spark type name, e.g. `interval day` or `interval day to second`.
    pub fn type_name(self) -> String {
        if self.start == self.end {
            format!("interval {}", field_name(self.start))
        } else {
            format!(
                "interval {} to {}",
                field_name(self.start),
                field_name(self.end)
            )
        }
    }

    fn qualifier(self) -> String {
        self.type_name()["interval ".len()..].to_uppercase()
    }
}

fn field_name(field: IntervalFieldType) -> &'static str {
    match field {
        IntervalFieldType::Year => "year",
        IntervalFieldType::Month => "month",
        IntervalFieldType::Day => "day",
        IntervalFieldType::Hour => "hour",
        IntervalFieldType::Minute => "minute",
        IntervalFieldType::Second => "second",
    }
}

/// Seconds as Spark writes them: `BigDecimal.valueOf(micros, 6).stripTrailingZeros.toPlainString`,
/// with a leading zero below ten seconds.
fn write_seconds(f: &mut Formatter<'_>, micros: i128) -> std::fmt::Result {
    if micros < 10 * MICROS_PER_SECOND {
        f.write_char('0')?;
    }
    write!(f, "{}", micros / MICROS_PER_SECOND)?;
    let fraction = micros % MICROS_PER_SECOND;
    if fraction > 0 {
        let fraction = format!("{fraction:06}");
        write!(f, ".{}", fraction.trim_end_matches('0'))?;
    }
    Ok(())
}

/// A day-time interval written with the fields of its range
/// (`SparkIntervalUtils.toDayTimeIntervalString`, ANSI style).
pub struct DayTimeIntervalFormatter(pub i64, pub IntervalRange);

impl Display for DayTimeIntervalFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(micros, range) = self;
        let (start, end) = (range.start, range.end);
        // `i128` so that the magnitude of `i64::MIN` is exact.
        let mut rest = i128::from(*micros).abs();
        f.write_str("INTERVAL '")?;
        if *micros < 0 {
            f.write_char('-')?;
        }
        match start {
            IntervalFieldType::Day => {
                write!(f, "{}", rest / MICROS_PER_DAY)?;
                rest %= MICROS_PER_DAY;
            }
            IntervalFieldType::Hour => {
                write!(f, "{:02}", rest / MICROS_PER_HOUR)?;
                rest %= MICROS_PER_HOUR;
            }
            IntervalFieldType::Minute => {
                write!(f, "{:02}", rest / MICROS_PER_MINUTE)?;
                rest %= MICROS_PER_MINUTE;
            }
            _ => write_seconds(f, rest)?,
        }
        if start < IntervalFieldType::Hour && IntervalFieldType::Hour <= end {
            write!(f, " {:02}", rest / MICROS_PER_HOUR)?;
            rest %= MICROS_PER_HOUR;
        }
        if start < IntervalFieldType::Minute && IntervalFieldType::Minute <= end {
            write!(f, ":{:02}", rest / MICROS_PER_MINUTE)?;
            rest %= MICROS_PER_MINUTE;
        }
        if start < IntervalFieldType::Second && IntervalFieldType::Second <= end {
            f.write_char(':')?;
            write_seconds(f, rest)?;
        }
        write!(f, "' {}", range.qualifier())
    }
}

/// A year-month interval written with the fields of its range
/// (`SparkIntervalUtils.toYearMonthIntervalString`, ANSI style).
pub struct YearMonthIntervalFormatter(pub i32, pub IntervalRange);

impl Display for YearMonthIntervalFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(months, range) = self;
        let sign = if *months < 0 { "-" } else { "" };
        let abs = i64::from(*months).abs();
        let year = format!("{sign}{}", abs / 12);
        match (range.start, range.end) {
            (IntervalFieldType::Year, IntervalFieldType::Year) => {
                write!(f, "INTERVAL '{year}' YEAR")
            }
            (IntervalFieldType::Month, IntervalFieldType::Month) => {
                write!(f, "INTERVAL '{months}' MONTH")
            }
            _ => write!(f, "INTERVAL '{year}-{}' YEAR TO MONTH", abs % 12),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day_time(micros: i64, start: IntervalFieldType, end: IntervalFieldType) -> String {
        DayTimeIntervalFormatter(micros, IntervalRange::new(start, end)).to_string()
    }

    #[test]
    fn day_time_interval_is_written_with_its_fields() {
        use IntervalFieldType::*;
        let micros = |d: i64, h: i64, m: i64, s: i64, us: i64| {
            (((d * 24 + h) * 60 + m) * 60 + s) * 1_000_000 + us
        };
        assert_eq!(
            day_time(micros(16, 0, 0, 0, 0), Day, Day),
            "INTERVAL '16' DAY"
        );
        assert_eq!(
            day_time(micros(15, 1, 0, 0, 0), Day, Hour),
            "INTERVAL '15 01' DAY TO HOUR"
        );
        assert_eq!(
            day_time(-micros(12, 23, 0, 0, 0), Day, Hour),
            "INTERVAL '-12 23' DAY TO HOUR"
        );
        assert_eq!(
            day_time(-micros(0, 0, 0, 1, 249_999), Hour, Second),
            "INTERVAL '-00:00:01.249999' HOUR TO SECOND"
        );
        assert_eq!(
            day_time(micros(3, 6, 9, 13, 500_000), Day, Second),
            "INTERVAL '3 06:09:13.5' DAY TO SECOND"
        );
        assert_eq!(
            day_time(-1, Day, Second),
            "INTERVAL '-0 00:00:00.000001' DAY TO SECOND"
        );
        assert_eq!(day_time(0, Second, Second), "INTERVAL '00' SECOND");
        assert_eq!(
            day_time(i64::MIN, Day, Second),
            "INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND"
        );
    }

    #[test]
    fn year_month_interval_is_written_with_its_fields() {
        use IntervalFieldType::*;
        let ym = |m: i32, start, end| {
            YearMonthIntervalFormatter(m, IntervalRange::new(start, end)).to_string()
        };
        assert_eq!(ym(36, Year, Year), "INTERVAL '3' YEAR");
        assert_eq!(ym(-3, Month, Month), "INTERVAL '-3' MONTH");
        assert_eq!(ym(-13, Year, Month), "INTERVAL '-1-1' YEAR TO MONTH");
    }

    #[test]
    fn range_is_read_from_the_field_metadata() -> Result<()> {
        use IntervalFieldType::*;
        let duration = DataType::Duration(TimeUnit::Microsecond);
        let field = Field::new("x", duration.clone(), true)
            .with_metadata(IntervalRange::single(Day).to_metadata()?);
        assert_eq!(
            IntervalRange::from_field(&field),
            Some(IntervalRange::single(Day))
        );
        assert_eq!(
            IntervalRange::from_field(&Field::new("x", duration, true)),
            Some(IntervalRange::DAY_TIME)
        );
        let wrong_family = Field::new("x", DataType::Interval(IntervalUnit::YearMonth), true)
            .with_metadata(IntervalRange::single(Day).to_metadata()?);
        assert_eq!(
            IntervalRange::from_field(&wrong_family),
            Some(IntervalRange::YEAR_MONTH)
        );
        assert_eq!(
            IntervalRange::from_field(&Field::new("x", DataType::Int64, true)),
            None
        );
        Ok(())
    }
}
