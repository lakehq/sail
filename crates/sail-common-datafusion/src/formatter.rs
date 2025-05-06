use std::fmt::{Display, Formatter, Write};

use chrono::format::{Item, Numeric, Pad};
use chrono::{DateTime, TimeZone, Timelike, Utc};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
use datafusion::arrow::temporal_conversions::{
    time32ms_to_time, time32s_to_time, time64ns_to_time, time64us_to_time,
};
use datafusion_common::arrow::temporal_conversions::{date32_to_datetime, date64_to_datetime};

pub struct BinaryFormatter<'a>(pub &'a [u8]);

impl Display for BinaryFormatter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(value) = self;
        write!(f, "[")?;
        for (i, byte) in value.iter().enumerate() {
            if i > 0 {
                write!(f, " ")?;
            }
            write!(f, "{byte:02X}")?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

/// A formatter for number fractions where trailing zeros are not shown.
/// The decimal point `.` is not shown when the fraction value is zero.
/// The fraction value must have no greater than `N` decimal digits.
struct FractionFormatter<const N: usize>(u32);

impl<const N: usize> Display for FractionFormatter<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(value) = self;
        if *value > 0 {
            let value = value.to_string();
            let prefix = value.trim_end_matches('0');
            let padding = N.saturating_sub(value.len());
            f.write_char('.')?;
            for _ in 0..padding {
                f.write_char('0')?;
            }
            f.write_str(prefix)
        } else {
            Ok(())
        }
    }
}

const DATETIME_FORMAT_ITEMS: &[Item<'static>] = &[
    Item::Numeric(Numeric::Year, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Month, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Day, Pad::Zero),
    Item::Literal(" "),
    Item::Numeric(Numeric::Hour, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Minute, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Second, Pad::Zero),
];
const DATE_FORMAT_ITEMS: &[Item<'static>] = &[
    Item::Numeric(Numeric::Year, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Month, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Day, Pad::Zero),
];
const TIME_FORMAT_ITEMS: &[Item<'static>] = &[
    Item::Numeric(Numeric::Hour, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Minute, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Second, Pad::Zero),
];

fn write_timestamp(
    f: &mut Formatter<'_>,
    datetime: &DateTime<Utc>,
    tz: Option<&Tz>,
) -> std::fmt::Result {
    let datetime = if let Some(tz) = tz {
        datetime.with_timezone(tz).naive_local()
    } else {
        datetime.naive_utc()
    };
    let value = datetime.format_with_items(DATETIME_FORMAT_ITEMS.iter());
    let fraction = FractionFormatter::<9>(datetime.nanosecond());
    write!(f, "{value}{fraction}")
}

pub struct TimestampSecondFormatter<'a>(pub i64, pub Option<&'a Tz>);

impl Display for TimestampSecondFormatter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(seconds, tz) = self;
        match Utc.timestamp_opt(*seconds, 0).earliest() {
            Some(datetime) => write_timestamp(f, &datetime, *tz),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct TimestampMillisecondFormatter<'a>(pub i64, pub Option<&'a Tz>);

impl Display for TimestampMillisecondFormatter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(milliseconds, tz) = self;
        match Utc.timestamp_millis_opt(*milliseconds).earliest() {
            Some(datetime) => write_timestamp(f, &datetime, *tz),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct TimestampMicrosecondFormatter<'a>(pub i64, pub Option<&'a Tz>);

impl Display for TimestampMicrosecondFormatter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(microseconds, tz) = self;
        match Utc.timestamp_micros(*microseconds).earliest() {
            Some(datetime) => write_timestamp(f, &datetime, *tz),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct TimestampNanosecondFormatter<'a>(pub i64, pub Option<&'a Tz>);

impl Display for TimestampNanosecondFormatter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(nanoseconds, tz) = self;
        let datetime = Utc.timestamp_nanos(*nanoseconds);
        write_timestamp(f, &datetime, *tz)
    }
}

pub struct Date32Formatter(pub i32);

impl Display for Date32Formatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match date32_to_datetime(self.0) {
            Some(datetime) => {
                let value = datetime.format_with_items(DATE_FORMAT_ITEMS.iter());
                write!(f, "{value}")
            }
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Date64Formatter(pub i64);

impl Display for Date64Formatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match date64_to_datetime(self.0) {
            Some(datetime) => {
                let value = datetime.format_with_items(DATE_FORMAT_ITEMS.iter());
                write!(f, "{value}")
            }
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time32SecondFormatter(pub i32);

impl Display for Time32SecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time32s_to_time(self.0) {
            Some(time) => {
                let value = time.format_with_items(TIME_FORMAT_ITEMS.iter());
                write!(f, "{value}")
            }
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time32MillisecondFormatter(pub i32);

impl Display for Time32MillisecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time32ms_to_time(self.0) {
            Some(time) => {
                let value = time.format_with_items(TIME_FORMAT_ITEMS.iter());
                let fraction = FractionFormatter::<3>(time.nanosecond() / 1_000_000);
                write!(f, "{value}{fraction}")
            }
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time64MicrosecondFormatter(pub i64);

impl Display for Time64MicrosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time64us_to_time(self.0) {
            Some(time) => {
                let value = time.format_with_items(TIME_FORMAT_ITEMS.iter());
                let fraction = FractionFormatter::<6>(time.nanosecond() / 1_000);
                write!(f, "{value}{fraction}")
            }
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time64NanosecondFormatter(pub i64);

impl Display for Time64NanosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time64ns_to_time(self.0) {
            Some(time) => {
                let value = time.format_with_items(TIME_FORMAT_ITEMS.iter());
                let fraction = FractionFormatter::<9>(time.nanosecond());
                write!(f, "{value}{fraction}")
            }
            None => write!(f, "ERROR"),
        }
    }
}

pub struct IntervalYearMonthFormatter(pub i32);

impl Display for IntervalYearMonthFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let months = self.0;
        let years = months / 12;
        let prepend = if years == 0 && months < 0 { "-" } else { "" };
        let months = (months % 12).abs();
        write!(f, "INTERVAL '{prepend}{years}-{months}' YEAR TO MONTH")
    }
}

pub struct IntervalDayTimeFormatter(pub IntervalDayTime);

impl Display for IntervalDayTimeFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(value) = self;
        let days = value.days + (value.milliseconds / 86_400_000); // Add days from milliseconds
        let milliseconds = value.milliseconds % 86_400_000; // Get remaining sub-day milliseconds
        let prepend = if days == 0 && milliseconds < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((milliseconds % 86_400_000) / 3_600_000).abs();
        let minutes = ((milliseconds % 3_600_000) / 60_000).abs();
        let seconds = ((milliseconds % 60_000) / 1_000).abs();
        let fraction = FractionFormatter::<3>((milliseconds % 1_000).unsigned_abs());
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}{fraction}' DAY TO SECOND"
        )
    }
}

pub struct IntervalMonthDayNanoFormatter(pub IntervalMonthDayNano);

macro_rules! write_interval_part {
    ($f:expr, $sep:expr, $value:expr, $unit:expr) => {
        if $value != 0 {
            write!($f, "{}{} {}", $sep, $value, $unit)?;
            " "
        } else {
            $sep
        }
    };
}

impl Display for IntervalMonthDayNanoFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(value) = self;

        let years = value.months / 12;
        let months = value.months % 12;
        let days = value.days;
        let hours = value.nanoseconds / 3_600_000_000_000;
        let minutes = (value.nanoseconds % 3_600_000_000_000) / 60_000_000_000;
        let seconds = (value.nanoseconds % 60_000_000_000) / 1_000_000_000;
        let nanoseconds = value.nanoseconds % 1_000_000_000;

        let sep = write_interval_part!(f, "", years, "years");
        let sep = write_interval_part!(f, sep, months, "months");
        let sep = write_interval_part!(f, sep, days, "days");
        let sep = write_interval_part!(f, sep, hours, "hours");
        let sep = write_interval_part!(f, sep, minutes, "minutes");
        let sep = if seconds != 0 || nanoseconds != 0 {
            let prepend = if seconds == 0 && nanoseconds < 0 {
                "-"
            } else {
                ""
            };
            let fraction = FractionFormatter::<9>(nanoseconds.unsigned_abs() as u32);
            write!(f, "{sep}{prepend}{seconds}{fraction} seconds")?;
            " "
        } else {
            sep
        };
        if sep.is_empty() {
            write!(f, "0 seconds")?;
        }
        Ok(())
    }
}

pub struct DurationSecondFormatter(pub i64);

impl Display for DurationSecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let seconds = self.0;
        let days = seconds / 86_400;
        let prepend = if days == 0 && seconds < 0 { "-" } else { "" };
        let hours = ((seconds % 86_400) / 3_600).abs();
        let minutes = ((seconds % 3_600) / 60).abs();
        let seconds = (seconds % 60).abs();
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}' DAY TO SECOND"
        )
    }
}

pub struct DurationMillisecondFormatter(pub i64);

impl Display for DurationMillisecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let milliseconds = self.0;
        let days = milliseconds / 86_400_000;
        let prepend = if days == 0 && milliseconds < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((milliseconds % 86_400_000) / 3_600_000).abs();
        let minutes = ((milliseconds % 3_600_000) / 60_000).abs();
        let seconds = ((milliseconds % 60_000) / 1_000).abs();
        let fraction = FractionFormatter::<3>((milliseconds % 1_000).unsigned_abs() as u32);
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}{fraction}' DAY TO SECOND"
        )
    }
}

pub struct DurationMicrosecondFormatter(pub i64);

impl Display for DurationMicrosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let microseconds = self.0;
        let days = microseconds / 86_400_000_000;
        let prepend = if days == 0 && microseconds < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((microseconds % 86_400_000_000) / 3_600_000_000).abs();
        let minutes = ((microseconds % 3_600_000_000) / 60_000_000).abs();
        let seconds = ((microseconds % 60_000_000) / 1_000_000).abs();
        let fraction = FractionFormatter::<6>((microseconds % 1_000_000).unsigned_abs() as u32);
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}{fraction}' DAY TO SECOND",
        )
    }
}

pub struct DurationNanosecondFormatter(pub i64);

impl Display for DurationNanosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let nanoseconds = self.0;
        let days = nanoseconds / 86_400_000_000_000;
        let prepend = if days == 0 && nanoseconds < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((nanoseconds % 86_400_000_000_000) / 3_600_000_000_000).abs();
        let minutes = ((nanoseconds % 3_600_000_000_000) / 60_000_000_000).abs();
        let seconds = ((nanoseconds % 60_000_000_000) / 1_000_000_000).abs();
        let fraction = FractionFormatter::<9>((nanoseconds % 1_000_000_000).unsigned_abs() as u32);
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}{fraction}' DAY TO SECOND"
        )
    }
}
