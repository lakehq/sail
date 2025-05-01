use std::fmt::{Display, Formatter};

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
    let value = datetime.format("%Y-%m-%d %H:%M:%S");
    if datetime.nanosecond() > 0 {
        let fraction = datetime.format("%.f").to_string();
        let fraction = fraction.trim_end_matches('0');
        write!(f, "{value}{fraction}")
    } else {
        write!(f, "{value}")
    }
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
            Some(dt) => write!(f, "DATE '{}'", dt.format("%Y-%m-%d")),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Date64Formatter(pub i64);

impl Display for Date64Formatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match date64_to_datetime(self.0) {
            Some(dt) => write!(f, "DATE '{}'", dt.format("%Y-%m-%d")),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time32SecondFormatter(pub i32);

impl Display for Time32SecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time32s_to_time(self.0) {
            Some(time) => write!(f, "TIME '{}'", time.format("%H:%M:%S")),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time32MillisecondFormatter(pub i32);

impl Display for Time32MillisecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time32ms_to_time(self.0) {
            Some(time) => write!(f, "TIME '{}'", time.format("%H:%M:%S.%3f")),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time64MicrosecondFormatter(pub i64);

impl Display for Time64MicrosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time64us_to_time(self.0) {
            Some(time) => write!(f, "TIME '{}'", time.format("%H:%M:%S.%6f")),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct Time64NanosecondFormatter(pub i64);

impl Display for Time64NanosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match time64ns_to_time(self.0) {
            Some(time) => write!(f, "TIME '{}'", time.format("%H:%M:%S.%9f")),
            None => write!(f, "ERROR"),
        }
    }
}

pub struct IntervalYearMonthFormatter(pub i32);

impl Display for IntervalYearMonthFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let months = self.0;
        let years = months / 12;
        let prepend = if years < 0 {
            ""
        } else if years == 0 && months < 0 {
            "-"
        } else {
            ""
        };
        let months = (months % 12).abs();
        write!(f, "INTERVAL '{prepend}{years}-{months}' YEAR TO MONTH")
    }
}

pub struct IntervalDayTimeFormatter(pub IntervalDayTime);

impl Display for IntervalDayTimeFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(value) = self;
        let total_days = value.days + (value.milliseconds / 86_400_000); // Add days from milliseconds
        let remaining_millis = value.milliseconds % 86_400_000; // Get remaining sub-day milliseconds
        let prepend = if total_days < 0 {
            ""
        } else if total_days == 0 && remaining_millis < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((remaining_millis % 86_400_000) / 3_600_000).abs();
        let minutes = ((remaining_millis % 3_600_000) / 60_000).abs();
        let seconds = ((remaining_millis % 60_000) / 1_000).abs();
        let milliseconds = (remaining_millis % 1_000).abs();
        write!(
            f,
            "INTERVAL '{prepend}{total_days} {hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}' DAY TO SECOND"
        )
    }
}

pub struct IntervalMonthDayNanoFormatter(pub IntervalMonthDayNano);

impl Display for IntervalMonthDayNanoFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self(value) = self;
        let years = value.months / 12;
        let months = value.months % 12;
        let days = value.days;
        let hours = value.nanoseconds / 3_600_000_000_000;
        let minutes = (value.nanoseconds % 3_600_000_000_000) / 60_000_000_000;
        let seconds = (value.nanoseconds % 60_000_000_000) / 1_000_000_000;
        let milliseconds = (value.nanoseconds % 1_000_000_000) / 1_000_000;
        let microseconds = (value.nanoseconds % 1_000_000) / 1_000;
        let nanoseconds = value.nanoseconds % 1_000;
        write!(
            f,
            "INTERVAL {years} YEAR {months} MONTH {days} DAY {hours} HOUR {minutes} MINUTE {seconds} SECOND {milliseconds} MILLISECOND {microseconds} MICROSECOND {nanoseconds} NANOSECOND"
        )
    }
}

pub struct DurationSecondFormatter(pub i64);

impl Display for DurationSecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let seconds = self.0;
        let days = seconds / 86_400;
        let prepend = if days < 0 {
            ""
        } else if days == 0 && seconds < 0 {
            "-"
        } else {
            ""
        };
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
        let prepend = if days < 0 {
            ""
        } else if days == 0 && milliseconds < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((milliseconds % 86_400_000) / 3_600_000).abs();
        let minutes = ((milliseconds % 3_600_000) / 60_000).abs();
        let seconds = ((milliseconds % 60_000) / 1_000).abs();
        let milliseconds = (milliseconds % 1_000).abs();
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}' DAY TO SECOND"
        )
    }
}

pub struct DurationMicrosecondFormatter(pub i64);

impl Display for DurationMicrosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let microseconds = self.0;
        let days = microseconds / 86_400_000_000;
        let prepend = if days < 0 {
            ""
        } else if days == 0 && microseconds < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((microseconds % 86_400_000_000) / 3_600_000_000).abs();
        let minutes = ((microseconds % 3_600_000_000) / 60_000_000).abs();
        let seconds = ((microseconds % 60_000_000) / 1_000_000).abs();
        let microseconds = (microseconds % 1_000_000).abs();
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}' DAY TO SECOND",
        )
    }
}

pub struct DurationNanosecondFormatter(pub i64);

impl Display for DurationNanosecondFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let nanoseconds = self.0;
        let days = nanoseconds / 86_400_000_000_000;
        let prepend = if days < 0 {
            ""
        } else if days == 0 && nanoseconds < 0 {
            "-"
        } else {
            ""
        };
        let hours = ((nanoseconds % 86_400_000_000_000) / 3_600_000_000_000).abs();
        let minutes = ((nanoseconds % 3_600_000_000_000) / 60_000_000_000).abs();
        let seconds = ((nanoseconds % 60_000_000_000) / 1_000_000_000).abs();
        let nanoseconds = (nanoseconds % 1_000_000_000).abs();
        write!(
            f,
            "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}.{nanoseconds:09}' DAY TO SECOND"
        )
    }
}
