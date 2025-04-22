use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use chrono::{TimeZone, Timelike, Utc};
use datafusion::arrow::array::Array;
use datafusion_common::ScalarValue;
use half::f16;
use sail_common::object::DynObject;
use sail_common::{impl_dyn_object_traits, spec};
use sail_common_datafusion::datetime::timestamp::parse_timezone;

use crate::config::{DefaultTimestampType, PlanConfig};
use crate::error::{PlanError, PlanResult};
use crate::utils::ItemTaker;

/// Utilities to format various data structures in the plan specification.
pub trait PlanFormatter: DynObject + Debug + Send + Sync {
    /// Returns a human-readable simple string for the data type.
    fn data_type_to_simple_string(
        &self,
        data_type: &spec::DataType,
        config: &PlanConfig,
    ) -> PlanResult<String>;

    /// Returns a human-readable string for the literal.
    fn literal_to_string(&self, literal: &ScalarValue, config: &PlanConfig) -> PlanResult<String>;

    /// Returns a human-readable string for the function call.
    fn function_to_string(
        &self,
        name: &str,
        arguments: Vec<&str>,
        is_distinct: bool,
    ) -> PlanResult<String>;
}

impl_dyn_object_traits!(PlanFormatter);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct DefaultPlanFormatter;

impl DefaultPlanFormatter {
    fn interval_field_type_to_simple_string(field: spec::IntervalFieldType) -> &'static str {
        match field {
            spec::IntervalFieldType::Year => "year",
            spec::IntervalFieldType::Month => "month",
            spec::IntervalFieldType::Day => "day",
            spec::IntervalFieldType::Hour => "hour",
            spec::IntervalFieldType::Minute => "minute",
            spec::IntervalFieldType::Second => "second",
        }
    }

    fn time_unit_to_simple_string(field: spec::TimeUnit) -> &'static str {
        match field {
            spec::TimeUnit::Second => "second",
            spec::TimeUnit::Millisecond => "millisecond",
            spec::TimeUnit::Microsecond => "microsecond",
            spec::TimeUnit::Nanosecond => "nanosecond",
        }
    }
}

impl PlanFormatter for DefaultPlanFormatter {
    fn data_type_to_simple_string(
        &self,
        data_type: &spec::DataType,
        config: &PlanConfig,
    ) -> PlanResult<String> {
        use spec::DataType;
        match data_type {
            DataType::Null => Ok("void".to_string()),
            DataType::Binary
            | DataType::FixedSizeBinary { size: _ }
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::ConfiguredBinary => Ok("binary".to_string()),
            DataType::Boolean => Ok("boolean".to_string()),
            DataType::Int8 => Ok("tinyint".to_string()),
            DataType::Int16 => Ok("smallint".to_string()),
            DataType::Int32 => Ok("int".to_string()),
            DataType::Int64 => Ok("bigint".to_string()),
            DataType::UInt8 => Ok("unsigned tinyint".to_string()),
            DataType::UInt16 => Ok("unsigned smallint".to_string()),
            DataType::UInt32 => Ok("unsigned int".to_string()),
            DataType::UInt64 => Ok("unsigned bigint".to_string()),
            DataType::Float16 => Ok("half float".to_string()),
            DataType::Float32 => Ok("float".to_string()),
            DataType::Float64 => Ok("double".to_string()),
            DataType::Decimal128 { precision, scale }
            | DataType::Decimal256 { precision, scale } => {
                Ok(format!("decimal({precision},{scale})"))
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Configured,
            } => Ok("string".to_string()),
            DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::VarChar { length },
            } => Ok(format!("varchar({length})")),
            DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Char { length },
            } => Ok(format!("char({length})")),
            DataType::Date32 => Ok("date".to_string()),
            DataType::Date64 => Ok("date64".to_string()),
            DataType::Time32 { time_unit } => Ok(format!(
                "time32({})",
                Self::time_unit_to_simple_string(*time_unit)
            )),
            DataType::Time64 { time_unit } => Ok(format!(
                "time64({})",
                Self::time_unit_to_simple_string(*time_unit)
            )),
            DataType::Duration { time_unit } => Ok(format!(
                "duration({})",
                Self::time_unit_to_simple_string(*time_unit)
            )),
            DataType::Timestamp {
                time_unit: _,
                timestamp_type: spec::TimestampType::Configured,
            } => match config.default_timestamp_type {
                DefaultTimestampType::TimestampLtz => Ok("timestamp".to_string()),
                DefaultTimestampType::TimestampNtz => Ok("timestamp_ntz".to_string()),
            },
            DataType::Timestamp {
                time_unit: _,
                timestamp_type: spec::TimestampType::WithLocalTimeZone,
            } => Ok("timestamp".to_string()),
            DataType::Timestamp {
                time_unit: _,
                timestamp_type: spec::TimestampType::WithoutTimeZone,
            } => Ok("timestamp_ntz".to_string()),
            DataType::Interval {
                interval_unit: spec::IntervalUnit::MonthDayNano,
                start_field: _,
                end_field: _,
            } => Ok("interval".to_string()),
            DataType::Interval {
                interval_unit: spec::IntervalUnit::YearMonth,
                start_field,
                end_field,
            } => {
                let (start_field, end_field) = match (*start_field, *end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(_)) => {
                        return Err(PlanError::invalid(
                            "year-month interval with end field and no start field",
                        ))
                    }
                    (None, None) => (
                        spec::IntervalFieldType::Year,
                        spec::IntervalFieldType::Month,
                    ),
                };

                match start_field.cmp(&end_field) {
                    Ordering::Less => Ok(format!(
                        "interval {} to {}",
                        Self::interval_field_type_to_simple_string(start_field),
                        Self::interval_field_type_to_simple_string(end_field),
                    )),
                    Ordering::Equal => Ok(format!(
                        "interval {}",
                        Self::interval_field_type_to_simple_string(start_field)
                    )),
                    Ordering::Greater => Err(PlanError::invalid(
                        "year-month interval with invalid start and end field order",
                    )),
                }
            }
            DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => {
                let (start_field, end_field) = match (*start_field, *end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(_)) => {
                        return Err(PlanError::invalid(
                            "day-time interval with end field and no start field",
                        ))
                    }
                    (None, None) => (
                        spec::IntervalFieldType::Day,
                        spec::IntervalFieldType::Second,
                    ),
                };

                match start_field.cmp(&end_field) {
                    Ordering::Less => Ok(format!(
                        "interval {} to {}",
                        Self::interval_field_type_to_simple_string(start_field),
                        Self::interval_field_type_to_simple_string(end_field),
                    )),
                    Ordering::Equal => Ok(format!(
                        "interval {}",
                        Self::interval_field_type_to_simple_string(start_field)
                    )),
                    Ordering::Greater => Err(PlanError::invalid(
                        "day-time interval with invalid start and end field order",
                    )),
                }
            }
            DataType::List {
                data_type,
                nullable: _,
            }
            | DataType::FixedSizeList {
                data_type,
                nullable: _,
                length: _,
            }
            | DataType::LargeList {
                data_type,
                nullable: _,
            } => Ok(format!(
                "array<{}>",
                self.data_type_to_simple_string(data_type, config)?
            )),
            DataType::Struct { fields } => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        Ok(format!(
                            "{}:{}",
                            field.name,
                            self.data_type_to_simple_string(&field.data_type, config)?
                        ))
                    })
                    .collect::<PlanResult<Vec<String>>>()?;
                Ok(format!("struct<{}>", fields.join(",")))
            }
            DataType::Map {
                key_type,
                value_type,
                value_type_nullable: _,
                keys_sorted: _,
            } => Ok(format!(
                "map<{},{}>",
                self.data_type_to_simple_string(key_type.as_ref(), config)?,
                self.data_type_to_simple_string(value_type.as_ref(), config)?
            )),
            DataType::UserDefined { sql_type, .. } => {
                self.data_type_to_simple_string(sql_type.as_ref(), config)
            }
            DataType::Union {
                union_fields: _,
                union_mode: _,
            } => {
                // TODO: Add union_fields and union_mode
                Ok("union".to_string())
            }
            DataType::Dictionary {
                key_type,
                value_type,
            } => Ok(format!(
                "dictionary<{},{}>",
                self.data_type_to_simple_string(key_type, config)?,
                self.data_type_to_simple_string(value_type, config)?
            )),
        }
    }

    fn literal_to_string(&self, literal: &ScalarValue, config: &PlanConfig) -> PlanResult<String> {
        let literal_list_to_string = |name: &str,
                                      values: Option<&dyn Array>|
         -> PlanResult<String> {
            let Some(values) = values else {
                return Ok("NULL".to_string());
            };
            let values = (0..values.len())
                .map(|i| self.literal_to_string(&ScalarValue::try_from_array(values, i)?, config))
                .collect::<PlanResult<Vec<String>>>()?;
            Ok(format!("{name}({})", values.join(", ")))
        };

        match literal {
            ScalarValue::Null => Ok("NULL".to_string()),
            ScalarValue::Boolean(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int8(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int16(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int32(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Int64(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt8(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt16(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt32(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::UInt64(value) => match value {
                Some(value) => Ok(format!("{value}")),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Float16(value) => match value {
                Some(value) => {
                    let value = f16::to_f32(*value);
                    let mut buffer = ryu::Buffer::new();
                    Ok(buffer.format(value).to_string())
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Float32(value) => match value {
                Some(value) => {
                    let mut buffer = ryu::Buffer::new();
                    Ok(buffer.format(*value).to_string())
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Float64(value) => match value {
                Some(value) => {
                    let mut buffer = ryu::Buffer::new();
                    Ok(buffer.format(*value).to_string())
                }
                None => Ok("NULL".to_string()),
            },
            // For timestamp values with no time zone, we use UTC as the time zone for formatting.
            ScalarValue::TimestampSecond(seconds, timezone) => match seconds {
                Some(seconds) => {
                    let datetime = Utc.timestamp_opt(*seconds, 0).earliest().ok_or_else(|| {
                        PlanError::invalid(format!("timestamp second: {seconds}"))
                    })?;
                    let timezone = if timezone.is_some() {
                        Some(config.session_timezone.as_ref())
                    } else {
                        None
                    };
                    format_timestamp(datetime, timezone)
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::TimestampMillisecond(milliseconds, timezone) => match milliseconds {
                Some(milliseconds) => {
                    let datetime = Utc
                        .timestamp_millis_opt(*milliseconds)
                        .earliest()
                        .ok_or_else(|| {
                            PlanError::invalid(format!("timestamp millisecond: {milliseconds}"))
                        })?;
                    let timezone = if timezone.is_some() {
                        Some(config.session_timezone.as_ref())
                    } else {
                        None
                    };
                    format_timestamp(datetime, timezone)
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::TimestampMicrosecond(microseconds, timezone) => match microseconds {
                Some(microseconds) => {
                    let datetime =
                        Utc.timestamp_micros(*microseconds)
                            .earliest()
                            .ok_or_else(|| {
                                PlanError::invalid(format!("timestamp microsecond: {microseconds}"))
                            })?;
                    let timezone = if timezone.is_some() {
                        Some(config.session_timezone.as_ref())
                    } else {
                        None
                    };
                    format_timestamp(datetime, timezone)
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::TimestampNanosecond(nanoseconds, timezone) => match nanoseconds {
                Some(nanoseconds) => {
                    let datetime = Utc.timestamp_nanos(*nanoseconds);
                    let timezone = if timezone.is_some() {
                        Some(config.session_timezone.as_ref())
                    } else {
                        None
                    };
                    format_timestamp(datetime, timezone)
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Date32(days) => match days {
                Some(days) => {
                    let date =
                        chrono::NaiveDateTime::UNIX_EPOCH + chrono::Duration::days(*days as i64);
                    Ok(format!("DATE '{}'", date.format("%Y-%m-%d")))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Date64(milliseconds) => match milliseconds {
                Some(milliseconds) => {
                    let date = chrono::NaiveDateTime::UNIX_EPOCH
                        + chrono::Duration::milliseconds(*milliseconds);
                    Ok(format!("DATE '{}'", date.format("%Y-%m-%d")))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time32Second(seconds) => match seconds {
                Some(seconds) => {
                    let secs = *seconds as u32;
                    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, 0)
                        .ok_or_else(|| {
                            PlanError::invalid("invalid Time32Second: literal to string")
                        })?;
                    Ok(format!("TIME '{}'", time.format("%H:%M:%S")))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time32Millisecond(milliseconds) => match milliseconds {
                Some(milliseconds) => {
                    let secs = (*milliseconds / 1000) as u32;
                    let nanos = ((*milliseconds % 1000) * 1_000_000) as u32;
                    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                        .ok_or_else(|| {
                        PlanError::invalid("invalid Time32Millisecond: literal to string")
                    })?;
                    Ok(format!("TIME '{}'", time.format("%H:%M:%S.%3f")))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time64Microsecond(microseconds) => match microseconds {
                Some(microseconds) => {
                    let secs = (*microseconds / 1_000_000) as u32;
                    let nanos = ((*microseconds % 1_000_000) * 1000) as u32;
                    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                        .ok_or_else(|| {
                        PlanError::invalid("invalid Time64Microsecond: literal to string")
                    })?;
                    Ok(format!("TIME '{}'", time.format("%H:%M:%S.%6f")))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Time64Nanosecond(nanoseconds) => match nanoseconds {
                Some(nanoseconds) => {
                    let secs = (*nanoseconds / 1_000_000_000) as u32;
                    let nanos = (*nanoseconds % 1_000_000_000) as u32;
                    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                        .ok_or_else(|| {
                        PlanError::invalid("invalid Time64Nanosecond: literal to string")
                    })?;
                    Ok(format!("TIME '{}'", time.format("%H:%M:%S.%9f")))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationSecond(seconds) => match seconds {
                Some(seconds) => {
                    let days = *seconds / 86_400;
                    let prepend = if days < 0 {
                        ""
                    } else if days == 0 && *seconds < 0 {
                        "-"
                    } else {
                        ""
                    };
                    let hours = ((*seconds % 86_400) / 3_600).abs();
                    let minutes = ((*seconds % 3_600) / 60).abs();
                    let seconds = (*seconds % 60).abs();
                    Ok(format!(
                        "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}' DAY TO SECOND"
                    ))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationMillisecond(milliseconds) => match milliseconds {
                Some(milliseconds) => {
                    let days = *milliseconds / 86_400_000;
                    let prepend = if days < 0 {
                        ""
                    } else if days == 0 && *milliseconds < 0 {
                        "-"
                    } else {
                        ""
                    };
                    let hours = ((*milliseconds % 86_400_000) / 3_600_000).abs();
                    let minutes = ((*milliseconds % 3_600_000) / 60_000).abs();
                    let seconds = ((*milliseconds % 60_000) / 1_000).abs();
                    let milliseconds = (*milliseconds % 1_000).abs();
                    Ok(format!(
                        "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}' DAY TO SECOND"
                    ))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationMicrosecond(microseconds) => match microseconds {
                Some(microseconds) => {
                    let days = *microseconds / 86_400_000_000;
                    let prepend = if days < 0 {
                        ""
                    } else if days == 0 && *microseconds < 0 {
                        "-"
                    } else {
                        ""
                    };
                    let hours = ((*microseconds % 86_400_000_000) / 3_600_000_000).abs();
                    let minutes = ((*microseconds % 3_600_000_000) / 60_000_000).abs();
                    let seconds = ((*microseconds % 60_000_000) / 1_000_000).abs();
                    let microseconds = (*microseconds % 1_000_000).abs();
                    Ok(format!(
                        "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}' DAY TO SECOND",
                    ))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::DurationNanosecond(nanoseconds) => match nanoseconds {
                Some(nanoseconds) => {
                    let days = *nanoseconds / 86_400_000_000_000;
                    let prepend = if days < 0 {
                        ""
                    } else if days == 0 && *nanoseconds < 0 {
                        "-"
                    } else {
                        ""
                    };
                    let hours = ((*nanoseconds % 86_400_000_000_000) / 3_600_000_000_000).abs();
                    let minutes = ((*nanoseconds % 3_600_000_000_000) / 60_000_000_000).abs();
                    let seconds = ((*nanoseconds % 60_000_000_000) / 1_000_000_000).abs();
                    let nanoseconds = (*nanoseconds % 1_000_000_000).abs();
                    Ok(format!(
                        "INTERVAL '{prepend}{days} {hours:02}:{minutes:02}:{seconds:02}.{nanoseconds:09}' DAY TO SECOND"
                    ))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::IntervalYearMonth(months) => match months {
                Some(months) => {
                    let years = *months / 12;
                    let prepend = if years < 0 {
                        ""
                    } else if years == 0 && *months < 0 {
                        "-"
                    } else {
                        ""
                    };
                    let months = (*months % 12).abs();
                    Ok(format!(
                        "INTERVAL '{prepend}{years}-{months}' YEAR TO MONTH"
                    ))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::IntervalDayTime(value) => match value {
                Some(value) => {
                    let (days, milliseconds) = (value.days, value.milliseconds);
                    let total_days = days + (milliseconds / 86_400_000); // Add days from milliseconds
                    let remaining_millis = milliseconds % 86_400_000; // Get remaining sub-day milliseconds
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
                    Ok(format!(
                        "INTERVAL '{prepend}{total_days} {hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}' DAY TO SECOND"
                    ))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::IntervalMonthDayNano(value) => match value {
                Some(value) => {
                    let (months, days, nanoseconds) = (value.months, value.days, value.nanoseconds);
                    let years = months / 12;
                    let months = months % 12;
                    let hours = nanoseconds / 3_600_000_000_000;
                    let minutes = (nanoseconds % 3_600_000_000_000) / 60_000_000_000;
                    let seconds = (nanoseconds % 60_000_000_000) / 1_000_000_000;
                    let milliseconds = (nanoseconds % 1_000_000_000) / 1_000_000;
                    let microseconds = (nanoseconds % 1_000_000) / 1_000;
                    let nanoseconds = nanoseconds % 1_000;
                    Ok(format!(
                        "INTERVAL {years} YEAR {months} MONTH {days} DAY {hours} HOUR {minutes} MINUTE {seconds} SECOND {milliseconds} MILLISECOND {microseconds} MICROSECOND {nanoseconds} NANOSECOND"
                    ))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Binary(value)
            | ScalarValue::FixedSizeBinary(_, value)
            | ScalarValue::LargeBinary(value)
            | ScalarValue::BinaryView(value) => match value {
                Some(value) => Ok(BinaryDisplay(value).to_string()),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Utf8(value)
            | ScalarValue::LargeUtf8(value)
            | ScalarValue::Utf8View(value) => match value {
                Some(value) => Ok(value.clone()),
                None => Ok("NULL".to_string()),
            },
            ScalarValue::List(values) => {
                let values = values.iter().collect::<Vec<_>>().one()?;
                literal_list_to_string("array", values.as_deref())
            }
            ScalarValue::FixedSizeList(values) => {
                let values = values.iter().collect::<Vec<_>>().one()?;
                literal_list_to_string("array", values.as_deref())
            }
            ScalarValue::LargeList(values) => {
                let values = values.iter().collect::<Vec<_>>().one()?;
                literal_list_to_string("array", values.as_deref())
            }
            ScalarValue::Struct(values) => {
                let fields = values
                    .fields()
                    .iter()
                    .zip(values.columns())
                    .map(|(field, array)| {
                        if array.len() != 1 {
                            return Err(PlanError::invalid(
                                "expected struct literal with one value",
                            ));
                        }
                        let value = ScalarValue::try_from_array(array, 0)?;
                        Ok(format!(
                            "{} AS {}",
                            self.literal_to_string(&value, config)?,
                            field.name()
                        ))
                    })
                    .collect::<PlanResult<Vec<String>>>()?;
                Ok(format!("struct({})", fields.join(", ")))
            }
            ScalarValue::Union(value, _union_fields, _union_mode) => match value {
                Some((id, value)) => {
                    let value = self.literal_to_string(value, config)?;
                    Ok(format!("{id}:{value}"))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Dictionary(_, value) => {
                let value = self.literal_to_string(value, config)?;
                Ok(format!("dictionary({value})"))
            }
            ScalarValue::Decimal128(value, _precision, scale) => match value {
                Some(value) => {
                    let value = format!("{value}");
                    Ok(format_decimal(value.as_str(), *scale))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Decimal256(value, _precision, scale) => match value {
                Some(value) => {
                    let value = format!("{value}");
                    Ok(format_decimal(value.as_str(), *scale))
                }
                None => Ok("NULL".to_string()),
            },
            ScalarValue::Map(array) => match array.iter().collect::<Vec<_>>().one()? {
                Some(value) => {
                    let [keys, values] = value.columns() else {
                        return Err(PlanError::invalid(
                            "expected map literal with keys and values",
                        ));
                    };
                    let k = literal_list_to_string("array", Some(keys))?;
                    let v = literal_list_to_string("array", Some(values))?;
                    Ok(format!("map({k}, {v})"))
                }
                None => Ok("NULL".to_string()),
            },
        }
    }

    fn function_to_string(
        &self,
        name: &str,
        arguments: Vec<&str>,
        is_distinct: bool,
    ) -> PlanResult<String> {
        match name.to_lowercase().as_str() {
            "!" | "~" => Ok(format!("({name} {})", arguments.one()?)),
            "+" | "-" => {
                if arguments.len() < 2 {
                    Ok(format!("({name} {})", arguments.one()?))
                } else {
                    let (left, right) = arguments.two()?;
                    Ok(format!("({left} {name} {right})"))
                }
            }
            "==" => {
                let (left, right) = arguments.two()?;
                Ok(format!("({left} = {right})"))
            }
            "&" | "^" | "|" | "*" | "/" | "%" | "!=" | "<" | "<=" | "<=>" | "=" | ">" | ">=" => {
                let (left, right) = arguments.two()?;
                Ok(format!("({left} {name} {right})"))
            }
            "and" | "or" => {
                let (left, right) = arguments.two()?;
                Ok(format!("({left} {} {right})", name.to_uppercase()))
            }
            "not" => Ok(format!("(NOT {})", arguments.one()?)),
            "isnull" => Ok(format!("({} IS NULL)", arguments.one()?)),
            "isnotnull" => Ok(format!("({} IS NOT NULL)", arguments.one()?)),
            "in" => {
                let (value, list) = arguments.at_least_one()?;
                Ok(format!("({value} IN ({}))", list.join(", ")))
            }
            "case" | "when" => {
                let mut result = String::from("CASE");
                let mut i = 0;
                while i < arguments.len() {
                    if i + 1 < arguments.len() {
                        result.push_str(&format!(
                            " WHEN {} THEN {}",
                            arguments[i],
                            arguments[i + 1]
                        ));
                        i += 2;
                    } else {
                        result.push_str(&format!(" ELSE {}", arguments[i]));
                        break;
                    }
                }
                result.push_str(" END");
                Ok(result)
            }
            "timestamp" | "date" => Ok(arguments.one()?.to_string()),
            "dateadd" => {
                let arguments = arguments.join(", ");
                Ok(format!("date_add({arguments})"))
            }
            "sum" => {
                let mut args = arguments.join(", ");
                if is_distinct {
                    args = format!("DISTINCT {args}");
                }
                Ok(format!("{name}({args})"))
            }
            "first" | "last" => {
                let name = name.to_lowercase();
                let arg = arguments[0];
                Ok(format!("{name}({arg})"))
            }
            "any_value" | "first_value" | "last_value" => {
                let arg = arguments[0];
                Ok(format!("{name}({arg})"))
            }
            "substr" | "substring" => {
                let args = if arguments.len() == 2 {
                    let args = arguments.join(", ");
                    format!("{args}, 2147483647")
                } else {
                    arguments.join(", ")
                };
                Ok(format!("{name}({args})"))
            }
            "ceil" | "floor" => {
                let name = if arguments.len() == 1 {
                    name.to_uppercase()
                } else {
                    name.to_string()
                };
                let args = arguments
                    .iter()
                    .map(|arg| {
                        if arg.starts_with("(- ") && arg.ends_with(")") {
                            format!("-{}", &arg[3..arg.len() - 1])
                        } else {
                            arg.to_string()
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                Ok(format!("{name}({args})"))
            }
            "ceiling" | "typeof" => {
                let args = arguments
                    .iter()
                    .map(|arg| {
                        if arg.starts_with("(- ") && arg.ends_with(")") {
                            format!("-{}", &arg[3..arg.len() - 1])
                        } else {
                            arg.to_string()
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                Ok(format!("{name}({args})"))
            }
            // This case is only reached when both conditions are true:
            //   1. The `explode` operation is `ExplodeKind::ExplodeOuter`
            //   2. The data type being exploded is `ExplodeDataType::List`
            // In this specific scenario, we always use "col" as the column name.
            "explode_outer" => Ok("col".to_string()),
            "current_schema" => Ok("current_database()".to_string()),
            "acos" | "acosh" | "asin" | "asinh" | "atan" | "atan2" | "atanh" | "cbrt" | "exp"
            | "log10" | "regexp" | "regexp_like" | "signum" | "sqrt" | "cos" | "cosh" | "cot"
            | "degrees" | "power" | "radians" | "sin" | "sinh" | "tan" | "tanh" | "pi"
            | "expm1" | "hypot" | "log1p" | "e" => {
                let name = name.to_uppercase();
                let arguments = arguments.join(", ");
                Ok(format!("{name}({arguments})"))
            }
            // FIXME: This is incorrect if the column name is `*`:
            //   ```
            //   SELECT count(`*`) FROM VALUES 1 AS t(`*`)
            //   ```
            "count" if matches!(arguments.as_slice(), ["*"]) => Ok("count(1)".to_string()),
            _ => {
                let arguments = arguments.join(", ");
                Ok(format!("{name}({arguments})"))
            }
        }
    }
}

struct BinaryDisplay<'a>(pub &'a Vec<u8>);

impl Display for BinaryDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "X'")?;
        for b in self.0 {
            write!(f, "{:02X}", b)?;
        }
        write!(f, "'")
    }
}

fn format_decimal(value: &str, scale: i8) -> String {
    let mut result = String::new();
    let start = if value.starts_with('-') {
        result.push('-');
        1
    } else {
        0
    };
    let scale = if scale > 0 { scale as usize } else { 0 };
    if scale == 0 {
        result.push_str(&value[start..]);
    } else if start + scale < value.len() {
        let d = value.len() - scale;
        result.push_str(&format!("{}.{}", &value[start..d], &value[d..]));
    } else {
        result.push_str(&format!("0.{:0>width$}", &value[start..], width = scale));
    }
    result
}

fn format_timestamp(datetime: chrono::DateTime<Utc>, timezone: Option<&str>) -> PlanResult<String> {
    let (prefix, datetime) = if let Some(timezone) = timezone {
        let tz = parse_timezone(timezone)?;
        ("TIMESTAMP", tz.from_utc(&datetime))
    } else {
        ("TIMESTAMP_NTZ", datetime.naive_utc())
    };
    let value = datetime.format("%Y-%m-%d %H:%M:%S");
    if datetime.nanosecond() > 0 {
        let fraction = datetime.format("%.f").to_string();
        let fraction = fraction.trim_end_matches('0');
        Ok(format!("{prefix} '{value}{fraction}'"))
    } else {
        Ok(format!("{prefix} '{value}'"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        FixedSizeListArray, Float64Array, LargeListArray, ListArray, MapArray, StringArray,
        StructArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{
        i256, DataType, Field, Int32Type, IntervalDayTime, IntervalMonthDayNano,
    };
    use datafusion_common::arrow::array::ArrayRef;

    use super::*;

    #[test]
    fn test_literal_to_string() -> PlanResult<()> {
        let plan_config = PlanConfig::new()?;
        let formatter = DefaultPlanFormatter;
        let to_string = |literal| formatter.literal_to_string(&literal, &plan_config);

        assert_eq!(to_string(ScalarValue::Null)?, "NULL");
        assert_eq!(
            to_string(ScalarValue::Binary(Some(vec![16, 0x20, 0xff])))?,
            "X'1020FF'",
        );
        assert_eq!(to_string(ScalarValue::Boolean(Some(true)))?, "true");
        assert_eq!(to_string(ScalarValue::Int8(Some(10)))?, "10");
        assert_eq!(to_string(ScalarValue::Int16(Some(-20)))?, "-20");
        assert_eq!(to_string(ScalarValue::Int32(Some(30)))?, "30");
        assert_eq!(to_string(ScalarValue::Int64(Some(-40)))?, "-40");
        assert_eq!(to_string(ScalarValue::Float32(Some(1.0)))?, "1.0");
        assert_eq!(to_string(ScalarValue::Float64(Some(-0.1)))?, "-0.1");
        assert_eq!(to_string(ScalarValue::Decimal128(Some(123), 3, 0))?, "123",);
        assert_eq!(
            to_string(ScalarValue::Decimal128(Some(-123), 3, 0))?,
            "-123",
        );
        assert_eq!(to_string(ScalarValue::Decimal128(Some(123), 3, 2))?, "1.23",);
        assert_eq!(
            to_string(ScalarValue::Decimal128(Some(123), 3, 5))?,
            "0.00123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal128(Some(12300), 3, -2))?,
            "12300",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(123)), 3, 0))?,
            "123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(-123)), 3, 0))?,
            "-123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(123)), 3, 2))?,
            "1.23",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(123)), 3, 5))?,
            "0.00123",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(Some(i256::from(12300)), 3, -2))?,
            "12300",
        );
        assert_eq!(
            to_string(ScalarValue::Decimal256(
                i256::from_string("120000000000000000000000000000000000000000"),
                42,
                5,
            ))?,
            "1200000000000000000000000000000000000.00000",
        );
        assert_eq!(
            to_string(ScalarValue::Utf8(Some("abc".to_string())))?,
            "abc",
        );
        assert_eq!(
            to_string(ScalarValue::LargeUtf8(Some("abc".to_string())))?,
            "abc",
        );
        assert_eq!(
            to_string(ScalarValue::Utf8View(Some("abc".to_string())))?,
            "abc",
        );
        assert_eq!(
            to_string(ScalarValue::Date32(Some(10)))?,
            "DATE '1970-01-11'",
        );
        assert_eq!(
            to_string(ScalarValue::Date32(Some(-5)))?,
            "DATE '1969-12-27'",
        );
        assert_eq!(
            to_string(ScalarValue::TimestampMicrosecond(
                Some(123_000_000),
                Some(Arc::from("UTC")),
            ))?,
            "TIMESTAMP '1970-01-01 00:02:03'",
        );
        assert_eq!(
            to_string(ScalarValue::TimestampMicrosecond(Some(-1), None))?,
            "TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalMonthDayNano (
                Some(IntervalMonthDayNano {
                    months: 15,
                    days: -20,
                    nanoseconds: 123_456_789_000,
                })
            ))?,
            "INTERVAL 1 YEAR 3 MONTH -20 DAY 0 HOUR 2 MINUTE 3 SECOND 456 MILLISECOND 789 MICROSECOND 0 NANOSECOND",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalMonthDayNano (
                Some(IntervalMonthDayNano {
                    months: -15,
                    days: 10,
                    nanoseconds: -1_001_000,
                })
            ))?,
            "INTERVAL -1 YEAR -3 MONTH 10 DAY 0 HOUR 0 MINUTE 0 SECOND -1 MILLISECOND -1 MICROSECOND 0 NANOSECOND",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalYearMonth(Some(15)))?,
            "INTERVAL '1-3' YEAR TO MONTH",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalYearMonth(Some(-15)))?,
            "INTERVAL '-1-3' YEAR TO MONTH",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 123_456_000,
            })))?,
            "INTERVAL '1 10:17:36.000' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: -123_456_000,
            })))?,
            "INTERVAL '-1 10:17:36.000' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::DurationMicrosecond(Some(123_456_789)))?,
            "INTERVAL '0 00:02:03.456789' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::DurationMicrosecond(Some(-123_456_789)))?,
            "INTERVAL '-0 00:02:03.456789' DAY TO SECOND",
        );
        assert_eq!(
            to_string(ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(-2)
                ])])
            )))?,
            "array(1, -2)",
        );
        assert_eq!(
            to_string(ScalarValue::LargeList(Arc::new(
                LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(-2),
                ])])
            )))?,
            "array(1, -2)",
        );
        assert_eq!(
            to_string(ScalarValue::FixedSizeList(Arc::new(
                FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    vec![Some(vec![Some(1), Some(-2),])],
                    2
                )
            )))?,
            "array(1, -2)",
        );
        assert_eq!(
            to_string(ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Float64, true)),
                        ]
                        .into()
                    ),
                    false
                )),
                OffsetBuffer::new(vec![0, 2].into()),
                StructArray::try_from(vec![
                    (
                        "keys",
                        Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef
                    ),
                    (
                        "values",
                        Arc::new(Float64Array::from(vec![Some(1.0), None])) as ArrayRef
                    )
                ])?,
                None,
                false
            ))))?,
            "map(array(a, b), array(1.0, NULL))",
        );
        assert_eq!(
            to_string(ScalarValue::Struct(Arc::new(StructArray::try_from(vec![
                (
                    "foo",
                    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                        Some(vec![Some(1), None])
                    ])) as ArrayRef
                ),
                (
                    "bar",
                    Arc::new(StructArray::try_from(vec![(
                        "baz",
                        Arc::new(StringArray::from(vec![Some("hello")])) as ArrayRef
                    )])?) as ArrayRef
                )
            ])?)))?,
            "struct(array(1, NULL) AS foo, struct(hello AS baz) AS bar)",
        );
        Ok(())
    }
}
