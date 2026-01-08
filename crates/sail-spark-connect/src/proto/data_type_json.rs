use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use monostate::MustBe;
use serde::{Deserialize, Serialize};

use crate::error::SparkResult;
use crate::spark::connect as sc;
use crate::spark::connect::data_type as dt;

/// References:
///   org.apache.spark.sql.types.DataType#parseDataType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonDataType {
    #[serde(alias = "void")]
    Null,
    String,
    Binary,
    Boolean,
    Decimal,
    Float,
    Double,
    Byte,
    Short,
    Integer,
    Long,
    Date,
    #[serde(alias = "timestamp_ltz")]
    Timestamp,
    TimestampNtz,
    #[serde(rename = "interval")]
    CalendarInterval,
    Variant,
    #[serde(untagged, with = "serde_char")]
    Char {
        length: i32,
    },
    #[serde(untagged, with = "serde_varchar")]
    Varchar {
        length: i32,
    },
    #[serde(untagged, with = "serde_fixed_decimal")]
    FixedDecimal {
        precision: i32,
        scale: i32,
    },
    #[serde(untagged, with = "serde_day_time_interval")]
    DayTimeInterval {
        start: Option<DayTimeIntervalField>,
        end: Option<DayTimeIntervalField>,
    },
    #[serde(untagged, with = "serde_year_month_interval")]
    YearMonthInterval {
        start: Option<YearMonthIntervalField>,
        end: Option<YearMonthIntervalField>,
    },
    #[serde(untagged, with = "serde_time")]
    Time {
        precision: Option<i32>,
    },
    #[serde(untagged, rename_all = "camelCase")]
    Array {
        r#type: MustBe!("array"),
        element_type: Box<JsonDataType>,
        contains_null: bool,
    },
    #[serde(untagged, rename_all = "camelCase")]
    Map {
        r#type: MustBe!("map"),
        key_type: Box<JsonDataType>,
        value_type: Box<JsonDataType>,
        value_contains_null: bool,
    },
    #[serde(untagged, rename_all = "camelCase")]
    Struct {
        r#type: MustBe!("struct"),
        fields: Vec<JsonStructField>,
    },
    #[serde(untagged, rename_all = "camelCase")]
    Udt {
        r#type: MustBe!("udt"),
        class: Option<String>,
        py_class: Option<String>,
        serialized_class: Option<String>,
        sql_type: Option<Box<JsonDataType>>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct JsonStructField {
    pub name: String,
    pub nullable: bool,
    pub r#type: JsonDataType,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DayTimeIntervalField {
    Day = 0,
    Hour = 1,
    Minute = 2,
    Second = 3,
}

impl FromStr for DayTimeIntervalField {
    type Err = serde::de::value::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "day" => Ok(DayTimeIntervalField::Day),
            "hour" => Ok(DayTimeIntervalField::Hour),
            "minute" => Ok(DayTimeIntervalField::Minute),
            "second" => Ok(DayTimeIntervalField::Second),
            _ => Err(serde::de::Error::custom(format!(
                "invalid day time interval field: {s}"
            ))),
        }
    }
}

impl Display for DayTimeIntervalField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DayTimeIntervalField::Day => write!(f, "day"),
            DayTimeIntervalField::Hour => write!(f, "hour"),
            DayTimeIntervalField::Minute => write!(f, "minute"),
            DayTimeIntervalField::Second => write!(f, "second"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum YearMonthIntervalField {
    Year = 0,
    Month = 1,
}

impl FromStr for YearMonthIntervalField {
    type Err = serde::de::value::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "year" => Ok(YearMonthIntervalField::Year),
            "month" => Ok(YearMonthIntervalField::Month),
            _ => Err(serde::de::Error::custom(format!(
                "invalid year month interval field: {s}"
            ))),
        }
    }
}

impl Display for YearMonthIntervalField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            YearMonthIntervalField::Year => write!(f, "year"),
            YearMonthIntervalField::Month => write!(f, "month"),
        }
    }
}

fn create_regex(regex: Result<regex::Regex, regex::Error>) -> regex::Regex {
    #[allow(clippy::unwrap_used)]
    regex.unwrap()
}

mod serde_time {
    use lazy_static::lazy_static;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::proto::data_type_json::create_regex;

    lazy_static! {
        static ref TIME_PRECISION: regex::Regex =
            create_regex(regex::Regex::new(r"^time(\(\s*(\d+)\s*\))?$"));
    }

    pub fn serialize<S>(precision: &Option<i32>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match precision {
            Some(p) => format!("time({})", *p).serialize(serializer),
            None => "time".to_string().serialize(serializer),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let caps = TIME_PRECISION
            .captures(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid time type: {s}")))?;
        if let Some(m) = caps.get(2) {
            Ok(Some(m.as_str().parse().map_err(serde::de::Error::custom)?))
        } else {
            Ok(None)
        }
    }
}

mod serde_char {
    use lazy_static::lazy_static;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::proto::data_type_json::create_regex;

    lazy_static! {
        static ref CHAR_LENGTH: regex::Regex =
            create_regex(regex::Regex::new(r"^char\(\s*(\d+)\s*\)$"));
    }

    pub fn serialize<S>(length: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("char({})", *length).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let caps = CHAR_LENGTH
            .captures(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid char type: {s}")))?;
        caps[1].parse().map_err(serde::de::Error::custom)
    }
}

mod serde_varchar {
    use lazy_static::lazy_static;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::proto::data_type_json::create_regex;

    lazy_static! {
        static ref VARCHAR_LENGTH: regex::Regex =
            create_regex(regex::Regex::new(r"^varchar\(\s*(\d+)\s*\)$"));
    }

    pub fn serialize<S>(length: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("varchar({})", *length).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let caps = VARCHAR_LENGTH
            .captures(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid varchar type: {s}")))?;
        caps[1].parse().map_err(serde::de::Error::custom)
    }
}

mod serde_fixed_decimal {
    use lazy_static::lazy_static;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::proto::data_type_json::create_regex;

    lazy_static! {
        static ref FIXED_DECIMAL: regex::Regex =
            create_regex(regex::Regex::new(r"^decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)$"));
    }

    pub fn serialize<S>(precision: &i32, scale: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("decimal({}, {})", *precision, *scale).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<(i32, i32), D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let caps = FIXED_DECIMAL
            .captures(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid decimal type: {s}")))?;
        Ok((
            caps[1].parse().map_err(serde::de::Error::custom)?,
            caps[2].parse().map_err(serde::de::Error::custom)?,
        ))
    }
}

mod serde_day_time_interval {
    use std::str::FromStr;

    use lazy_static::lazy_static;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::proto::data_type_json::create_regex;

    lazy_static! {
        static ref DAY_TIME_INTERVAL: regex::Regex = create_regex(regex::Regex::new(
            r"^interval\s+(day|hour|minute|second)(\s+to\s+(day|hour|minute|second))?$"
        ));
    }

    pub fn serialize<S>(
        start: &Option<super::DayTimeIntervalField>,
        end: &Option<super::DayTimeIntervalField>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let start = start.as_ref().map(|f| f.to_string());
        let end = end.as_ref().map(|f| f.to_string());
        let interval = match (start, end) {
            (Some(start), Some(end)) => format!("interval {start} to {end}"),
            (Some(start), None) => format!("interval {start}"),
            _ => return Err(serde::ser::Error::custom("invalid day time interval")),
        };
        interval.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<
        (
            Option<super::DayTimeIntervalField>,
            Option<super::DayTimeIntervalField>,
        ),
        D::Error,
    >
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let caps = DAY_TIME_INTERVAL
            .captures(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid day time interval: {s}")))?;
        let start = caps
            .get(1)
            .map(|m| super::DayTimeIntervalField::from_str(m.as_str()))
            .transpose()
            .map_err(serde::de::Error::custom)?;
        let end = caps
            .get(3)
            .map(|m| super::DayTimeIntervalField::from_str(m.as_str()))
            .transpose()
            .map_err(serde::de::Error::custom)?;
        Ok((start, end))
    }
}

mod serde_year_month_interval {
    use std::str::FromStr;

    use lazy_static::lazy_static;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::proto::data_type_json::create_regex;

    lazy_static! {
        static ref YEAR_MONTH_INTERVAL: regex::Regex = create_regex(regex::Regex::new(
            r"^interval\s+(year|month)(\s+to\s+(year|month))?$"
        ));
    }

    pub fn serialize<S>(
        start: &Option<super::YearMonthIntervalField>,
        end: &Option<super::YearMonthIntervalField>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let start = start.as_ref().map(|f| f.to_string());
        let end = end.as_ref().map(|f| f.to_string());
        let interval = match (start, end) {
            (Some(start), Some(end)) => format!("interval {start} to {end}"),
            (Some(start), None) => format!("interval {start}"),
            _ => return Err(serde::ser::Error::custom("invalid year month interval")),
        };
        interval.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<
        (
            Option<super::YearMonthIntervalField>,
            Option<super::YearMonthIntervalField>,
        ),
        D::Error,
    >
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let caps = YEAR_MONTH_INTERVAL
            .captures(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid year month interval: {s}")))?;
        let start = caps
            .get(1)
            .map(|m| super::YearMonthIntervalField::from_str(m.as_str()))
            .transpose()
            .map_err(serde::de::Error::custom)?;
        let end = caps
            .get(3)
            .map(|m| super::YearMonthIntervalField::from_str(m.as_str()))
            .transpose()
            .map_err(serde::de::Error::custom)?;
        Ok((start, end))
    }
}

pub(crate) fn parse_spark_json_data_type(schema: &str) -> SparkResult<sc::DataType> {
    let schema: JsonDataType = serde_json::from_str(schema)?;
    from_spark_json_data_type(schema)
}

fn from_spark_json_data_type(data_type: JsonDataType) -> SparkResult<sc::DataType> {
    let out = match data_type {
        JsonDataType::Null => sc::DataType {
            kind: Some(dt::Kind::Null(dt::Null::default())),
        },
        JsonDataType::String => sc::DataType {
            kind: Some(dt::Kind::String(dt::String::default())),
        },
        JsonDataType::Char { length } => sc::DataType {
            kind: Some(dt::Kind::Char(dt::Char {
                length,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Varchar { length } => sc::DataType {
            kind: Some(dt::Kind::VarChar(dt::VarChar {
                length,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Binary => sc::DataType {
            kind: Some(dt::Kind::Binary(dt::Binary::default())),
        },
        JsonDataType::Boolean => sc::DataType {
            kind: Some(dt::Kind::Boolean(dt::Boolean::default())),
        },
        JsonDataType::Decimal => sc::DataType {
            kind: Some(dt::Kind::Decimal(dt::Decimal::default())),
        },
        JsonDataType::FixedDecimal { precision, scale } => sc::DataType {
            kind: Some(dt::Kind::Decimal(dt::Decimal {
                precision: Some(precision),
                scale: Some(scale),
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Float => sc::DataType {
            kind: Some(dt::Kind::Float(dt::Float::default())),
        },
        JsonDataType::Double => sc::DataType {
            kind: Some(dt::Kind::Double(dt::Double::default())),
        },
        JsonDataType::Byte => sc::DataType {
            kind: Some(dt::Kind::Byte(dt::Byte::default())),
        },
        JsonDataType::Short => sc::DataType {
            kind: Some(dt::Kind::Short(dt::Short::default())),
        },
        JsonDataType::Integer => sc::DataType {
            kind: Some(dt::Kind::Integer(dt::Integer::default())),
        },
        JsonDataType::Long => sc::DataType {
            kind: Some(dt::Kind::Long(dt::Long::default())),
        },
        JsonDataType::Date => sc::DataType {
            kind: Some(dt::Kind::Date(dt::Date::default())),
        },
        JsonDataType::Time { precision } => sc::DataType {
            kind: Some(dt::Kind::Time(dt::Time {
                precision,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Timestamp => sc::DataType {
            kind: Some(dt::Kind::Timestamp(dt::Timestamp::default())),
        },
        JsonDataType::TimestampNtz => sc::DataType {
            kind: Some(dt::Kind::TimestampNtz(dt::TimestampNtz::default())),
        },
        JsonDataType::CalendarInterval => sc::DataType {
            kind: Some(dt::Kind::CalendarInterval(dt::CalendarInterval::default())),
        },
        JsonDataType::DayTimeInterval { start, end } => sc::DataType {
            kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                start_field: start.map(|f| f as i32),
                end_field: end.map(|f| f as i32),
                type_variation_reference: 0,
            })),
        },
        JsonDataType::YearMonthInterval { start, end } => sc::DataType {
            kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                start_field: start.map(|f| f as i32),
                end_field: end.map(|f| f as i32),
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Variant => sc::DataType {
            kind: Some(dt::Kind::Variant(dt::Variant::default())),
        },
        JsonDataType::Array {
            r#type: _,
            element_type,
            contains_null,
        } => sc::DataType {
            kind: Some(dt::Kind::Array(Box::new(dt::Array {
                element_type: Some(Box::new(from_spark_json_data_type(*element_type)?)),
                contains_null,
                type_variation_reference: 0,
            }))),
        },
        JsonDataType::Map {
            r#type: _,
            key_type,
            value_type,
            value_contains_null,
        } => sc::DataType {
            kind: Some(dt::Kind::Map(Box::new(dt::Map {
                key_type: Some(Box::new(from_spark_json_data_type(*key_type)?)),
                value_type: Some(Box::new(from_spark_json_data_type(*value_type)?)),
                value_contains_null,
                type_variation_reference: 0,
            }))),
        },
        JsonDataType::Struct { r#type: _, fields } => sc::DataType {
            kind: Some(dt::Kind::Struct(dt::Struct {
                fields: fields
                    .into_iter()
                    .map(|field| {
                        Ok(dt::StructField {
                            name: field.name,
                            data_type: Some(from_spark_json_data_type(field.r#type)?),
                            nullable: field.nullable,
                            metadata: field
                                .metadata
                                .map(|m| Ok(serde_json::to_string(&m)?) as SparkResult<_>)
                                .transpose()?,
                        })
                    })
                    .collect::<SparkResult<_>>()?,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Udt {
            r#type: _,
            class,
            py_class,
            serialized_class,
            sql_type,
        } => sc::DataType {
            kind: Some(dt::Kind::Udt(Box::new(dt::Udt {
                r#type: "udt".to_string(),
                jvm_class: class,
                python_class: py_class,
                serialized_python_class: serialized_class,
                sql_type: sql_type
                    .map(|t| Ok(Box::new(from_spark_json_data_type(*t)?)) as SparkResult<_>)
                    .transpose()?,
            }))),
        },
    };
    Ok(out)
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_spark_json_data_type() {
        let data = vec![
            (
                r#""null""#,
                sc::DataType {
                    kind: Some(dt::Kind::Null(dt::Null::default())),
                },
            ),
            (
                r#""void""#,
                sc::DataType {
                    kind: Some(dt::Kind::Null(dt::Null::default())),
                },
            ),
            (
                r#""string""#,
                sc::DataType {
                    kind: Some(dt::Kind::String(dt::String::default())),
                },
            ),
            (
                r#""char(10)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Char(dt::Char {
                        length: 10,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""varchar(20)""#,
                sc::DataType {
                    kind: Some(dt::Kind::VarChar(dt::VarChar {
                        length: 20,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""binary""#,
                sc::DataType {
                    kind: Some(dt::Kind::Binary(dt::Binary::default())),
                },
            ),
            (
                r#""boolean""#,
                sc::DataType {
                    kind: Some(dt::Kind::Boolean(dt::Boolean::default())),
                },
            ),
            (
                r#""decimal""#,
                sc::DataType {
                    kind: Some(dt::Kind::Decimal(dt::Decimal::default())),
                },
            ),
            (
                r#""decimal(10, 2)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Decimal(dt::Decimal {
                        precision: Some(10),
                        scale: Some(2),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""float""#,
                sc::DataType {
                    kind: Some(dt::Kind::Float(dt::Float::default())),
                },
            ),
            (
                r#""double""#,
                sc::DataType {
                    kind: Some(dt::Kind::Double(dt::Double::default())),
                },
            ),
            (
                r#""byte""#,
                sc::DataType {
                    kind: Some(dt::Kind::Byte(dt::Byte::default())),
                },
            ),
            (
                r#""short""#,
                sc::DataType {
                    kind: Some(dt::Kind::Short(dt::Short::default())),
                },
            ),
            (
                r#""integer""#,
                sc::DataType {
                    kind: Some(dt::Kind::Integer(dt::Integer::default())),
                },
            ),
            (
                r#""long""#,
                sc::DataType {
                    kind: Some(dt::Kind::Long(dt::Long::default())),
                },
            ),
            (
                r#""date""#,
                sc::DataType {
                    kind: Some(dt::Kind::Date(dt::Date::default())),
                },
            ),
            (
                r#""timestamp""#,
                sc::DataType {
                    kind: Some(dt::Kind::Timestamp(dt::Timestamp::default())),
                },
            ),
            (
                r#""timestamp_ltz""#,
                sc::DataType {
                    kind: Some(dt::Kind::Timestamp(dt::Timestamp::default())),
                },
            ),
            (
                r#""timestamp_ntz""#,
                sc::DataType {
                    kind: Some(dt::Kind::TimestampNtz(dt::TimestampNtz::default())),
                },
            ),
            (
                r#""interval""#,
                sc::DataType {
                    kind: Some(dt::Kind::CalendarInterval(dt::CalendarInterval::default())),
                },
            ),
            (
                r#""interval day""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval day to hour""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: Some(1),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval day to minute""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: Some(2),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval day to second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval hour""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(1),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval hour to minute""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(1),
                        end_field: Some(2),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval hour to second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(1),
                        end_field: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval minute""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(2),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval minute to second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(2),
                        end_field: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(3),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval year""#,
                sc::DataType {
                    kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                        start_field: Some(0),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval year to month""#,
                sc::DataType {
                    kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                        start_field: Some(0),
                        end_field: Some(1),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval month""#,
                sc::DataType {
                    kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                        start_field: Some(1),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""time""#,
                sc::DataType {
                    kind: Some(dt::Kind::Time(dt::Time {
                        precision: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""time(3)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Time(dt::Time {
                        precision: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""variant""#,
                sc::DataType {
                    kind: Some(dt::Kind::Variant(dt::Variant::default())),
                },
            ),
            (
                r#"{
                    "type": "array",
                    "elementType": "integer",
                    "containsNull": true
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Array(Box::new(dt::Array {
                        element_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                        contains_null: true,
                        type_variation_reference: 0,
                    }))),
                },
            ),
            (
                r#"{
                    "type": "map",
                    "keyType": "string",
                    "valueType": "integer",
                    "valueContainsNull": false
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Map(Box::new(dt::Map {
                        key_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::String(dt::String::default())),
                        })),
                        value_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                        value_contains_null: false,
                        type_variation_reference: 0,
                    }))),
                },
            ),
            (
                r#"{
                    "type": "struct",
                    "fields": [
                        {
                            "name": "name",
                            "type": "string",
                            "nullable": false,
                            "metadata": {}
                        },
                        {
                            "name": "age",
                            "type": {
                                "type": "array",
                                "elementType": "integer",
                                "containsNull": true
                            },
                            "nullable": true
                        }
                    ]
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Struct(dt::Struct {
                        fields: vec![
                            dt::StructField {
                                name: "name".to_string(),
                                data_type: Some(sc::DataType {
                                    kind: Some(dt::Kind::String(dt::String::default())),
                                }),
                                nullable: false,
                                metadata: Some("{}".to_string()),
                            },
                            dt::StructField {
                                name: "age".to_string(),
                                data_type: Some(sc::DataType {
                                    kind: Some(dt::Kind::Array(Box::new(dt::Array {
                                        element_type: Some(Box::new(sc::DataType {
                                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                                        })),
                                        contains_null: true,
                                        type_variation_reference: 0,
                                    }))),
                                }),
                                nullable: true,
                                metadata: None,
                            },
                        ],
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#"{
                    "type": "udt",
                    "class": "SomeClass",
                    "pyClass": "app.udf",
                    "sqlType": "integer"
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Udt(Box::new(dt::Udt {
                        r#type: "udt".to_string(),
                        jvm_class: Some("SomeClass".to_string()),
                        python_class: Some("app.udf".to_string()),
                        serialized_python_class: None,
                        sql_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                    }))),
                },
            ),
            (
                r#"{
                    "type": "udt",
                    "pyClass": "app.udf",
                    "serializedClass": "SomeSerializedClass",
                    "sqlType": "integer"
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Udt(Box::new(dt::Udt {
                        r#type: "udt".to_string(),
                        jvm_class: None,
                        python_class: Some("app.udf".to_string()),
                        serialized_python_class: Some("SomeSerializedClass".to_string()),
                        sql_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                    }))),
                },
            ),
        ];
        for (schema, expected) in data {
            // test serde roundtrip
            let d: JsonDataType = serde_json::from_str(schema).unwrap();
            let s = serde_json::to_string(&d).unwrap();
            assert_eq!(d, serde_json::from_str(&s).unwrap());
            // test data type conversion
            let data_type = parse_spark_json_data_type(schema).unwrap();
            assert_eq!(data_type, expected);
        }
    }
}
