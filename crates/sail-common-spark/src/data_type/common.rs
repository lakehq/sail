use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use monostate::MustBe;
use serde::{Deserialize, Serialize};

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
    #[serde(alias = "tinyint")]
    Byte,
    #[serde(alias = "smallint")]
    Short,
    #[serde(alias = "int")]
    Integer,
    #[serde(alias = "bigint")]
    Long,
    Date,
    #[serde(alias = "timestamp_ltz")]
    Timestamp,
    TimestampNtz,
    #[serde(rename = "timestamp_ltz(9)")]
    TimestampLtzNanos,
    #[serde(rename = "timestamp_ntz(9)")]
    TimestampNtzNanos,
    #[serde(rename = "interval")]
    CalendarInterval,
    Variant,
    #[serde(untagged, with = "serde_geometry")]
    Geometry {
        srid: i32,
    },
    #[serde(untagged, with = "serde_geography")]
    Geography {
        srid: i32,
        algorithm: String,
    },
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
        #[serde(default = "default_nullable")]
        contains_null: bool,
    },
    #[serde(untagged, rename_all = "camelCase")]
    Map {
        r#type: MustBe!("map"),
        key_type: Box<JsonDataType>,
        value_type: Box<JsonDataType>,
        #[serde(default = "default_nullable")]
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
        #[serde(alias = "sql_type")]
        sql_type: Option<Box<JsonDataType>>,
    },
    /// The object form of a simple type, such as `{"type": "integer"}`.
    #[serde(untagged)]
    Type {
        r#type: Box<JsonDataType>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct JsonStructField {
    pub name: String,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    pub r#type: JsonDataType,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
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

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        crate::data_type::parser::parse(
            input,
            crate::data_type::parser::create_day_time_interval_field_parser(),
        )
        .map_err(serde::de::Error::custom)
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

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        crate::data_type::parser::parse(
            input,
            crate::data_type::parser::create_year_month_interval_field_parser(),
        )
        .map_err(serde::de::Error::custom)
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

mod serde_time {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

    pub fn serialize<S>(precision: &Option<i32>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match precision {
            Some(precision) => format!("time({})", *precision).serialize(serializer),
            None => "time".to_string().serialize(serializer),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        parser::parse(&input, parser::create_time_parser()).map_err(serde::de::Error::custom)
    }
}

mod serde_char {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

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
        let input = String::deserialize(deserializer)?;
        parser::parse(&input, parser::create_character_parser("char"))
            .map_err(serde::de::Error::custom)
    }
}

mod serde_varchar {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

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
        let input = String::deserialize(deserializer)?;
        parser::parse(&input, parser::create_character_parser("varchar"))
            .map_err(serde::de::Error::custom)
    }
}

mod serde_fixed_decimal {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

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
        let input = String::deserialize(deserializer)?;
        parser::parse(&input, parser::create_decimal_parser()).map_err(serde::de::Error::custom)
    }
}

mod serde_geometry {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

    pub fn serialize<S>(srid: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if *srid == -1 {
            "geometry(ANY)".serialize(serializer)
        } else {
            format!("geometry({})", *srid).serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        parser::parse(&input, parser::create_geometry_parser()).map_err(serde::de::Error::custom)
    }
}

mod serde_geography {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

    pub fn serialize<S>(srid: &i32, algorithm: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if *srid == -1 {
            format!("geography(ANY, {})", algorithm).serialize(serializer)
        } else {
            format!("geography({}, {})", *srid, algorithm).serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<(i32, String), D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        parser::parse(&input, parser::create_geography_parser()).map_err(serde::de::Error::custom)
    }
}

mod serde_day_time_interval {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

    pub fn serialize<S>(
        start: &Option<super::DayTimeIntervalField>,
        end: &Option<super::DayTimeIntervalField>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let start = start.as_ref().map(|field| field.to_string());
        let end = end.as_ref().map(|field| field.to_string());
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
        let input = String::deserialize(deserializer)?;
        parser::parse(
            &input,
            parser::create_interval_parser(parser::create_day_time_interval_field_parser()),
        )
        .map_err(serde::de::Error::custom)
    }
}

mod serde_year_month_interval {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::data_type::parser;

    pub fn serialize<S>(
        start: &Option<super::YearMonthIntervalField>,
        end: &Option<super::YearMonthIntervalField>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let start = start.as_ref().map(|field| field.to_string());
        let end = end.as_ref().map(|field| field.to_string());
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
        let input = String::deserialize(deserializer)?;
        parser::parse(
            &input,
            parser::create_interval_parser(parser::create_year_month_interval_field_parser()),
        )
        .map_err(serde::de::Error::custom)
    }
}

fn default_nullable() -> bool {
    true
}
