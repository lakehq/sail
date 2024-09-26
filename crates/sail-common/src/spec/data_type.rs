use std::sync::Arc;

use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

use crate::error::CommonError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DataType {
    Null,
    Binary,
    Boolean,
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    String,
    Char {
        length: u32,
    },
    VarChar {
        length: u32,
    },
    Date,
    Timestamp(Option<TimeUnit>, Option<Arc<str>>),
    TimestampNtz,
    CalendarInterval,
    YearMonthInterval {
        start_field: Option<YearMonthIntervalField>,
        end_field: Option<YearMonthIntervalField>,
    },
    DayTimeInterval {
        start_field: Option<DayTimeIntervalField>,
        end_field: Option<DayTimeIntervalField>,
    },
    Array {
        element_type: Box<DataType>,
        contains_null: bool,
    },
    Struct {
        fields: Fields,
    },
    Map {
        key_type: Box<DataType>,
        value_type: Box<DataType>,
        value_contains_null: bool,
    },
    UserDefined {
        jvm_class: Option<String>,
        python_class: Option<String>,
        serialized_python_class: Option<String>,
        sql_type: Box<DataType>,
    },
}

impl DataType {
    pub fn into_schema(self, default_field_name: &str, nullable: bool) -> Schema {
        let fields = match self {
            DataType::Struct { fields } => fields,
            x => Fields::new(vec![Field {
                name: default_field_name.to_string(),
                data_type: x,
                nullable,
                metadata: vec![],
            }]),
        };
        Schema { fields }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Fields(pub Vec<Field>);

impl Fields {
    pub fn new(fields: Vec<Field>) -> Self {
        Fields(fields)
    }

    pub fn empty() -> Self {
        Fields(Vec::new())
    }
}

impl From<Fields> for Vec<Field> {
    fn from(fields: Fields) -> Vec<Field> {
        fields.0
    }
}

impl From<Vec<Field>> for Fields {
    fn from(fields: Vec<Field>) -> Fields {
        Fields(fields)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = DayTimeIntervalField::invalid))]
#[repr(i32)]
pub enum DayTimeIntervalField {
    Day = 0,
    Hour = 1,
    Minute = 2,
    Second = 3,
}

impl DayTimeIntervalField {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("day time interval field: {value}"))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = YearMonthIntervalField::invalid))]
#[repr(i32)]
pub enum YearMonthIntervalField {
    Year = 0,
    Month = 1,
}

impl YearMonthIntervalField {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("year month interval field: {value}"))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Fields,
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            fields: Fields::empty(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}
