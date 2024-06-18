use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    Decimal {
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
    Timestamp,
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
                metadata: HashMap::new(),
            }]),
        };
        Schema { fields }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DayTimeIntervalField {
    Day = 0,
    Hour = 1,
    Minute = 2,
    Second = 3,
}

impl TryFrom<i32> for DayTimeIntervalField {
    type Error = CommonError;

    fn try_from(value: i32) -> CommonResult<Self> {
        match value {
            0 => Ok(DayTimeIntervalField::Day),
            1 => Ok(DayTimeIntervalField::Hour),
            2 => Ok(DayTimeIntervalField::Minute),
            3 => Ok(DayTimeIntervalField::Second),
            _ => Err(CommonError::invalid(format!(
                "day time interval field: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum YearMonthIntervalField {
    Year = 0,
    Month = 1,
}

impl TryFrom<i32> for YearMonthIntervalField {
    type Error = CommonError;

    fn try_from(value: i32) -> CommonResult<Self> {
        match value {
            0 => Ok(YearMonthIntervalField::Year),
            1 => Ok(YearMonthIntervalField::Month),
            _ => Err(CommonError::invalid(format!(
                "year month interval field: {}",
                value
            ))),
        }
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
