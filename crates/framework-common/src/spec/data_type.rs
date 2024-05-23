use crate::error::{CommonError, CommonResult};
use datafusion::arrow::datatypes as adt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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
    Unparsed(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fields(Vec<Field>);

impl Fields {
    pub fn new(fields: Vec<Field>) -> Self {
        Fields(fields)
    }

    pub fn empty() -> Self {
        Fields(Vec::new())
    }
}

impl Into<Vec<Field>> for Fields {
    fn into(self) -> Vec<Field> {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl TryFrom<Field> for adt::Field {
    type Error = CommonError;

    fn try_from(field: Field) -> CommonResult<adt::Field> {
        let Field {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let mut metadata = match metadata {
            Some(m) => m
                .into_iter()
                .map(|(k, v)| (format!("metadata.{}", k), v))
                .collect(),
            None => HashMap::new(),
        };
        let data_type = match data_type {
            DataType::UserDefined {
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type,
            } => {
                if let Some(jvm_class) = jvm_class {
                    metadata.insert("udt.jvm_class".to_string(), jvm_class);
                }
                if let Some(python_class) = python_class {
                    metadata.insert("udt.python_class".to_string(), python_class);
                }
                if let Some(serialized_python_class) = serialized_python_class {
                    metadata.insert(
                        "udt.serialized_python_class".to_string(),
                        serialized_python_class,
                    );
                }
                *sql_type
            }
            x => x,
        };
        Ok(adt::Field::new(name, data_type.try_into()?, nullable).with_metadata(metadata))
    }
}

impl TryFrom<adt::FieldRef> for Field {
    type Error = CommonError;

    fn try_from(field: adt::FieldRef) -> CommonResult<Field> {
        let name = field.name();
        let is_udt = field.metadata().keys().any(|k| k.starts_with("udt."));
        let data_type = if is_udt {
            DataType::UserDefined {
                jvm_class: field.metadata().get("udt.jvm_class").cloned(),
                python_class: field.metadata().get("udt.python_class").cloned(),
                serialized_python_class: field
                    .metadata()
                    .get("udt.serialized_python_class")
                    .cloned(),
                sql_type: Box::new(field.data_type().clone().try_into()?),
            }
        } else {
            field.data_type().clone().try_into()?
        };
        let metadata = field
            .metadata()
            .iter()
            .filter(|(k, _)| k.starts_with("metadata."))
            .map(|(k, v)| (k["metadata.".len()..].to_string(), v.clone()))
            .collect::<HashMap<_, _>>();
        Ok(Field {
            name: name.clone(),
            data_type,
            nullable: field.is_nullable(),
            metadata: Some(metadata),
        })
    }
}

impl TryFrom<Fields> for adt::Fields {
    type Error = CommonError;

    fn try_from(fields: Fields) -> CommonResult<adt::Fields> {
        let fields = fields
            .0
            .into_iter()
            .map(adt::Field::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(adt::Fields::from(fields))
    }
}

impl TryFrom<adt::Fields> for Fields {
    type Error = CommonError;

    fn try_from(fields: adt::Fields) -> CommonResult<Fields> {
        let fields = fields
            .iter()
            .map(|f| Ok(f.clone().try_into()?))
            .collect::<CommonResult<Vec<_>>>()?;
        Ok(Fields(fields))
    }
}

/// References:
///   org.apache.spark.sql.util.ArrowUtils#toArrowType
///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
impl TryFrom<DataType> for adt::DataType {
    type Error = CommonError;

    fn try_from(data_type: DataType) -> CommonResult<adt::DataType> {
        match data_type {
            DataType::Null => Ok(adt::DataType::Null),
            DataType::Boolean => Ok(adt::DataType::Boolean),
            DataType::Byte => Ok(adt::DataType::Int8),
            DataType::Short => Ok(adt::DataType::Int16),
            DataType::Integer => Ok(adt::DataType::Int32),
            DataType::Long => Ok(adt::DataType::Int64),
            DataType::Float => Ok(adt::DataType::Float32),
            DataType::Double => Ok(adt::DataType::Float64),
            DataType::Binary => Ok(adt::DataType::Binary),
            DataType::String => Ok(adt::DataType::Utf8),
            DataType::Array {
                element_type,
                contains_null,
            } => {
                let field = Field {
                    name: "element".to_string(),
                    data_type: *element_type,
                    nullable: contains_null,
                    metadata: None,
                };
                Ok(adt::DataType::List(Arc::new(field.try_into()?)))
            }
            DataType::Struct { fields } => Ok(adt::DataType::Struct(fields.try_into()?)),
            DataType::Decimal { scale, precision } => {
                Ok(adt::DataType::Decimal128(precision as u8, scale as i8))
            }
            DataType::Char { .. } => Ok(adt::DataType::Utf8),
            DataType::VarChar { .. } => Ok(adt::DataType::Utf8),
            DataType::Date => Ok(adt::DataType::Date32),
            DataType::Timestamp => {
                // TODO: should we use "spark.sql.session.timeZone"?
                let timezone: Arc<str> = Arc::from("UTC");
                Ok(adt::DataType::Timestamp(
                    adt::TimeUnit::Microsecond,
                    Some(timezone),
                ))
            }
            DataType::TimestampNtz => {
                Ok(adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None))
            }
            DataType::CalendarInterval => {
                Ok(adt::DataType::Interval(adt::IntervalUnit::MonthDayNano))
            }
            DataType::YearMonthInterval { .. } => {
                Ok(adt::DataType::Interval(adt::IntervalUnit::YearMonth))
            }
            DataType::DayTimeInterval { .. } => {
                Ok(adt::DataType::Duration(adt::TimeUnit::Microsecond))
            }
            DataType::Map {
                key_type,
                value_type,
                value_contains_null,
            } => {
                let fields = vec![
                    Field {
                        name: "key".to_string(),
                        data_type: *key_type,
                        nullable: false,
                        metadata: None,
                    },
                    Field {
                        name: "value".to_string(),
                        data_type: *value_type,
                        nullable: value_contains_null,
                        metadata: None,
                    },
                ];
                Ok(adt::DataType::Map(
                    Arc::new(adt::Field::new(
                        "entries",
                        adt::DataType::Struct(Fields(fields).try_into()?),
                        false,
                    )),
                    false,
                ))
            }
            DataType::UserDefined { .. } => Err(CommonError::unsupported(
                "user defined data type should only exist in a field",
            )),
            DataType::Unparsed(_) => Err(CommonError::unsupported("unparsed")),
        }
    }
}

impl TryFrom<adt::DataType> for DataType {
    type Error = CommonError;

    fn try_from(data_type: adt::DataType) -> CommonResult<DataType> {
        match data_type {
            adt::DataType::Null => Ok(DataType::Null),
            adt::DataType::Boolean => Ok(DataType::Boolean),
            adt::DataType::UInt8 | adt::DataType::Int8 => Ok(DataType::Byte),
            adt::DataType::UInt16 | adt::DataType::Int16 => Ok(DataType::Short),
            adt::DataType::UInt32 | adt::DataType::Int32 => Ok(DataType::Integer),
            adt::DataType::UInt64 | adt::DataType::Int64 => Ok(DataType::Long),
            adt::DataType::Float16 => Err(CommonError::unsupported("float16")),
            adt::DataType::Float32 => Ok(DataType::Float),
            adt::DataType::Float64 => Ok(DataType::Double),
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None) => {
                Ok(DataType::TimestampNtz)
            }
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, Some(_)) => {
                Ok(DataType::Timestamp)
            }
            adt::DataType::Timestamp(_, _) => Err(CommonError::unsupported("timestamp")),
            adt::DataType::Date32 => Ok(DataType::Date),
            adt::DataType::Date64 => Err(CommonError::unsupported("date64")),
            adt::DataType::Time32(_) => Err(CommonError::unsupported("time32")),
            adt::DataType::Time64(_) => Err(CommonError::unsupported("time64")),
            adt::DataType::Duration(adt::TimeUnit::Microsecond) => Ok(DataType::DayTimeInterval {
                start_field: Some(DayTimeIntervalField::Day),
                end_field: Some(DayTimeIntervalField::Second),
            }),
            adt::DataType::Duration(_) => Err(CommonError::unsupported("duration")),
            adt::DataType::Interval(adt::IntervalUnit::YearMonth) => {
                Ok(DataType::YearMonthInterval {
                    start_field: Some(YearMonthIntervalField::Year),
                    end_field: Some(YearMonthIntervalField::Month),
                })
            }
            adt::DataType::Interval(adt::IntervalUnit::MonthDayNano) => {
                Ok(DataType::CalendarInterval)
            }
            adt::DataType::Interval(_) => Err(CommonError::unsupported("interval")),
            adt::DataType::Binary
            | adt::DataType::FixedSizeBinary(_)
            | adt::DataType::LargeBinary => Ok(DataType::Binary),
            adt::DataType::Utf8 | adt::DataType::LargeUtf8 => Ok(DataType::String),
            adt::DataType::List(field)
            | adt::DataType::FixedSizeList(field, _)
            | adt::DataType::LargeList(field) => {
                let field = Field::try_from(field.clone())?;
                Ok(DataType::Array {
                    element_type: Box::new(field.data_type),
                    contains_null: field.nullable,
                })
            }
            adt::DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| Field::try_from(field.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataType::Struct {
                    fields: Fields(fields),
                })
            }
            adt::DataType::Union(_, _) => Err(CommonError::unsupported("union")),
            adt::DataType::Dictionary(_, _) => Err(CommonError::unsupported("dictionary")),
            adt::DataType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal { precision, scale })
            }
            adt::DataType::Decimal256(_, _) => Err(CommonError::unsupported("decimal256")),
            adt::DataType::Map(field, _sorted) => {
                let field: Field = field.clone().try_into()?;
                let fields = match field.data_type {
                    DataType::Struct { fields } => fields,
                    _ => return Err(CommonError::invalid("invalid map data type")),
                };
                if fields.0.len() != 2 {
                    return Err(CommonError::invalid(
                        "map data type must have key and value fields",
                    ));
                }
                Ok(DataType::Map {
                    key_type: Box::new(fields.0[0].data_type.clone()),
                    value_type: Box::new(fields.0[1].data_type.clone()),
                    value_contains_null: fields.0[1].nullable,
                })
            }
            adt::DataType::RunEndEncoded(_, _) => Err(CommonError::unsupported("run end encoded")),
            adt::DataType::BinaryView => Err(CommonError::unsupported("BinaryView")),
            adt::DataType::Utf8View => Err(CommonError::unsupported("Utf8View")),
            adt::DataType::ListView(_) => Err(CommonError::unsupported("ListView")),
            adt::DataType::LargeListView(_) => Err(CommonError::unsupported("LargeListView")),
        }
    }
}
