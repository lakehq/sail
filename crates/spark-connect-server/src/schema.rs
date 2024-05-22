use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_cast::cast;
use datafusion::arrow::datatypes as adt;
use datafusion::arrow::datatypes::Fields;
use datafusion::arrow::record_batch::RecordBatch;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect as sc;
use crate::spark::connect::data_type as sdt;

pub(crate) enum SparkDataType {
    BuiltIn(adt::DataType),
    UserDefined {
        r#type: adt::DataType,
        jvm_class: Option<String>,
        python_class: Option<String>,
        serialized_python_class: Option<String>,
    },
}

pub(crate) fn from_spark_field(
    name: impl Into<String>,
    data_type: &sc::DataType,
    nullable: bool,
    metadata: Option<HashMap<String, String>>,
) -> SparkResult<adt::Field> {
    let name = name.into();
    let data_type = from_spark_data_type(data_type)?;
    let mut metadata = match metadata {
        Some(m) => m
            .into_iter()
            .map(|(k, v)| (format!("metadata.{}", k), v))
            .collect(),
        None => HashMap::new(),
    };
    let data_type = match data_type {
        SparkDataType::BuiltIn(t) => t,
        SparkDataType::UserDefined {
            r#type,
            jvm_class,
            python_class,
            serialized_python_class,
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
            r#type
        }
    };
    Ok(adt::Field::new(name, data_type, nullable).with_metadata(metadata))
}

pub(crate) fn from_spark_fields(fields: &Vec<sdt::StructField>) -> SparkResult<Fields> {
    let fields = fields
        .iter()
        .map(|field| -> SparkResult<adt::Field> {
            let name = field.name.as_str();
            let data_type = field.data_type.as_ref().required("data type")?;
            let metadata = field
                .metadata
                .as_ref()
                .map(|m| serde_json::from_str(m))
                .transpose()?;
            let field = from_spark_field(name, data_type, field.nullable, metadata)?;
            Ok(field)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(adt::Fields::from(fields))
}

pub(crate) fn from_spark_built_in_data_type(
    data_type: &sc::DataType,
) -> SparkResult<adt::DataType> {
    match from_spark_data_type(data_type)? {
        SparkDataType::BuiltIn(t) => Ok(t),
        SparkDataType::UserDefined { .. } => Err(SparkError::invalid("user-defined type")),
    }
}

/// References:
///   org.apache.spark.sql.util.ArrowUtils#toArrowType
///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
pub(crate) fn from_spark_data_type(data_type: &sc::DataType) -> SparkResult<SparkDataType> {
    let sc::DataType { kind } = data_type;
    let kind = kind.as_ref().required("data type kind")?;
    match kind {
        sdt::Kind::Null(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Null)),
        sdt::Kind::Boolean(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Boolean)),
        sdt::Kind::Byte(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Int8)),
        sdt::Kind::Short(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Int16)),
        sdt::Kind::Integer(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Int32)),
        sdt::Kind::Long(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Int64)),
        sdt::Kind::Float(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Float32)),
        sdt::Kind::Double(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Float64)),
        sdt::Kind::Binary(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Binary)),
        sdt::Kind::String(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Utf8)),
        sdt::Kind::Array(array) => {
            let element_type = array.element_type.as_ref().required("array element type")?;
            let field = from_spark_field("element", element_type, array.contains_null, None)?;
            Ok(SparkDataType::BuiltIn(adt::DataType::List(Arc::new(field))))
        }
        sdt::Kind::Struct(r#struct) => {
            let fields = from_spark_fields(&r#struct.fields)?;
            Ok(SparkDataType::BuiltIn(adt::DataType::Struct(fields)))
        }
        sdt::Kind::Decimal(decimal) => {
            let precision = decimal.precision.required("decimal precision")?;
            let scale = decimal.scale.required("decimal scale")?;
            Ok(SparkDataType::BuiltIn(adt::DataType::Decimal128(
                precision as u8,
                scale as i8,
            )))
        }
        sdt::Kind::Char(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Utf8)),
        sdt::Kind::VarChar(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Utf8)),
        sdt::Kind::Date(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Date32)),
        sdt::Kind::Timestamp(_) => {
            // TODO: should we use "spark.sql.session.timeZone"?
            let timezone: Arc<str> = Arc::from("UTC");
            Ok(SparkDataType::BuiltIn(adt::DataType::Timestamp(
                adt::TimeUnit::Microsecond,
                Some(timezone),
            )))
        }
        sdt::Kind::TimestampNtz(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Timestamp(
            adt::TimeUnit::Microsecond,
            None,
        ))),
        sdt::Kind::CalendarInterval(_) => Err(SparkError::unsupported("calendar interval")),
        sdt::Kind::YearMonthInterval(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Interval(
            adt::IntervalUnit::YearMonth,
        ))),
        sdt::Kind::DayTimeInterval(_) => Ok(SparkDataType::BuiltIn(adt::DataType::Duration(
            adt::TimeUnit::Microsecond,
        ))),
        sdt::Kind::Map(map) => {
            let key_type = map.key_type.as_ref().required("map key type")?;
            let value_type = map.value_type.as_ref().required("map value type")?;
            let fields = vec![
                from_spark_field("key", key_type, false, None)?,
                from_spark_field("value", value_type, map.value_contains_null, None)?,
            ];
            Ok(SparkDataType::BuiltIn(adt::DataType::Map(
                Arc::new(adt::Field::new(
                    "entries",
                    adt::DataType::Struct(adt::Fields::from(fields)),
                    false,
                )),
                false,
            )))
        }
        sdt::Kind::Udt(udt) => {
            let sql_type = udt.sql_type.as_ref().required("UDT SQL type")?;
            let r#type = from_spark_built_in_data_type(sql_type)?;
            Ok(SparkDataType::UserDefined {
                r#type,
                jvm_class: udt.jvm_class.clone(),
                python_class: udt.python_class.clone(),
                serialized_python_class: udt.serialized_python_class.clone(),
            })
        }
        sdt::Kind::Unparsed(_) => Err(SparkError::todo("unparsed")),
    }
}

pub(crate) fn to_spark_schema(schema: adt::SchemaRef) -> SparkResult<sc::DataType> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| to_spark_field(f.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(sc::DataType {
        kind: Some(sdt::Kind::Struct(sdt::Struct {
            fields,
            type_variation_reference: 0,
        })),
    })
}

pub(crate) fn to_spark_field(field: adt::FieldRef) -> SparkResult<sdt::StructField> {
    let name = field.name();
    let is_udt = field.metadata().keys().any(|k| k.starts_with("udt."));
    let data_type = if is_udt {
        sc::DataType {
            kind: Some(sdt::Kind::Udt(Box::new(sdt::Udt {
                r#type: "udt".to_string(),
                jvm_class: field.metadata().get("udt.jvm_class").cloned(),
                python_class: field.metadata().get("udt.python_class").cloned(),
                serialized_python_class: field
                    .metadata()
                    .get("udt.serialized_python_class")
                    .cloned(),
                sql_type: Some(Box::new(to_spark_data_type(field.data_type())?)),
            }))),
        }
    } else {
        to_spark_data_type(field.data_type())?
    };
    let metadata = field
        .metadata()
        .iter()
        .filter(|(k, _)| k.starts_with("metadata."))
        .map(|(k, v)| (k["metadata.".len()..].to_string(), v.clone()))
        .collect::<HashMap<_, _>>();
    let metadata = serde_json::to_string(&metadata)?;
    Ok(sdt::StructField {
        name: name.to_string(),
        data_type: Some(data_type),
        nullable: field.is_nullable(),
        metadata: Some(metadata),
    })
}

pub(crate) fn to_spark_data_type(data_type: &adt::DataType) -> SparkResult<sc::DataType> {
    match data_type {
        adt::DataType::Null => Ok(sc::DataType {
            kind: Some(sdt::Kind::Null(sdt::Null::default())),
        }),
        adt::DataType::Boolean => Ok(sc::DataType {
            kind: Some(sdt::Kind::Boolean(sdt::Boolean::default())),
        }),
        adt::DataType::UInt8 | adt::DataType::Int8 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Byte(sdt::Byte::default())),
        }),
        adt::DataType::UInt16 | adt::DataType::Int16 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Short(sdt::Short::default())),
        }),
        adt::DataType::UInt32 | adt::DataType::Int32 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Integer(sdt::Integer::default())),
        }),
        adt::DataType::UInt64 | adt::DataType::Int64 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Long(sdt::Long::default())),
        }),
        adt::DataType::Float16 => Err(SparkError::unsupported("float16")),
        adt::DataType::Float32 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Float(sdt::Float::default())),
        }),
        adt::DataType::Float64 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Double(sdt::Double::default())),
        }),
        adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None) => Ok(sc::DataType {
            kind: Some(sdt::Kind::TimestampNtz(sdt::TimestampNtz::default())),
        }),
        adt::DataType::Timestamp(adt::TimeUnit::Microsecond, Some(_)) => Ok(sc::DataType {
            kind: Some(sdt::Kind::Timestamp(sdt::Timestamp::default())),
        }),
        adt::DataType::Timestamp(_, _) => Err(SparkError::unsupported("timestamp")),
        adt::DataType::Date32 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Date(sdt::Date::default())),
        }),
        adt::DataType::Date64 => Err(SparkError::unsupported("date64")),
        adt::DataType::Time32(_) => Err(SparkError::unsupported("time32")),
        adt::DataType::Time64(_) => Err(SparkError::unsupported("time64")),
        adt::DataType::Duration(adt::TimeUnit::Microsecond) => Ok(sc::DataType {
            kind: Some(sdt::Kind::DayTimeInterval(sdt::DayTimeInterval::default())),
        }),
        adt::DataType::Duration(_) => Err(SparkError::unsupported("duration")),
        adt::DataType::Interval(adt::IntervalUnit::YearMonth) => Ok(sc::DataType {
            kind: Some(sdt::Kind::YearMonthInterval(
                sdt::YearMonthInterval::default(),
            )),
        }),
        adt::DataType::Interval(_) => Err(SparkError::unsupported("interval")),
        adt::DataType::Binary | adt::DataType::FixedSizeBinary(_) | adt::DataType::LargeBinary => {
            Ok(sc::DataType {
                kind: Some(sdt::Kind::Binary(sdt::Binary::default())),
            })
        }
        adt::DataType::Utf8 | adt::DataType::LargeUtf8 => Ok(sc::DataType {
            kind: Some(sdt::Kind::String(sdt::String::default())),
        }),
        adt::DataType::List(field)
        | adt::DataType::FixedSizeList(field, _)
        | adt::DataType::LargeList(field) => {
            let field = to_spark_field(field.clone())?;
            Ok(sc::DataType {
                kind: Some(sdt::Kind::Array(Box::new(sdt::Array {
                    element_type: field.data_type.map(Box::new),
                    contains_null: field.nullable,
                    type_variation_reference: 0,
                }))),
            })
        }
        adt::DataType::Struct(fields) => Ok(sc::DataType {
            kind: Some(sdt::Kind::Struct(sdt::Struct {
                fields: fields
                    .iter()
                    .map(|f| to_spark_field(f.clone()))
                    .collect::<Result<Vec<_>, _>>()?,
                type_variation_reference: 0,
            })),
        }),
        adt::DataType::Union(_, _) => Err(SparkError::unsupported("union")),
        adt::DataType::Dictionary(_, _) => Err(SparkError::unsupported("dictionary")),
        adt::DataType::Decimal128(precision, scale) => Ok(sc::DataType {
            kind: Some(sdt::Kind::Decimal(sdt::Decimal {
                precision: Some(*precision as i32),
                scale: Some(*scale as i32),
                type_variation_reference: 0,
            })),
        }),
        adt::DataType::Decimal256(_, _) => Err(SparkError::unsupported("decimal256")),
        adt::DataType::Map(field, _sorted) => {
            let field = to_spark_field(field.clone())?;
            let fields = match field.data_type {
                Some(sc::DataType {
                    kind: Some(sdt::Kind::Struct(sdt::Struct { fields, .. })),
                }) => fields,
                _ => return Err(SparkError::invalid("invalid map data type")),
            };
            if fields.len() != 2 {
                return Err(SparkError::invalid(
                    "map data type must have key and value fields",
                ));
            }
            Ok(sc::DataType {
                kind: Some(sdt::Kind::Map(Box::new(sdt::Map {
                    key_type: fields[0].data_type.clone().map(Box::new),
                    value_type: fields[1].data_type.clone().map(Box::new),
                    value_contains_null: fields[1].nullable,
                    type_variation_reference: 0,
                }))),
            })
        }
        adt::DataType::RunEndEncoded(_, _) => Err(SparkError::unsupported("run end encoded")),
        adt::DataType::BinaryView => Err(SparkError::unsupported("BinaryView")),
        adt::DataType::Utf8View => Err(SparkError::unsupported("Utf8View")),
        adt::DataType::ListView(_) => Err(SparkError::unsupported("ListView")),
        adt::DataType::LargeListView(_) => Err(SparkError::unsupported("LargeListView")),
    }
}

pub(crate) fn cast_record_batch(
    batch: RecordBatch,
    schema: adt::SchemaRef,
) -> SparkResult<RecordBatch> {
    let fields = schema.fields();
    let columns = batch.columns();
    let columns = fields
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            let data_type = field.data_type();
            let column = cast(column, data_type)?;
            Ok(column)
        })
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

impl fmt::Display for sc::DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for sdt::Kind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl sc::DataType {
    pub fn to_simple_string(&self) -> SparkResult<String> {
        match &self.kind {
            Some(kind) => kind.to_simple_string(),
            None => Ok("none".to_string()),
        }
    }
}

impl sdt::Kind {
    pub fn to_simple_string(&self) -> SparkResult<String> {
        match self {
            sdt::Kind::Null(_) => Ok("void".to_string()),
            sdt::Kind::Binary(_) => Ok("binary".to_string()),
            sdt::Kind::Boolean(_) => Ok("boolean".to_string()),
            sdt::Kind::Byte(_) => Ok("tinyint".to_string()),
            sdt::Kind::Short(_) => Ok("smallint".to_string()),
            sdt::Kind::Integer(_) => Ok("int".to_string()),
            sdt::Kind::Long(_) => Ok("bigint".to_string()),
            sdt::Kind::Float(_) => Ok("float".to_string()),
            sdt::Kind::Double(_) => Ok("double".to_string()),
            sdt::Kind::Decimal(decimal) => Ok(format!(
                "decimal({},{})",
                decimal.precision.unwrap_or(10),
                decimal.scale.unwrap_or(0)
            )),
            sdt::Kind::String(_) => Ok("string".to_string()),
            sdt::Kind::Char(char) => Ok(format!("char({})", char.length)),
            sdt::Kind::VarChar(car_char) => Ok(format!("varchar({})", car_char.length)),
            sdt::Kind::Date(_) => Ok("date".to_string()),
            sdt::Kind::Timestamp(_) => Ok("timestamp".to_string()),
            sdt::Kind::TimestampNtz(_) => Ok("timestamp_ntz".to_string()),
            sdt::Kind::CalendarInterval(_) => Ok("interval".to_string()),
            sdt::Kind::YearMonthInterval(interval) => {
                let (start_field, end_field) = match (interval.start_field, interval.end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(end)) => (end, end), // Should not occur, but let's handle it
                    (None, None) => (0, 1),          // Default to YEAR and MONTH
                };

                let start_field_name = match start_field {
                    0 => "year",
                    1 => "month",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid year-month start_field value: {}",
                            start_field
                        )))
                    }
                };
                let end_field_name = match end_field {
                    0 => "year",
                    1 => "month",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid year-month end_field value: {}",
                            end_field
                        )))
                    }
                };

                if start_field_name == end_field_name {
                    Ok(format!("interval {}", start_field_name))
                } else if start_field < end_field {
                    Ok(format!(
                        "interval {} to {}",
                        start_field_name, end_field_name
                    ))
                } else {
                    Err(SparkError::invalid(format!(
                        "Invalid year-month interval: start_field={}, end_field={}",
                        start_field, end_field
                    )))
                }
            }
            sdt::Kind::DayTimeInterval(interval) => {
                let (start_field, end_field) = match (interval.start_field, interval.end_field) {
                    (Some(start), Some(end)) => (start, end),
                    (Some(start), None) => (start, start),
                    (None, Some(end)) => (end, end), // Should not occur, but let's handle it
                    (None, None) => (0, 3),          // Default to DAY and SECOND
                };

                let start_field_name = match start_field {
                    0 => "day",
                    1 => "hour",
                    2 => "minute",
                    3 => "second",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid day-time start_field value: {}",
                            start_field
                        )))
                    }
                };
                let end_field_name = match end_field {
                    0 => "day",
                    1 => "hour",
                    2 => "minute",
                    3 => "second",
                    _ => {
                        return Err(SparkError::invalid(format!(
                            "Invalid day-time end_field value: {}",
                            end_field
                        )))
                    }
                };

                if start_field_name == end_field_name {
                    Ok(format!("interval {}", start_field_name))
                } else if start_field < end_field {
                    Ok(format!(
                        "interval {} to {}",
                        start_field_name, end_field_name
                    ))
                } else {
                    Err(SparkError::invalid(format!(
                        "Invalid day-time interval: start_field={}, end_field={}",
                        start_field, end_field
                    )))
                }
            }
            sdt::Kind::Array(array) => {
                let element_type = array.element_type.as_ref().required("array element type")?;
                Ok(format!("array<{}>", element_type.to_simple_string()?))
            }
            sdt::Kind::Struct(_) => Ok("struct".to_string()),
            sdt::Kind::Map(map) => {
                let key_type = map.key_type.as_ref().required("map key type")?;
                let value_type = map.value_type.as_ref().required("map value type")?;
                Ok(format!(
                    "map<{},{}>",
                    key_type.to_simple_string()?,
                    value_type.to_simple_string()?
                ))
            }
            sdt::Kind::Udt(udt) => {
                let sql_type = udt.sql_type.as_ref().required("UDT SQL type")?;
                Ok(sql_type.to_simple_string()?)
            }
            sdt::Kind::Unparsed(_) => Err(SparkError::invalid("to_simple_string for unparsed")),
        }
    }
}
