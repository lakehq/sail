use arrow::datatypes::Fields;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_cast::cast;
use datafusion::arrow::datatypes as adt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::planner::SqlToRel;
use sqlparser::ast::{ColumnDef, Ident};

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::expression::EmptyContextProvider;
use crate::schema::json::parse_spark_json_data_type;
use crate::spark::connect as sc;
use crate::spark::connect::data_type as sdt;
use crate::sql::new_sql_parser;

static DEFAULT_FIELD_NAME: &str = "value";

pub(crate) enum SparkDataType {
    BuiltIn(adt::DataType),
    UserDefined {
        r#type: adt::DataType,
        jvm_class: Option<String>,
        python_class: Option<String>,
        serialized_python_class: Option<String>,
    },
}

pub(crate) fn parse_spark_schema_string(schema: &str) -> SparkResult<SchemaRef> {
    let mut parser = new_sql_parser(schema)?;
    if let Ok(dt) = parser.parse_spark_schema() {
        let provider = EmptyContextProvider::default();
        let planner = SqlToRel::new(&provider);
        Ok(Arc::new(planner.build_schema(dt)?))
    } else {
        let data_type = parse_spark_json_data_type(schema)?;
        let sc::DataType { ref kind } = data_type;
        let kind = kind.as_ref().required("data type kind")?;
        let fields = match kind {
            sdt::Kind::Struct(r#struct) => from_spark_fields(&r#struct.fields)?,
            _ => Fields::from(vec![from_spark_field(
                DEFAULT_FIELD_NAME,
                &data_type,
                true,
                None,
            )?]),
        };
        Ok(Arc::new(adt::Schema::new(fields)))
    }
}

pub(crate) fn parse_spark_data_type_string(dt: &str) -> SparkResult<adt::DataType> {
    let mut parser = new_sql_parser(dt)?;
    let dt = parser.parse_spark_data_type()?;
    let col = ColumnDef {
        name: Ident {
            value: "a".to_string(),
            quote_style: None,
        },
        data_type: dt,
        collation: None,
        options: vec![],
    };
    let provider = EmptyContextProvider::default();
    let planner = SqlToRel::new(&provider);
    let schema = planner.build_schema(vec![col])?;
    Ok(schema.field_with_name("a")?.data_type().clone())
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
            let metadata: HashMap<String, String> = if let Some(m) = &field.metadata {
                serde_json::from_str(m)?
            } else {
                HashMap::new()
            };
            let field = from_spark_field(name, data_type, field.nullable, Some(metadata))?;
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
            let r#type = match from_spark_data_type(sql_type)? {
                SparkDataType::BuiltIn(t) => t,
                SparkDataType::UserDefined { .. } => {
                    return Err(SparkError::invalid("user-defined type cannot be nested"));
                }
            };
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
