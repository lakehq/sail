use std::sync::Arc;

use datafusion::arrow::datatypes as adt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{ColumnDef, Ident};

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::expression::EmptyContextProvider;
use crate::schema::json::parse_spark_json_data_type;
use crate::spark::connect as sc;
use crate::spark::connect::data_type as sdt;
use crate::sql::new_sql_parser;

static DEFAULT_FIELD_NAME: &str = "value";

pub(crate) fn parse_spark_schema_string(schema: &str) -> SparkResult<SchemaRef> {
    let mut parser = new_sql_parser(schema)?;
    if let Ok(dt) = parser.parse_spark_schema() {
        let provider = EmptyContextProvider::default();
        let planner = SqlToRel::new(&provider);
        Ok(Arc::new(planner.build_schema(dt)?))
    } else {
        let data_type = parse_spark_json_data_type(schema)?;
        let data_type = from_spark_data_type(&data_type)?;
        match data_type {
            adt::DataType::Struct(fields) => Ok(Arc::new(adt::Schema::new(fields))),
            other => Ok(Arc::new(adt::Schema::new(vec![adt::Field::new(
                DEFAULT_FIELD_NAME,
                other,
                true,
            )]))),
        }
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

/// References:
///   org.apache.spark.sql.util.ArrowUtils#toArrowType
///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
pub(crate) fn from_spark_data_type(data_type: &sc::DataType) -> SparkResult<adt::DataType> {
    let sc::DataType { kind } = data_type;
    let kind = kind.as_ref().required("data type kind")?;
    match kind {
        sdt::Kind::Null(_) => Ok(adt::DataType::Null),
        sdt::Kind::Boolean(_) => Ok(adt::DataType::Boolean),
        sdt::Kind::Byte(_) => Ok(adt::DataType::Int8),
        sdt::Kind::Short(_) => Ok(adt::DataType::Int16),
        sdt::Kind::Integer(_) => Ok(adt::DataType::Int32),
        sdt::Kind::Long(_) => Ok(adt::DataType::Int64),
        sdt::Kind::Float(_) => Ok(adt::DataType::Float32),
        sdt::Kind::Double(_) => Ok(adt::DataType::Float64),
        sdt::Kind::Binary(_) => Ok(adt::DataType::Binary),
        sdt::Kind::String(_) => Ok(adt::DataType::Utf8),
        sdt::Kind::Array(array) => {
            let element_type = array.element_type.as_ref().required("array element type")?;
            let element_type = from_spark_data_type(element_type)?;
            Ok(adt::DataType::List(Arc::new(adt::Field::new(
                "element",
                element_type,
                array.contains_null,
            ))))
        }
        sdt::Kind::Struct(r#struct) => {
            let fields = r#struct
                .fields
                .iter()
                .map(|field| -> SparkResult<adt::Field> {
                    let name = field.name.as_str();
                    let data_type = field.data_type.as_ref().required("data type")?;
                    let data_type = from_spark_data_type(data_type)?;
                    Ok(adt::Field::new(name, data_type, field.nullable))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(adt::DataType::Struct(adt::Fields::from(fields)))
        }
        sdt::Kind::Decimal(decimal) => {
            let precision = decimal.precision.required("decimal precision")?;
            let scale = decimal.scale.required("decimal scale")?;
            Ok(adt::DataType::Decimal128(precision as u8, scale as i8))
        }
        sdt::Kind::Char(_) => Ok(adt::DataType::Utf8),
        sdt::Kind::VarChar(_) => Ok(adt::DataType::Utf8),
        sdt::Kind::Date(_) => Ok(adt::DataType::Date32),
        sdt::Kind::Timestamp(_) => Ok(adt::DataType::Timestamp(
            adt::TimeUnit::Microsecond,
            // TODO: should we use "spark.sql.session.timeZone"?
            None,
        )),
        sdt::Kind::TimestampNtz(_) => {
            Ok(adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None))
        }
        sdt::Kind::CalendarInterval(_) => Err(SparkError::unsupported("calendar interval")),
        sdt::Kind::YearMonthInterval(_) => {
            Ok(adt::DataType::Interval(adt::IntervalUnit::YearMonth))
        }
        sdt::Kind::DayTimeInterval(_) => Ok(adt::DataType::Duration(adt::TimeUnit::Microsecond)),
        sdt::Kind::Map(map) => {
            let key_type = map.key_type.as_ref().required("map key type")?;
            let value_type = map.value_type.as_ref().required("map value type")?;
            let fields = vec![
                adt::Field::new("key", from_spark_data_type(key_type)?, false),
                adt::Field::new(
                    "value",
                    from_spark_data_type(value_type)?,
                    map.value_contains_null,
                ),
            ];
            Ok(adt::DataType::Map(
                Arc::new(adt::Field::new(
                    "entries",
                    adt::DataType::Struct(adt::Fields::from(fields)),
                    false,
                )),
                false,
            ))
        }
        sdt::Kind::Udt(_) => Err(SparkError::todo("udt")),
        sdt::Kind::Unparsed(_) => Err(SparkError::todo("unparsed")),
    }
}

pub(crate) fn to_spark_schema(schema: adt::SchemaRef) -> SparkResult<sc::DataType> {
    to_spark_data_type(&adt::DataType::Struct(schema.fields().clone()))
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
        adt::DataType::List(f)
        | adt::DataType::FixedSizeList(f, _)
        | adt::DataType::LargeList(f) => Ok(sc::DataType {
            kind: Some(sdt::Kind::Array(Box::new(sdt::Array {
                element_type: Some(Box::new(to_spark_data_type(f.data_type())?)),
                contains_null: f.is_nullable(),
                type_variation_reference: 0,
            }))),
        }),
        adt::DataType::Struct(fields) => Ok(sc::DataType {
            kind: Some(sdt::Kind::Struct(sdt::Struct {
                fields: fields
                    .iter()
                    .map(|field| -> SparkResult<sdt::StructField> {
                        let name = field.name();
                        let data_type = to_spark_data_type(field.data_type())?;
                        Ok(sdt::StructField {
                            name: name.to_string(),
                            data_type: Some(data_type),
                            nullable: field.is_nullable(),
                            metadata: None,
                        })
                    })
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
            let (key_type, value_type, value_contains_null) = match field.data_type() {
                adt::DataType::Struct(s) => {
                    if let [key_field, value_field] = s.iter().as_slice() {
                        (
                            key_field.data_type(),
                            value_field.data_type(),
                            value_field.is_nullable(),
                        )
                    } else {
                        return Err(SparkError::invalid("map data type"));
                    }
                }
                _ => return Err(SparkError::invalid("map data type")),
            };
            Ok(sc::DataType {
                kind: Some(sdt::Kind::Map(Box::new(sdt::Map {
                    key_type: Some(Box::new(to_spark_data_type(key_type)?)),
                    value_type: Some(Box::new(to_spark_data_type(value_type)?)),
                    value_contains_null,
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
