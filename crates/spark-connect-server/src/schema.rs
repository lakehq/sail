use crate::error::{ProtoFieldExt, SparkError};
use crate::spark::connect as sc;
use crate::spark::connect::data_type as sdt;
use datafusion::arrow::datatypes as adt;
use std::sync::Arc;

pub(crate) fn from_spark_data_type(data_type: &sc::DataType) -> Result<adt::DataType, SparkError> {
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
        sdt::Kind::Struct(struct_type) => {
            let fields = struct_type
                .fields
                .iter()
                .map(|field| -> Result<adt::Field, SparkError> {
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
            Ok(adt::DataType::Decimal256(precision as u8, scale as i8))
        }
        sdt::Kind::Char(_) => {
            todo!()
        }
        sdt::Kind::VarChar(_) => {
            todo!()
        }
        sdt::Kind::Date(_) => {
            todo!()
        }
        sdt::Kind::Timestamp(_) => {
            todo!()
        }
        sdt::Kind::TimestampNtz(_) => {
            todo!()
        }
        sdt::Kind::CalendarInterval(_) => {
            todo!()
        }
        sdt::Kind::YearMonthInterval(_) => {
            todo!()
        }
        sdt::Kind::DayTimeInterval(_) => {
            todo!()
        }
        sdt::Kind::Map(_) => {
            todo!()
        }
        sdt::Kind::Udt(_) => {
            todo!()
        }
        sdt::Kind::Unparsed(_) => {
            todo!()
        }
    }
}

pub(crate) fn to_spark_schema(schema: adt::SchemaRef) -> Result<sc::DataType, SparkError> {
    to_spark_data_type(&adt::DataType::Struct(schema.fields().clone()))
}

pub(crate) fn to_spark_data_type(data_type: &adt::DataType) -> Result<sc::DataType, SparkError> {
    match data_type {
        adt::DataType::Null => Ok(sc::DataType {
            kind: Some(sdt::Kind::Null(sdt::Null::default())),
        }),
        adt::DataType::Boolean => Ok(sc::DataType {
            kind: Some(sdt::Kind::Boolean(sdt::Boolean::default())),
        }),
        adt::DataType::Int8 | adt::DataType::UInt8 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Byte(sdt::Byte::default())),
        }),
        adt::DataType::Int16 | adt::DataType::UInt16 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Short(sdt::Short::default())),
        }),
        adt::DataType::Int32 | adt::DataType::UInt32 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Integer(sdt::Integer::default())),
        }),
        adt::DataType::Int64 | adt::DataType::UInt64 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Long(sdt::Long::default())),
        }),
        adt::DataType::Float16 | adt::DataType::Float32 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Float(sdt::Float::default())),
        }),
        adt::DataType::Float64 => Ok(sc::DataType {
            kind: Some(sdt::Kind::Double(sdt::Double::default())),
        }),
        adt::DataType::Timestamp(_, _) => Err(SparkError::todo("timestamp")),
        adt::DataType::Date32 => Err(SparkError::todo("date32")),
        adt::DataType::Date64 => Err(SparkError::todo("date64")),
        adt::DataType::Time32(_) => Err(SparkError::todo("time32")),
        adt::DataType::Time64(_) => Err(SparkError::todo("time64")),
        adt::DataType::Duration(_) => Err(SparkError::todo("duration")),
        adt::DataType::Interval(_) => Err(SparkError::todo("interval")),
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
                    .map(|field| -> Result<sdt::StructField, SparkError> {
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
        adt::DataType::Union(_, _) => Err(SparkError::todo("union")),
        adt::DataType::Dictionary(_, _) => Err(SparkError::todo("dictionary")),
        adt::DataType::Decimal128(precision, scale)
        | adt::DataType::Decimal256(precision, scale) => Ok(sc::DataType {
            kind: Some(sdt::Kind::Decimal(sdt::Decimal {
                precision: Some(*precision as i32),
                scale: Some(*scale as i32),
                type_variation_reference: 0,
            })),
        }),
        adt::DataType::Map(_, _) => Err(SparkError::todo("map")),
        adt::DataType::RunEndEncoded(_, _) => Err(SparkError::todo("run end encoded")),
    }
}
