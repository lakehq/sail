use std::collections::HashMap;

use datafusion::arrow::datatypes as adt;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::{data_type as sdt, DataType};

impl TryFrom<adt::Field> for sdt::StructField {
    type Error = SparkError;

    fn try_from(field: adt::Field) -> SparkResult<sdt::StructField> {
        let is_udt = field.metadata().keys().any(|k| k.starts_with("udt."));
        let data_type = if is_udt {
            DataType {
                kind: Some(sdt::Kind::Udt(Box::new(sdt::Udt {
                    r#type: "udt".to_string(),
                    jvm_class: field.metadata().get("udt.jvm_class").cloned(),
                    python_class: field.metadata().get("udt.python_class").cloned(),
                    serialized_python_class: field
                        .metadata()
                        .get("udt.serialized_python_class")
                        .cloned(),
                    sql_type: Some(Box::new(field.data_type().clone().try_into()?)),
                }))),
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
        let metadata = serde_json::to_string(&metadata)?;
        Ok(sdt::StructField {
            name: field.name().clone(),
            data_type: Some(data_type),
            nullable: field.is_nullable(),
            metadata: Some(metadata),
        })
    }
}

/// Reference: https://github.com/apache/spark/blob/bb17665955ad536d8c81605da9a59fb94b6e0162/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
impl TryFrom<adt::DataType> for DataType {
    type Error = SparkError;

    fn try_from(data_type: adt::DataType) -> SparkResult<DataType> {
        use sdt::Kind;

        let error =
            |x: &adt::DataType| SparkError::unsupported(format!("cast {x:?} to Spark data type"));
        let kind = match data_type {
            adt::DataType::Null => Ok(Kind::Null(sdt::Null::default())),
            adt::DataType::Binary
            | adt::DataType::FixedSizeBinary(_)
            | adt::DataType::LargeBinary
            | adt::DataType::BinaryView => Ok(Kind::Binary(sdt::Binary::default())),
            adt::DataType::Boolean => Ok(Kind::Boolean(sdt::Boolean::default())),
            adt::DataType::Int8 => Ok(Kind::Byte(sdt::Byte::default())),
            adt::DataType::UInt8 | adt::DataType::Int16 => Ok(Kind::Short(sdt::Short::default())),
            adt::DataType::UInt16 | adt::DataType::Int32 => {
                Ok(Kind::Integer(sdt::Integer::default()))
            }
            // FIXME: `adt::DataType::UInt64` to `Kind::Long` will overflow.
            adt::DataType::UInt32 | adt::DataType::UInt64 | adt::DataType::Int64 => {
                Ok(Kind::Long(sdt::Long::default()))
            }
            adt::DataType::Float16 => Err(error(&data_type)),
            adt::DataType::Float32 => Ok(Kind::Float(sdt::Float::default())),
            adt::DataType::Float64 => Ok(Kind::Double(sdt::Double::default())),
            adt::DataType::Decimal128(precision, scale)
            | adt::DataType::Decimal256(precision, scale) => Ok(Kind::Decimal(sdt::Decimal {
                scale: Some(scale as i32),
                precision: Some(precision as i32),
                type_variation_reference: 0,
            })),
            // FIXME: This mapping might not always be correct due to converting to Arrow data types and back.
            //  For example, this originally may have been a `Kind::Char` or `Kind::VarChar` in Spark.
            //  We retain the original type information in the spec, but it is lost after converting to Arrow.
            adt::DataType::Utf8 | adt::DataType::LargeUtf8 | adt::DataType::Utf8View => {
                Ok(Kind::String(sdt::String::default()))
            }
            adt::DataType::Date32 => Ok(Kind::Date(sdt::Date::default())),
            adt::DataType::Date64 => Err(error(&data_type)),
            adt::DataType::Time32 { .. } => Err(error(&data_type)),
            adt::DataType::Time64 { .. } => Err(error(&data_type)),
            // FIXME: return error for nanosecond time unit once Parquet INT96 data type
            //   is handled properly
            adt::DataType::Timestamp(
                adt::TimeUnit::Microsecond | adt::TimeUnit::Nanosecond,
                None,
            ) => Ok(Kind::TimestampNtz(sdt::TimestampNtz::default())),
            adt::DataType::Timestamp(
                adt::TimeUnit::Microsecond | adt::TimeUnit::Nanosecond,
                Some(_),
            ) => Ok(Kind::Timestamp(sdt::Timestamp::default())),
            adt::DataType::Timestamp(adt::TimeUnit::Second, _)
            | adt::DataType::Timestamp(adt::TimeUnit::Millisecond, _) => Err(error(&data_type)),
            adt::DataType::Interval(adt::IntervalUnit::MonthDayNano) => {
                Ok(Kind::CalendarInterval(sdt::CalendarInterval::default()))
            }
            adt::DataType::Interval(adt::IntervalUnit::YearMonth) => {
                Ok(Kind::YearMonthInterval(sdt::YearMonthInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                }))
            }
            adt::DataType::Interval(adt::IntervalUnit::DayTime) => {
                Ok(Kind::DayTimeInterval(sdt::DayTimeInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                }))
            }
            adt::DataType::Duration(adt::TimeUnit::Microsecond) => {
                Ok(Kind::DayTimeInterval(sdt::DayTimeInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                }))
            }
            adt::DataType::Duration(
                adt::TimeUnit::Second | adt::TimeUnit::Millisecond | adt::TimeUnit::Nanosecond,
            ) => Err(error(&data_type)),
            adt::DataType::List(field)
            | adt::DataType::FixedSizeList(field, _)
            | adt::DataType::LargeList(field)
            | adt::DataType::ListView(field)
            | adt::DataType::LargeListView(field) => Ok(Kind::Array(Box::new(sdt::Array {
                element_type: Some(Box::new(field.data_type().clone().try_into()?)),
                contains_null: field.is_nullable(),
                type_variation_reference: 0,
            }))),
            adt::DataType::Struct(fields) => Ok(Kind::Struct(sdt::Struct {
                fields: fields
                    .into_iter()
                    .map(|f| f.as_ref().clone().try_into())
                    .collect::<SparkResult<Vec<sdt::StructField>>>()?,
                type_variation_reference: 0,
            })),
            adt::DataType::Map(ref field, ref _keys_sorted) => {
                let adt::DataType::Struct(fields) = field.data_type() else {
                    return Err(error(&data_type));
                };
                let [key_field, value_field] = fields.as_ref() else {
                    return Err(error(&data_type));
                };
                Ok(Kind::Map(Box::new(sdt::Map {
                    key_type: Some(Box::new(key_field.data_type().clone().try_into()?)),
                    value_type: Some(Box::new(value_field.data_type().clone().try_into()?)),
                    value_contains_null: value_field.is_nullable(),
                    type_variation_reference: 0,
                })))
            }
            adt::DataType::Union { .. }
            | adt::DataType::Dictionary { .. }
            | adt::DataType::RunEndEncoded(_, _) => Err(error(&data_type)),
        };
        Ok(DataType { kind: Some(kind?) })
    }
}
