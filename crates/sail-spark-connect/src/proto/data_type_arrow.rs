use std::collections::HashMap;

use datafusion::arrow::datatypes as adt;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::{data_type as sdt, DataType};

impl TryFrom<adt::Field> for sdt::StructField {
    type Error = SparkError;

    fn try_from(field: adt::Field) -> SparkResult<sdt::StructField> {
        let is_udt = field.metadata().keys().any(|k| k.starts_with("udt."));
        let is_variant = field
            .metadata()
            .get("ARROW:extension:name")
            .map(|s| s == "arrow.parquet.variant")
            .unwrap_or(false);

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
        } else if is_variant {
            DataType {
                kind: Some(sdt::Kind::Variant(sdt::Variant {
                    type_variation_reference: 0,
                })),
            }
        } else {
            field.data_type().clone().try_into()?
        };
        // FIXME: The metadata. prefix is managed by Sail and the convention should be respected everywhere.
        let metadata = &field
            .metadata()
            .iter()
            .filter(|(k, _)| !k.starts_with("udt."))
            .map(|(k, v)| {
                let parsed = serde_json::from_str::<serde_json::Value>(v)
                    .unwrap_or_else(|_| serde_json::Value::String(v.clone()));
                Ok((k.strip_prefix("metadata.").unwrap_or(k), parsed))
            })
            .collect::<SparkResult<HashMap<_, serde_json::Value>>>()?;
        let metadata = serde_json::to_string(metadata)?;
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
            adt::DataType::Null => Kind::Null(sdt::Null::default()),
            adt::DataType::Binary
            | adt::DataType::FixedSizeBinary(_)
            | adt::DataType::LargeBinary
            | adt::DataType::BinaryView => Kind::Binary(sdt::Binary::default()),
            adt::DataType::Boolean => Kind::Boolean(sdt::Boolean::default()),
            // TODO: cast unsigned integer types to signed integer types in the query output,
            //   and return an error if unsigned integer types are found here.
            adt::DataType::UInt8 | adt::DataType::Int8 => Kind::Byte(sdt::Byte::default()),
            adt::DataType::UInt16 | adt::DataType::Int16 => Kind::Short(sdt::Short::default()),
            adt::DataType::UInt32 | adt::DataType::Int32 => Kind::Integer(sdt::Integer::default()),
            adt::DataType::UInt64 | adt::DataType::Int64 => Kind::Long(sdt::Long::default()),
            adt::DataType::Float16 => return Err(error(&data_type)),
            adt::DataType::Float32 => Kind::Float(sdt::Float::default()),
            adt::DataType::Float64 => Kind::Double(sdt::Double::default()),
            adt::DataType::Decimal128(precision, scale)
            | adt::DataType::Decimal256(precision, scale) => Kind::Decimal(sdt::Decimal {
                scale: Some(scale as i32),
                precision: Some(precision as i32),
                type_variation_reference: 0,
            }),
            // FIXME: This mapping might not always be correct due to converting to Arrow data types and back.
            //  For example, this originally may have been a `Kind::Char` or `Kind::VarChar` in Spark.
            //  We retain the original type information in the spec, but it is lost after converting to Arrow.
            adt::DataType::Utf8 | adt::DataType::LargeUtf8 | adt::DataType::Utf8View => {
                Kind::String(sdt::String::default())
            }
            adt::DataType::Date32 => Kind::Date(sdt::Date::default()),
            adt::DataType::Date64 | adt::DataType::Time32 { .. } | adt::DataType::Time64 { .. } => {
                return Err(error(&data_type))
            }
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, None) => {
                Kind::TimestampNtz(sdt::TimestampNtz::default())
            }
            adt::DataType::Timestamp(adt::TimeUnit::Microsecond, Some(_)) => {
                Kind::Timestamp(sdt::Timestamp::default())
            }
            adt::DataType::Timestamp(adt::TimeUnit::Second, _)
            | adt::DataType::Timestamp(adt::TimeUnit::Millisecond, _)
            | adt::DataType::Timestamp(adt::TimeUnit::Nanosecond, _) => {
                return Err(error(&data_type))
            }
            adt::DataType::Interval(adt::IntervalUnit::MonthDayNano) => {
                Kind::CalendarInterval(sdt::CalendarInterval::default())
            }
            adt::DataType::Interval(adt::IntervalUnit::YearMonth) => {
                Kind::YearMonthInterval(sdt::YearMonthInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                })
            }
            adt::DataType::Interval(adt::IntervalUnit::DayTime) => {
                Kind::DayTimeInterval(sdt::DayTimeInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                })
            }
            adt::DataType::Duration(adt::TimeUnit::Microsecond) => {
                Kind::DayTimeInterval(sdt::DayTimeInterval {
                    start_field: None,
                    end_field: None,
                    type_variation_reference: 0,
                })
            }
            adt::DataType::Duration(
                adt::TimeUnit::Second | adt::TimeUnit::Millisecond | adt::TimeUnit::Nanosecond,
            ) => return Err(error(&data_type)),
            adt::DataType::List(field)
            | adt::DataType::FixedSizeList(field, _)
            | adt::DataType::LargeList(field)
            | adt::DataType::ListView(field)
            | adt::DataType::LargeListView(field) => {
                let field = sdt::StructField::try_from(field.as_ref().clone())?;
                Kind::Array(Box::new(sdt::Array {
                    element_type: field.data_type.map(Box::new),
                    contains_null: field.nullable,
                    type_variation_reference: 0,
                }))
            }
            adt::DataType::Struct(fields) => {
                // Always treat as a regular Struct
                // Variant detection is handled at the Field level via extension type metadata
                Kind::Struct(sdt::Struct {
                    fields: fields
                        .into_iter()
                        .map(|f| f.as_ref().clone().try_into())
                        .collect::<SparkResult<Vec<sdt::StructField>>>()?,
                    type_variation_reference: 0,
                })
            }
            adt::DataType::Map(ref field, ref _keys_sorted) => {
                let field = sdt::StructField::try_from(field.as_ref().clone())?;
                let Some(DataType {
                    kind: Some(Kind::Struct(sdt::Struct { fields, .. })),
                }) = field.data_type
                else {
                    return Err(error(&data_type));
                };
                let [key_field, value_field] = fields.as_slice() else {
                    return Err(error(&data_type));
                };
                Kind::Map(Box::new(sdt::Map {
                    key_type: key_field.data_type.clone().map(Box::new),
                    value_type: value_field.data_type.clone().map(Box::new),
                    value_contains_null: value_field.nullable,
                    type_variation_reference: 0,
                }))
            }
            adt::DataType::Union { .. }
            | adt::DataType::Dictionary { .. }
            | adt::DataType::RunEndEncoded(_, _)
            | adt::DataType::Decimal32(_, _)
            | adt::DataType::Decimal64(_, _) => return Err(error(&data_type)),
        };
        Ok(DataType { kind: Some(kind) })
    }
}

#[cfg(test)]
mod tests {
    use adt::{DataType as ArrowDataType, Field, Fields};

    use super::*;

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_struct_with_metadata_value_fields_not_variant() {
        // A struct with fields named "metadata" and "value" should NOT be treated as Variant
        // unless it has the extension type metadata
        let fields = Fields::from(vec![
            Field::new("value", ArrowDataType::Binary, true),
            Field::new("metadata", ArrowDataType::Binary, true),
        ]);
        let struct_type = ArrowDataType::Struct(fields);

        // Convert to Spark DataType
        let spark_type = match DataType::try_from(struct_type) {
            Ok(dt) => dt,
            Err(e) => {
                assert!(false, "Should convert successfully, got error: {e}");
                return;
            }
        };

        // Should be a Struct, NOT a Variant
        assert!(
            matches!(spark_type.kind, Some(sdt::Kind::Struct(_))),
            "Struct with metadata/value fields should be treated as regular Struct, not Variant"
        );
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_field_with_variant_extension_type_is_variant() {
        // A field with the arrow.parquet.variant extension type SHOULD be treated as Variant
        let fields = Fields::from(vec![
            Field::new("value", ArrowDataType::Binary, true),
            Field::new("metadata", ArrowDataType::Binary, true),
        ]);

        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        );

        let field = Field::new("test", ArrowDataType::Struct(fields), true).with_metadata(metadata);

        // Convert to Spark StructField
        let spark_field = match sdt::StructField::try_from(field) {
            Ok(f) => f,
            Err(e) => {
                assert!(false, "Should convert successfully, got error: {e}");
                return;
            }
        };

        // Should be a Variant
        assert!(
            matches!(
                spark_field.data_type.and_then(|dt| dt.kind),
                Some(sdt::Kind::Variant(_))
            ),
            "Field with arrow.parquet.variant extension should be treated as Variant"
        );
    }
}
