use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use datafusion_common::Result;

use crate::spec::{NestedField, PrimitiveType, Schema, StructType, Type};

/// Convert Iceberg schema to Arrow schema
pub fn iceberg_schema_to_arrow(schema: &Schema) -> Result<ArrowSchema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| iceberg_field_to_arrow(field))
        .collect::<Result<Vec<_>>>()?;

    Ok(ArrowSchema::new(fields))
}

/// Convert Iceberg field to Arrow field
pub fn iceberg_field_to_arrow(field: &NestedField) -> Result<ArrowField> {
    let arrow_type = iceberg_type_to_arrow(&field.field_type)?;
    let nullable = !field.required;

    Ok(ArrowField::new(&field.name, arrow_type, nullable))
}

/// Convert Iceberg type to Arrow data type
pub fn iceberg_type_to_arrow(iceberg_type: &Type) -> Result<ArrowDataType> {
    match iceberg_type {
        Type::Primitive(primitive) => iceberg_primitive_to_arrow(primitive),
        Type::Struct(struct_type) => iceberg_struct_to_arrow(struct_type),
        Type::List(list_type) => {
            let element_field = iceberg_field_to_arrow(&list_type.element_field)?;
            Ok(ArrowDataType::List(Arc::new(element_field)))
        }
        Type::Map(map_type) => {
            let key_field = iceberg_field_to_arrow(&map_type.key_field)?;
            let value_field = iceberg_field_to_arrow(&map_type.value_field)?;

            // Arrow Map type expects a struct with key and value fields
            let entries_field = ArrowField::new(
                "entries",
                ArrowDataType::Struct(vec![key_field, value_field].into()),
                false, // entries field itself is not nullable
            );

            Ok(ArrowDataType::Map(Arc::new(entries_field), false))
        }
    }
}

/// Convert Iceberg primitive type to Arrow data type
pub fn iceberg_primitive_to_arrow(primitive: &PrimitiveType) -> Result<ArrowDataType> {
    let arrow_type = match primitive {
        PrimitiveType::Boolean => ArrowDataType::Boolean,
        PrimitiveType::Int => ArrowDataType::Int32,
        PrimitiveType::Long => ArrowDataType::Int64,
        PrimitiveType::Float => ArrowDataType::Float32,
        PrimitiveType::Double => ArrowDataType::Float64,
        PrimitiveType::Decimal { precision, scale } => {
            ArrowDataType::Decimal128(*precision as u8, *scale as i8)
        }
        PrimitiveType::Date => ArrowDataType::Date32,
        PrimitiveType::Time => ArrowDataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Timestamptz => {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        PrimitiveType::TimestampNs => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
        PrimitiveType::TimestamptzNs => {
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        }
        PrimitiveType::String => ArrowDataType::Utf8,
        PrimitiveType::Uuid => ArrowDataType::FixedSizeBinary(16),
        PrimitiveType::Fixed(size) => ArrowDataType::FixedSizeBinary(*size as i32),
        PrimitiveType::Binary => ArrowDataType::Binary,
    };

    Ok(arrow_type)
}

/// Convert Iceberg struct type to Arrow struct data type
pub fn iceberg_struct_to_arrow(struct_type: &StructType) -> Result<ArrowDataType> {
    let fields = struct_type
        .fields()
        .iter()
        .map(|field| iceberg_field_to_arrow(field))
        .collect::<Result<Vec<_>>>()?;

    Ok(ArrowDataType::Struct(fields.into()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{NestedField, PrimitiveType, Schema, Type};

    #[test]
    fn test_primitive_type_conversion() {
        let test_cases = vec![
            (PrimitiveType::Boolean, ArrowDataType::Boolean),
            (PrimitiveType::Int, ArrowDataType::Int32),
            (PrimitiveType::Long, ArrowDataType::Int64),
            (PrimitiveType::Float, ArrowDataType::Float32),
            (PrimitiveType::Double, ArrowDataType::Float64),
            (PrimitiveType::String, ArrowDataType::Utf8),
            (PrimitiveType::Binary, ArrowDataType::Binary),
            (PrimitiveType::Date, ArrowDataType::Date32),
            (
                PrimitiveType::Time,
                ArrowDataType::Time64(TimeUnit::Microsecond),
            ),
            (
                PrimitiveType::Timestamp,
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                PrimitiveType::Timestamptz,
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (PrimitiveType::Uuid, ArrowDataType::FixedSizeBinary(16)),
            (PrimitiveType::Fixed(10), ArrowDataType::FixedSizeBinary(10)),
        ];

        for (iceberg_type, expected_arrow_type) in test_cases {
            let result = iceberg_primitive_to_arrow(&iceberg_type)
                .expect("Failed to convert iceberg type to arrow");
            assert_eq!(result, expected_arrow_type);
        }
    }

    #[test]
    fn test_decimal_type_conversion() {
        let decimal_type = PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        };
        let result = iceberg_primitive_to_arrow(&decimal_type)
            .expect("Failed to convert decimal type to arrow");
        assert_eq!(result, ArrowDataType::Decimal128(10, 2));
    }

    #[test]
    fn test_schema_conversion() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "price",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                )),
            ])
            .build()
            .expect("Failed to build schema");

        let arrow_schema =
            iceberg_schema_to_arrow(&schema).expect("Failed to convert schema to arrow");

        assert_eq!(arrow_schema.fields().len(), 3);

        let id_field = arrow_schema.field(0);
        assert_eq!(id_field.name(), "id");
        assert_eq!(id_field.data_type(), &ArrowDataType::Int64);
        assert!(!id_field.is_nullable());

        let name_field = arrow_schema.field(1);
        assert_eq!(name_field.name(), "name");
        assert_eq!(name_field.data_type(), &ArrowDataType::Utf8);
        assert!(name_field.is_nullable());

        let price_field = arrow_schema.field(2);
        assert_eq!(price_field.name(), "price");
        assert_eq!(price_field.data_type(), &ArrowDataType::Decimal128(10, 2));
        assert!(!price_field.is_nullable());
    }
}
