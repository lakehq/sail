use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use datafusion::arrow::datatypes::{
    validate_decimal_precision_and_scale, Decimal128Type as ArrowDecimal128Type,
};
use datafusion_common::{plan_datafusion_err, plan_err, Result};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use rust_decimal::prelude::ToPrimitive;

use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

pub const ICEBERG_ARROW_FIELD_DOC_KEY: &str = "doc";

fn get_field_id(field: &ArrowField) -> i32 {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0)
}

fn get_field_doc(field: &ArrowField) -> Option<String> {
    if let Some(value) = field.metadata().get(ICEBERG_ARROW_FIELD_DOC_KEY) {
        return Some(value.clone());
    }
    None
}

/// Convert Iceberg schema to Arrow schema
pub fn iceberg_schema_to_arrow(schema: &Schema) -> Result<ArrowSchema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| iceberg_field_to_arrow(field))
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrowSchema::new(fields))
}

/// Convert Arrow schema to Iceberg schema
pub fn arrow_schema_to_iceberg(schema: &ArrowSchema) -> Result<Schema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Ok(Arc::new(arrow_field_to_iceberg(field)?)))
        .collect::<Result<Vec<_>>>()?;
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| plan_datafusion_err!("Failed to build Iceberg schema: {e}"))
}

/// Convert Iceberg field to Arrow field
pub fn iceberg_field_to_arrow(field: &NestedField) -> Result<ArrowField> {
    let arrow_type = iceberg_type_to_arrow(&field.field_type)?;
    let nullable = !field.required;
    let metadata = if let Some(doc) = &field.doc {
        HashMap::from([
            (PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string()),
            (ICEBERG_ARROW_FIELD_DOC_KEY.to_string(), doc.clone()),
        ])
    } else {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string())])
    };
    Ok(ArrowField::new(&field.name, arrow_type, nullable).with_metadata(metadata))
}

/// Convert Arrow field to Iceberg field
pub fn arrow_field_to_iceberg(field: &ArrowField) -> Result<NestedField> {
    let iceberg_type = arrow_type_to_iceberg(field.data_type())?;
    let required = !field.is_nullable();
    let doc = get_field_doc(field);
    let field = NestedField::new(
        get_field_id(field),
        field.name().clone(),
        iceberg_type,
        required,
    );
    if let Some(doc) = doc {
        Ok(field.with_doc(doc))
    } else {
        Ok(field)
    }
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

/// Convert Arrow data type to Iceberg type
pub fn arrow_type_to_iceberg(arrow_type: &ArrowDataType) -> Result<Type> {
    match arrow_type {
        ArrowDataType::Struct(_) => {
            let struct_type = arrow_struct_to_iceberg(arrow_type)?;
            Ok(Type::Struct(struct_type))
        }
        ArrowDataType::List(field)
        | ArrowDataType::ListView(field)
        | ArrowDataType::LargeList(field) => {
            let element_field = arrow_field_to_iceberg(field)?;
            Ok(Type::List(ListType::new(Arc::new(element_field))))
        }
        ArrowDataType::Map(entries_field, _sorted) => {
            if let ArrowDataType::Struct(fields) = entries_field.data_type() {
                if fields.len() != 2 {
                    return plan_err!(
                        "Map entries struct must have exactly 2 fields, found: {}",
                        fields.len()
                    );
                }
                let key_nested = Arc::new(arrow_field_to_iceberg(&fields[0])?);
                let value_nested = Arc::new(arrow_field_to_iceberg(&fields[1])?);
                Ok(Type::Map(MapType::new(key_nested, value_nested)))
            } else {
                plan_err!("Map entries field must be a Struct")
            }
        }
        _ => {
            let primitive = arrow_primitive_to_iceberg(arrow_type)?;
            Ok(Type::Primitive(primitive))
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
            let (precision, scale) = {
                let precision: u8 = (*precision).try_into().map_err(|_| {
                    plan_datafusion_err!("Decimal precision overflow: {}", precision)
                })?;
                let scale: i8 = (*scale)
                    .try_into()
                    .map_err(|_| plan_datafusion_err!("Decimal scale overflow: {}", scale))?;
                (precision, scale)
            };
            validate_decimal_precision_and_scale::<ArrowDecimal128Type>(precision, scale)
                .map_err(|e| plan_datafusion_err!("Invalid decimal precision/scale: {e}"))?;
            ArrowDataType::Decimal128(precision, scale)
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
        PrimitiveType::Fixed(size) => size
            .to_i32()
            .map(ArrowDataType::FixedSizeBinary)
            .unwrap_or(ArrowDataType::LargeBinary),
        PrimitiveType::Binary => ArrowDataType::LargeBinary,
    };
    Ok(arrow_type)
}

/// Convert Arrow data type to Iceberg primitive type
pub fn arrow_primitive_to_iceberg(arrow_type: &ArrowDataType) -> Result<PrimitiveType> {
    let primitive_type = match arrow_type {
        ArrowDataType::Boolean => PrimitiveType::Boolean,
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16 => PrimitiveType::Int,
        ArrowDataType::UInt32 | ArrowDataType::Int64 => PrimitiveType::Long,
        ArrowDataType::Float32 => PrimitiveType::Float,
        ArrowDataType::Float64 => PrimitiveType::Double,
        ArrowDataType::Decimal128(precision, scale) => {
            let iceberg_type = Type::decimal(*precision as u32, *scale as u32)
                .map_err(|e| plan_datafusion_err!("Failed to create decimal type: {e}"))?;
            match iceberg_type {
                Type::Primitive(p) => p,
                _ => return plan_err!("Expected decimal to be a primitive type"),
            }
        }
        ArrowDataType::Date32 => PrimitiveType::Date,
        ArrowDataType::Time64(TimeUnit::Microsecond) => PrimitiveType::Time,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => PrimitiveType::Timestamp,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
            if tz.as_ref() == "UTC" || tz.as_ref() == "+00:00" =>
        {
            PrimitiveType::Timestamptz
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => PrimitiveType::TimestampNs,
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some(tz))
            if tz.as_ref() == "UTC" || tz.as_ref() == "+00:00" =>
        {
            PrimitiveType::TimestamptzNs
        }
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            PrimitiveType::String
        }
        ArrowDataType::FixedSizeBinary(16) => PrimitiveType::Uuid,
        ArrowDataType::FixedSizeBinary(size) => PrimitiveType::Fixed(*size as u64),
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => {
            PrimitiveType::Binary
        }
        _ => {
            return plan_err!(
                "Unsupported Arrow data type for Iceberg primitive conversion: {arrow_type}"
            );
        }
    };
    Ok(primitive_type)
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

/// Convert Arrow struct data type to Iceberg struct type
pub fn arrow_struct_to_iceberg(struct_type: &ArrowDataType) -> Result<StructType> {
    if let ArrowDataType::Struct(fields) = struct_type {
        let fields = fields
            .iter()
            .map(|field| Ok(Arc::new(arrow_field_to_iceberg(field)?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(StructType::new(fields))
    } else {
        plan_err!("Expected Struct type, found: {struct_type}")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{NestedField, PrimitiveType, Schema, Type};

    #[test]
    fn test_iceberg_primitive_type_to_arrow_type_conversion() {
        let test_cases = vec![
            (PrimitiveType::Boolean, ArrowDataType::Boolean),
            (PrimitiveType::Int, ArrowDataType::Int32),
            (PrimitiveType::Long, ArrowDataType::Int64),
            (PrimitiveType::Float, ArrowDataType::Float32),
            (PrimitiveType::Double, ArrowDataType::Float64),
            (
                PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                ArrowDataType::Decimal128(10, 2),
            ),
            (PrimitiveType::String, ArrowDataType::Utf8),
            (PrimitiveType::Binary, ArrowDataType::LargeBinary),
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
            (
                PrimitiveType::TimestampNs,
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                PrimitiveType::TimestamptzNs,
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
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
    fn test_arrow_type_to_iceberg_primitive_type_conversion() {
        let test_cases = vec![
            (ArrowDataType::Boolean, PrimitiveType::Boolean),
            (ArrowDataType::Int8, PrimitiveType::Int),
            (ArrowDataType::Int16, PrimitiveType::Int),
            (ArrowDataType::Int32, PrimitiveType::Int),
            (ArrowDataType::UInt8, PrimitiveType::Int),
            (ArrowDataType::UInt16, PrimitiveType::Int),
            (ArrowDataType::UInt32, PrimitiveType::Long),
            (ArrowDataType::Int64, PrimitiveType::Long),
            (ArrowDataType::Float32, PrimitiveType::Float),
            (ArrowDataType::Float64, PrimitiveType::Double),
            (
                ArrowDataType::Decimal128(10, 2),
                PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
            (ArrowDataType::Utf8, PrimitiveType::String),
            (ArrowDataType::LargeUtf8, PrimitiveType::String),
            (ArrowDataType::Utf8View, PrimitiveType::String),
            (ArrowDataType::Binary, PrimitiveType::Binary),
            (ArrowDataType::LargeBinary, PrimitiveType::Binary),
            (ArrowDataType::BinaryView, PrimitiveType::Binary),
            (ArrowDataType::Date32, PrimitiveType::Date),
            (
                ArrowDataType::Time64(TimeUnit::Microsecond),
                PrimitiveType::Time,
            ),
            (
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                PrimitiveType::Timestamp,
            ),
            (
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                PrimitiveType::Timestamptz,
            ),
            (
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                PrimitiveType::Timestamptz,
            ),
            (
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                PrimitiveType::TimestampNs,
            ),
            (
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                PrimitiveType::TimestamptzNs,
            ),
            (
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                PrimitiveType::TimestamptzNs,
            ),
            (ArrowDataType::FixedSizeBinary(16), PrimitiveType::Uuid),
            (ArrowDataType::FixedSizeBinary(10), PrimitiveType::Fixed(10)),
        ];

        for (arrow_type, expected_iceberg_type) in test_cases {
            let result = arrow_primitive_to_iceberg(&arrow_type)
                .expect("Failed to convert arrow type to iceberg");
            assert_eq!(result, expected_iceberg_type);
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

    #[test]
    fn test_arrow_decimal_to_iceberg_conversion() {
        let arrow_type = ArrowDataType::Decimal128(10, 2);
        let result = arrow_primitive_to_iceberg(&arrow_type)
            .expect("Failed to convert decimal type to iceberg");
        assert_eq!(
            result,
            PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn test_arrow_schema_to_iceberg_conversion() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
                (
                    ICEBERG_ARROW_FIELD_DOC_KEY.to_string(),
                    "Unique identifier".to_string(),
                ),
            ])),
            ArrowField::new("name", ArrowDataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            ArrowField::new("price", ArrowDataType::Decimal128(10, 2), false).with_metadata(
                HashMap::from([
                    (PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string()),
                    (
                        ICEBERG_ARROW_FIELD_DOC_KEY.to_string(),
                        "Price in USD".to_string(),
                    ),
                ]),
            ),
        ]);

        let iceberg_schema = arrow_schema_to_iceberg(&arrow_schema)
            .expect("Failed to convert arrow schema to iceberg");

        let fields = iceberg_schema.fields();
        assert_eq!(fields.len(), 3);

        let id_field = &fields[0];
        assert_eq!(id_field.id, 1);
        assert_eq!(id_field.name, "id");
        assert_eq!(*id_field.field_type, Type::Primitive(PrimitiveType::Long));
        assert!(id_field.required);
        assert_eq!(id_field.doc, Some("Unique identifier".to_string()));

        let name_field = &fields[1];
        assert_eq!(name_field.id, 2);
        assert_eq!(name_field.name, "name");
        assert_eq!(
            *name_field.field_type,
            Type::Primitive(PrimitiveType::String)
        );
        assert!(!name_field.required);
        assert_eq!(name_field.doc, None);

        let price_field = &fields[2];
        assert_eq!(price_field.id, 3);
        assert_eq!(price_field.name, "price");
        assert_eq!(
            *price_field.field_type,
            Type::Primitive(PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            })
        );
        assert!(price_field.required);
        assert_eq!(price_field.doc, Some("Price in USD".to_string()));
    }

    #[allow(clippy::panic)]
    #[test]
    fn test_arrow_list_to_iceberg_conversion() {
        let element_field =
            ArrowField::new("element", ArrowDataType::Int64, true).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
                (
                    ICEBERG_ARROW_FIELD_DOC_KEY.to_string(),
                    "List element".to_string(),
                ),
            ]));
        let arrow_list = ArrowDataType::List(Arc::new(element_field));

        let iceberg_type =
            arrow_type_to_iceberg(&arrow_list).expect("Failed to convert Arrow list to Iceberg");

        match iceberg_type {
            Type::List(list_type) => {
                assert_eq!(list_type.element_field.id, 1);
                assert_eq!(
                    *list_type.element_field.field_type,
                    Type::Primitive(PrimitiveType::Long)
                );
                assert!(!list_type.element_field.required);
                assert_eq!(
                    list_type.element_field.doc,
                    Some("List element".to_string())
                );
            }
            _ => panic!("Expected List type"),
        }
    }

    #[allow(clippy::panic)]
    #[test]
    fn test_arrow_map_to_iceberg_conversion() {
        let key_field =
            ArrowField::new("key", ArrowDataType::Utf8, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
                (
                    ICEBERG_ARROW_FIELD_DOC_KEY.to_string(),
                    "Map key".to_string(),
                ),
            ]));
        let value_field = ArrowField::new("value", ArrowDataType::Int64, true).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
        );
        let entries_struct = ArrowDataType::Struct(vec![key_field, value_field].into());
        let entries_field = ArrowField::new("entries", entries_struct, false);
        let arrow_map = ArrowDataType::Map(Arc::new(entries_field), false);

        let iceberg_type =
            arrow_type_to_iceberg(&arrow_map).expect("Failed to convert Arrow map to Iceberg");

        match iceberg_type {
            Type::Map(map_type) => {
                assert_eq!(map_type.key_field.id, 1);
                assert_eq!(
                    *map_type.key_field.field_type,
                    Type::Primitive(PrimitiveType::String)
                );
                assert!(map_type.key_field.required);
                assert_eq!(map_type.key_field.doc, Some("Map key".to_string()));
                assert_eq!(map_type.value_field.id, 2);
                assert_eq!(
                    *map_type.value_field.field_type,
                    Type::Primitive(PrimitiveType::Long)
                );
                assert!(!map_type.value_field.required);
                assert_eq!(map_type.value_field.doc, None);
            }
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn test_arrow_struct_to_iceberg_conversion() {
        let struct_fields = vec![
            ArrowField::new("id", ArrowDataType::Int64, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
                (
                    ICEBERG_ARROW_FIELD_DOC_KEY.to_string(),
                    "Struct ID field".to_string(),
                ),
            ])),
            ArrowField::new("name", ArrowDataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ];
        let arrow_struct = ArrowDataType::Struct(struct_fields.into());

        let struct_type = arrow_struct_to_iceberg(&arrow_struct)
            .expect("Failed to convert Arrow struct to Iceberg");

        let fields = struct_type.fields();
        assert_eq!(fields.len(), 2);

        assert_eq!(fields[0].id, 1);
        assert_eq!(fields[0].name, "id");
        assert_eq!(*fields[0].field_type, Type::Primitive(PrimitiveType::Long));
        assert!(fields[0].required);
        assert_eq!(fields[0].doc, Some("Struct ID field".to_string()));

        assert_eq!(fields[1].id, 2);
        assert_eq!(fields[1].name, "name");
        assert_eq!(
            *fields[1].field_type,
            Type::Primitive(PrimitiveType::String)
        );
        assert!(!fields[1].required);
        assert_eq!(fields[1].doc, None);
    }

    #[test]
    fn test_roundtrip_schema_conversion() {
        let original_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                        .with_doc("Primary key"),
                ),
                Arc::new(NestedField::optional(
                    2,
                    "data",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("Failed to build schema");

        let arrow_schema =
            iceberg_schema_to_arrow(&original_schema).expect("Failed to convert to Arrow");

        // Verify Arrow schema has metadata with field IDs and docs
        assert_eq!(
            arrow_schema
                .field(0)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );
        assert_eq!(
            arrow_schema
                .field(0)
                .metadata()
                .get(ICEBERG_ARROW_FIELD_DOC_KEY),
            Some(&"Primary key".to_string())
        );
        assert_eq!(
            arrow_schema
                .field(1)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2".to_string())
        );
        assert_eq!(
            arrow_schema
                .field(1)
                .metadata()
                .get(ICEBERG_ARROW_FIELD_DOC_KEY),
            None
        );

        let roundtrip_schema =
            arrow_schema_to_iceberg(&arrow_schema).expect("Failed to convert back to Iceberg");

        let original_fields = original_schema.fields();
        let roundtrip_fields = roundtrip_schema.fields();

        assert_eq!(original_fields.len(), roundtrip_fields.len());
        for i in 0..original_fields.len() {
            assert_eq!(original_fields[i].id, roundtrip_fields[i].id);
            assert_eq!(original_fields[i].name, roundtrip_fields[i].name);
            assert_eq!(
                original_fields[i].field_type,
                roundtrip_fields[i].field_type
            );
            assert_eq!(original_fields[i].required, roundtrip_fields[i].required);
            assert_eq!(original_fields[i].doc, roundtrip_fields[i].doc);
        }
    }
}
