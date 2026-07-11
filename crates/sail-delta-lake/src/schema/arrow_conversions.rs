use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::error::ArrowError;
use itertools::Itertools;
use parquet_variant_compute::VariantType;
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use crate::spec::schema::{
    ArrayType, DataType, MapType, MetadataValue, PrimitiveType, StructField, StructType,
};

// ── Delta → Arrow ────────────────────────────────────────────────────────────

impl TryFrom<&StructType> for ArrowSchema {
    type Error = ArrowError;
    fn try_from(s: &StructType) -> Result<Self, ArrowError> {
        let fields: Vec<ArrowField> = s.fields().map(ArrowField::try_from).try_collect()?;
        Ok(Self::new(fields))
    }
}

impl TryFrom<&StructField> for ArrowField {
    type Error = ArrowError;
    fn try_from(f: &StructField) -> Result<Self, ArrowError> {
        let mut metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| match val {
                MetadataValue::String(val) => Ok((key.clone(), val.clone())),
                _ => Ok((key.clone(), serde_json::to_string(val)?)),
            })
            .collect::<Result<HashMap<_, _>, serde_json::Error>>()
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;
        if matches!(f.data_type(), DataType::Variant(_)) {
            metadata.insert(
                EXTENSION_TYPE_NAME_KEY.to_string(),
                VariantType::NAME.to_string(),
            );
        }

        Ok(ArrowField::new(
            f.name(),
            ArrowDataType::try_from(f.data_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata))
    }
}

impl TryFrom<&ArrayType> for ArrowField {
    type Error = ArrowError;
    fn try_from(a: &ArrayType) -> Result<Self, ArrowError> {
        ArrowField::try_from(&StructField::new(
            "element",
            a.element_type().clone(),
            a.contains_null(),
        ))
    }
}

impl TryFrom<&MapType> for ArrowField {
    type Error = ArrowError;
    fn try_from(m: &MapType) -> Result<Self, ArrowError> {
        let key_field = ArrowField::try_from(&StructField::not_null("key", m.key_type().clone()))?;
        let value_field = ArrowField::try_from(&StructField::new(
            "value",
            m.value_type().clone(),
            m.value_contains_null(),
        ))?;
        Ok(ArrowField::new(
            "key_value",
            ArrowDataType::Struct(vec![key_field, value_field].into()),
            false,
        ))
    }
}

impl TryFrom<&DataType> for ArrowDataType {
    type Error = ArrowError;
    fn try_from(t: &DataType) -> Result<Self, ArrowError> {
        match t {
            DataType::Primitive(p) => match p {
                PrimitiveType::String => Ok(Self::Utf8),
                PrimitiveType::Long => Ok(Self::Int64),
                PrimitiveType::Integer => Ok(Self::Int32),
                PrimitiveType::Short => Ok(Self::Int16),
                PrimitiveType::Byte => Ok(Self::Int8),
                PrimitiveType::Float => Ok(Self::Float32),
                PrimitiveType::Double => Ok(Self::Float64),
                PrimitiveType::Boolean => Ok(Self::Boolean),
                PrimitiveType::Binary => Ok(Self::Binary),
                PrimitiveType::Decimal(dtype) => {
                    Ok(Self::Decimal128(dtype.precision(), dtype.scale() as i8))
                }
                PrimitiveType::Date => Ok(Self::Date32),
                PrimitiveType::Timestamp => {
                    Ok(Self::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
                }
                PrimitiveType::TimestampNtz => Ok(Self::Timestamp(TimeUnit::Microsecond, None)),
            },
            DataType::Struct(s) => Ok(Self::Struct(
                s.fields()
                    .map(ArrowField::try_from)
                    .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(Self::List(Arc::new(ArrowField::try_from(a.as_ref())?))),
            DataType::Map(m) => Ok(Self::Map(
                Arc::new(ArrowField::try_from(m.as_ref())?),
                false,
            )),
            DataType::Variant(s) => {
                if *t == DataType::unshredded_variant() {
                    Ok(Self::Struct(
                        s.fields()
                            .map(ArrowField::try_from)
                            .collect::<Result<Vec<ArrowField>, ArrowError>>()?
                            .into(),
                    ))
                } else {
                    Err(ArrowError::SchemaError(
                        "Incorrect Variant Schema: only unshredded variant is supported"
                            .to_string(),
                    ))
                }
            }
        }
    }
}

// ── Arrow → Delta ────────────────────────────────────────────────────────────

impl TryFrom<&ArrowSchema> for StructType {
    type Error = ArrowError;
    fn try_from(arrow_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        StructType::try_from_results(
            arrow_schema
                .fields()
                .iter()
                .map(|field| StructField::try_from(field.as_ref())),
        )
        .map_err(|e| ArrowError::from_external_error(Box::new(e)))
    }
}

impl TryFrom<ArrowSchemaRef> for StructType {
    type Error = ArrowError;
    fn try_from(arrow_schema: ArrowSchemaRef) -> Result<Self, ArrowError> {
        StructType::try_from(arrow_schema.as_ref())
    }
}

impl TryFrom<&ArrowField> for StructField {
    type Error = ArrowError;
    fn try_from(arrow_field: &ArrowField) -> Result<Self, ArrowError> {
        let is_variant = is_variant_arrow_field(arrow_field);
        let data_type = if is_variant {
            if !is_unshredded_variant_arrow_type(arrow_field.data_type()) {
                return Err(ArrowError::SchemaError(format!(
                    "Invalid Variant data type for Delta Lake: {}",
                    arrow_field.data_type()
                )));
            }
            DataType::unshredded_variant()
        } else {
            DataType::try_from(arrow_field.data_type())?
        };
        Ok(StructField::new(
            arrow_field.name().clone(),
            data_type,
            arrow_field.is_nullable(),
        )
        .with_metadata(
            arrow_field
                .metadata()
                .iter()
                .filter(|(k, _)| {
                    !(is_variant
                        && (*k == EXTENSION_TYPE_NAME_KEY || *k == EXTENSION_TYPE_METADATA_KEY))
                })
                .map(|(k, v)| (k.clone(), parse_metadata_value(v))),
        ))
    }
}

fn parse_metadata_value(v: &str) -> MetadataValue {
    match serde_json::from_str::<serde_json::Value>(v) {
        Ok(serde_json::Value::Number(n)) => n
            .as_i64()
            .map(MetadataValue::Number)
            .unwrap_or_else(|| MetadataValue::String(v.to_string())),
        Ok(serde_json::Value::Bool(b)) => MetadataValue::Boolean(b),
        Ok(serde_json::Value::String(s)) => MetadataValue::String(s),
        Ok(other) => MetadataValue::Other(other),
        Err(_) => MetadataValue::String(v.to_string()),
    }
}

fn is_variant_arrow_field(field: &ArrowField) -> bool {
    field.extension_type_name() == Some(VariantType::NAME)
        || is_marked_variant_storage_type(field.data_type())
}

fn is_unshredded_variant_arrow_type(data_type: &ArrowDataType) -> bool {
    let ArrowDataType::Struct(fields) = data_type else {
        return false;
    };
    fields.len() == 2
        && ["metadata", "value"].iter().all(|name| {
            fields.iter().any(|field| {
                field.name() == name
                    && matches!(
                        field.data_type(),
                        ArrowDataType::Binary
                            | ArrowDataType::LargeBinary
                            | ArrowDataType::BinaryView
                    )
            })
        })
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = ArrowError;
    fn try_from(arrow_datatype: &ArrowDataType) -> Result<Self, ArrowError> {
        match arrow_datatype {
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                Ok(DataType::STRING)
            }
            ArrowDataType::Int64 | ArrowDataType::UInt64 => Ok(DataType::LONG),
            ArrowDataType::Int32 | ArrowDataType::UInt32 => Ok(DataType::INTEGER),
            ArrowDataType::Int16 | ArrowDataType::UInt16 => Ok(DataType::SHORT),
            ArrowDataType::Int8 | ArrowDataType::UInt8 => Ok(DataType::BYTE),
            ArrowDataType::Float32 => Ok(DataType::FLOAT),
            ArrowDataType::Float64 => Ok(DataType::DOUBLE),
            ArrowDataType::Boolean => Ok(DataType::BOOLEAN),
            ArrowDataType::Binary
            | ArrowDataType::FixedSizeBinary(_)
            | ArrowDataType::LargeBinary
            | ArrowDataType::BinaryView => Ok(DataType::BINARY),
            ArrowDataType::Decimal128(p, s) => {
                if *s < 0 {
                    return Err(ArrowError::SchemaError(
                        "Negative scales are not supported in Delta".to_string(),
                    ));
                }
                DataType::decimal(*p, *s as u8)
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))
            }
            ArrowDataType::Date32 | ArrowDataType::Date64 => Ok(DataType::DATE),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(DataType::TIMESTAMP_NTZ),
            // Any timezone-aware timestamp maps to Delta TIMESTAMP regardless of the timezone
            // label — the session/display timezone does not affect the logical type.
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(DataType::TIMESTAMP),
            ArrowDataType::Struct(fields) => DataType::try_struct_type_from_results(
                fields
                    .iter()
                    .map(|field| StructField::try_from(field.as_ref())),
            )
            .map_err(|e| ArrowError::from_external_error(Box::new(e))),
            ArrowDataType::List(field)
            | ArrowDataType::ListView(field)
            | ArrowDataType::LargeList(field)
            | ArrowDataType::LargeListView(field)
            | ArrowDataType::FixedSizeList(field, _) => Ok(ArrayType::new(
                DataType::try_from(field.data_type())?,
                field.is_nullable(),
            )
            .into()),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = DataType::try_from(struct_fields[0].data_type())?;
                    let value_type = DataType::try_from(struct_fields[1].data_type())?;
                    Ok(MapType::new(key_type, value_type, struct_fields[1].is_nullable()).into())
                } else {
                    Err(ArrowError::SchemaError(
                        "DataType::Map should contain a struct field child".to_string(),
                    ))
                }
            }
            ArrowDataType::Dictionary(_, value_type) => {
                Ok(DataType::try_from(value_type.as_ref())?)
            }
            unsupported => Err(ArrowError::SchemaError(format!(
                "Invalid data type for Delta Lake: {unsupported}"
            ))),
        }
    }
}

impl TryFrom<ArrowSchemaRef> for DataType {
    type Error = crate::spec::DeltaError;

    fn try_from(schema: ArrowSchemaRef) -> Result<Self, Self::Error> {
        let struct_type = StructType::try_from(schema)?;
        Ok(DataType::Struct(Box::new(struct_type)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_variant_element_preserves_extension_metadata() -> Result<(), ArrowError> {
        let field = ArrowField::try_from(&ArrayType::new(DataType::unshredded_variant(), true))?;

        let ArrowDataType::Struct(fields) = field.data_type() else {
            return Err(ArrowError::SchemaError(
                "variant element should use struct storage".to_string(),
            ));
        };
        assert_eq!(field.extension_type_name(), Some(VariantType::NAME));
        assert!(fields.iter().any(|field| field.name() == "metadata"));
        assert!(fields.iter().any(|field| field.name() == "value"));
        Ok(())
    }

    #[test]
    fn map_variant_value_preserves_extension_metadata() -> Result<(), ArrowError> {
        let field = ArrowField::try_from(&MapType::new(
            DataType::STRING,
            DataType::unshredded_variant(),
            true,
        ))?;

        let ArrowDataType::Struct(entries) = field.data_type() else {
            return Err(ArrowError::SchemaError(
                "map entries should use struct storage".to_string(),
            ));
        };
        let Some(value) = entries.iter().find(|entry| entry.name() == "value") else {
            return Err(ArrowError::SchemaError(
                "map value field should exist".to_string(),
            ));
        };
        assert_eq!(value.extension_type_name(), Some(VariantType::NAME));
        Ok(())
    }
}
