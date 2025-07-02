//! Provide schema merging for delta schemas
//!
//! This module implements schema merging functionality required for ADD COLUMN operations.
//! Since we've disabled the datafusion feature in delta-rs, we need to implement
//! the middleware layer ourselves.

use std::collections::HashMap;

use datafusion::arrow::error::ArrowError;
use deltalake::kernel::{ArrayType, DataType as DeltaDataType, MapType, StructField, StructType};

/// Try to merge metadata from two fields
fn try_merge_metadata<T: std::cmp::PartialEq + Clone>(
    left: &mut HashMap<String, T>,
    right: &HashMap<String, T>,
) -> Result<(), ArrowError> {
    for (k, v) in right {
        if let Some(vl) = left.get(k) {
            if vl != v {
                return Err(ArrowError::SchemaError(format!(
                    "Cannot merge metadata with different values for key {k}"
                )));
            }
        } else {
            // Don't allow adding generated expressions to existing columns
            // TODO We'll skip this check for now since we don't have access to ColumnMetadataKey
            // in the deltalake re-exports
            left.insert(k.clone(), v.clone());
        }
    }
    Ok(())
}

/// Merge two Delta data types
pub(crate) fn merge_delta_type(
    left: &DeltaDataType,
    right: &DeltaDataType,
) -> Result<DeltaDataType, ArrowError> {
    if left == right {
        return Ok(left.clone());
    }
    match (left, right) {
        (DeltaDataType::Array(a), DeltaDataType::Array(b)) => {
            let merged = merge_delta_type(&a.element_type, &b.element_type)?;
            Ok(DeltaDataType::Array(Box::new(ArrayType::new(
                merged,
                a.contains_null() || b.contains_null(),
            ))))
        }
        (DeltaDataType::Map(a), DeltaDataType::Map(b)) => {
            let merged_key = merge_delta_type(&a.key_type, &b.key_type)?;
            let merged_value = merge_delta_type(&a.value_type, &b.value_type)?;
            Ok(DeltaDataType::Map(Box::new(MapType::new(
                merged_key,
                merged_value,
                a.value_contains_null() || b.value_contains_null(),
            ))))
        }
        (DeltaDataType::Struct(a), DeltaDataType::Struct(b)) => {
            let merged = merge_delta_struct(a, b)?;
            Ok(DeltaDataType::Struct(Box::new(merged)))
        }
        (a, b) => Err(ArrowError::SchemaError(format!(
            "Cannot merge types {a} and {b}"
        ))),
    }
}

/// Merge two Delta struct types
pub(crate) fn merge_delta_struct(
    left: &StructType,
    right: &StructType,
) -> Result<StructType, ArrowError> {
    let mut errors = Vec::new();
    let merged_fields: Result<Vec<StructField>, ArrowError> = left
        .fields()
        .map(|field| {
            let right_field = right.field(field.name());
            if let Some(right_field) = right_field {
                let type_or_not = merge_delta_type(field.data_type(), right_field.data_type());
                match type_or_not {
                    Err(e) => {
                        errors.push(e.to_string());
                        Err(e)
                    }
                    Ok(f) => {
                        let mut new_field = StructField::new(
                            field.name(),
                            f,
                            field.is_nullable() || right_field.is_nullable(),
                        );

                        new_field.metadata.clone_from(&field.metadata);
                        try_merge_metadata(&mut new_field.metadata, &right_field.metadata)?;
                        Ok(new_field)
                    }
                }
            } else {
                Ok(field.clone())
            }
        })
        .collect();

    match merged_fields {
        Ok(mut fields) => {
            // Add fields that exist only in the right schema
            for field in right.fields() {
                if left.field(field.name()).is_none() {
                    fields.push(field.clone());
                }
            }

            Ok(StructType::new(fields))
        }
        Err(e) => {
            errors.push(e.to_string());
            Err(ArrowError::SchemaError(errors.join("\n")))
        }
    }
}
