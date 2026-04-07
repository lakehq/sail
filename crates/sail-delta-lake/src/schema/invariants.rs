use serde_json::Value;

use crate::spec::{
    ColumnMetadataKey, DataType, DeltaError, DeltaResult, MetadataValue, StructField, StructType,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaInvariant {
    pub field_path: String,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvariantPathSupport {
    Supported,
    UnsupportedCollection,
}

pub fn extract_invariants(schema: &StructType) -> DeltaResult<Vec<DeltaInvariant>> {
    let mut remaining_fields: Vec<(String, StructField)> = schema
        .fields()
        .map(|field| (field.name.clone(), field.clone()))
        .collect();
    let mut invariants = Vec::new();

    while let Some((field_path, field)) = remaining_fields.pop() {
        match field.data_type() {
            DataType::Struct(inner) => {
                remaining_fields.extend(
                    inner
                        .fields()
                        .map(|child| (join_field_path(&field_path, &child.name), child.clone())),
                );
            }
            DataType::Array(inner) => {
                remaining_fields.push((
                    join_field_path(&field_path, "element"),
                    StructField::new(String::new(), inner.element_type.clone(), false),
                ));
            }
            DataType::Map(inner) => {
                remaining_fields.push((
                    join_field_path(&field_path, "key"),
                    StructField::new(String::new(), inner.key_type.clone(), false),
                ));
                remaining_fields.push((
                    join_field_path(&field_path, "value"),
                    StructField::new(String::new(), inner.value_type.clone(), false),
                ));
            }
            _ => {}
        }

        let Some(MetadataValue::String(invariant_json)) =
            field.metadata().get(ColumnMetadataKey::Invariants.as_ref())
        else {
            continue;
        };

        let json: Value = serde_json::from_str(invariant_json).map_err(|e| {
            DeltaError::generic(format!(
                "invalid delta.invariants JSON for field '{field_path}': {e}"
            ))
        })?;
        let sql = json
            .get("expression")
            .and_then(Value::as_object)
            .and_then(|expr| expr.get("expression"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                DeltaError::generic(format!(
                    "invalid delta.invariants payload for field '{field_path}'"
                ))
            })?;
        invariants.push(DeltaInvariant {
            field_path,
            sql: sql.to_string(),
        });
    }

    Ok(invariants)
}

pub fn invariant_path_support(
    schema: &StructType,
    field_path: &str,
) -> DeltaResult<InvariantPathSupport> {
    let mut current = DataType::Struct(Box::new(schema.clone()));
    for segment in field_path.split('.').filter(|segment| !segment.is_empty()) {
        current = match current {
            DataType::Struct(inner) => {
                let field = inner.field(segment).ok_or_else(|| {
                    DeltaError::schema(format!(
                        "invariant field path '{field_path}' does not exist in schema"
                    ))
                })?;
                field.data_type().clone()
            }
            DataType::Array(_) | DataType::Map(_) => {
                return Ok(InvariantPathSupport::UnsupportedCollection)
            }
            _ => {
                return Err(DeltaError::schema(format!(
                    "invariant field path '{field_path}' does not match schema"
                )))
            }
        };
    }

    Ok(InvariantPathSupport::Supported)
}

fn join_field_path(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}.{segment}")
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::{extract_invariants, invariant_path_support, DeltaInvariant, InvariantPathSupport};
    use crate::spec::StructType;

    #[test]
    fn extract_invariants_reads_nested_struct_metadata() {
        let schema: StructType = serde_json::from_value(serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "person",
                "type": {
                    "type": "struct",
                    "fields": [{
                        "name": "age",
                        "type": "integer",
                        "nullable": true,
                        "metadata": {
                            "delta.invariants": "{\"expression\":{\"expression\":\"person.age >= 0\"}}"
                        }
                    }]
                },
                "nullable": true,
                "metadata": {}
            }]
        }))
        .unwrap();

        assert_eq!(
            extract_invariants(&schema).unwrap(),
            vec![DeltaInvariant {
                field_path: "person.age".to_string(),
                sql: "person.age >= 0".to_string(),
            }]
        );
    }

    #[test]
    fn invariant_path_support_rejects_collection_paths() {
        let schema: StructType = serde_json::from_value(serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "items",
                "type": {
                    "type": "array",
                    "elementType": {
                        "type": "struct",
                        "fields": [{
                            "name": "value",
                            "type": "integer",
                            "nullable": true,
                            "metadata": {}
                        }]
                    },
                    "containsNull": true
                },
                "nullable": true,
                "metadata": {}
            }]
        }))
        .unwrap();

        assert_eq!(
            invariant_path_support(&schema, "items.element.value").unwrap(),
            InvariantPathSupport::UnsupportedCollection
        );
    }
}
