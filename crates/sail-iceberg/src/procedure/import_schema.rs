// SPDX-License-Identifier: Apache-2.0

use arrow_schema::{DataType, Fields};
use datafusion_common::{DataFusionError, Result, plan_err};
use object_store::path::Path;
use parquet::schema::types::SchemaDescriptor;
use serde_json::Value;

use crate::spec::{Schema, StructType, Type};

pub(super) struct ImportSchema {
    iceberg: Schema,
    parquet: SchemaDescriptor,
}

impl ImportSchema {
    pub(super) fn new(schema: &Schema) -> Result<Self> {
        let arrow = crate::datasource::type_converter::iceberg_schema_to_arrow(schema)?;
        Ok(Self {
            iceberg: schema.clone(),
            parquet: parquet::arrow::ArrowSchemaConverter::new().convert(&arrow)?,
        })
    }

    pub(super) fn validate_parquet(&self, actual: &SchemaDescriptor, path: &Path) -> Result<()> {
        if has_field_ids(actual.root_schema()) {
            validate_field_ids(actual.root_schema(), Some(self.parquet.root_schema()), path)?;
        }
        // Arrow normalizes the physical LIST and MAP wrappers, whose names vary by writer.
        let arrow = parquet::arrow::parquet_to_arrow_schema(actual, None)?;
        validate_struct_names(arrow.fields(), self.iceberg.as_struct(), path)
    }

    pub(super) fn validate_avro(&self, header: &[u8], path: &Path) -> Result<()> {
        let schema: Value = serde_json::from_slice(header)
            .map_err(|e| DataFusionError::Plan(format!("Invalid Avro schema in {path}: {e}")))?;
        let mut ids = AvroIds::default();
        ids.visit(&schema, "");
        if ids.fields.iter().any(|(_, id)| id.is_some()) {
            if ids.named_reference {
                return plan_err!(
                    "Cannot verify Avro field IDs in a named type reference in {path}"
                );
            }
            for (name, id) in ids.fields {
                let expected = self.avro_field_id(&name);
                if expected.is_none() || expected.map(i64::from) != id.and_then(Value::as_i64) {
                    return plan_err!("Incompatible Avro field ID for {name} in {path}");
                }
            }
        }
        Ok(())
    }

    fn avro_field_id(&self, path: &str) -> Option<i32> {
        // Avro identifiers cannot contain dots. Follow each structural component,
        // including elements of nested lists and both sides of maps.
        let mut parts = path.split('.');
        let mut field = self.iceberg.as_struct().field_by_name(parts.next()?)?;
        for part in parts {
            field = match field.field_type.as_ref() {
                Type::Struct(s) => s.field_by_name(part)?,
                Type::List(list) if part == "element" => &list.element_field,
                Type::Map(map) if part == "key" => &map.key_field,
                Type::Map(map) if part == "value" => &map.value_field,
                _ => return None,
            };
        }
        Some(field.id)
    }
}

fn validate_struct_names(actual: &Fields, expected: &StructType, path: &Path) -> Result<()> {
    for field in actual {
        if let Some(target) = expected.field_by_name(field.name()) {
            validate_nested_names(field.data_type(), &target.field_type, path)?;
        } else if let Some(target) = expected
            .fields()
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(field.name()))
        {
            return plan_err!(
                "Parquet field name {} differs in case from {} in {path}",
                field.name(),
                target.name
            );
        }
    }
    Ok(())
}

fn validate_nested_names(actual: &DataType, expected: &Type, path: &Path) -> Result<()> {
    match (actual, expected) {
        (DataType::Struct(fields), Type::Struct(s)) => validate_struct_names(fields, s, path),
        (
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _),
            Type::List(list),
        ) => validate_nested_names(f.data_type(), &list.element_field.field_type, path),
        (DataType::Map(entries, _), Type::Map(map)) => {
            if let DataType::Struct(pair) = entries.data_type() {
                validate_nested_names(pair[0].data_type(), &map.key_field.field_type, path)?;
                validate_nested_names(pair[1].data_type(), &map.value_field.field_type, path)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[derive(Default)]
struct AvroIds<'a> {
    fields: Vec<(String, Option<&'a Value>)>,
    named_reference: bool,
}

impl<'a> AvroIds<'a> {
    fn visit(&mut self, schema: &'a Value, parent: &str) {
        let child = |name: &str| {
            if parent.is_empty() {
                name.to_owned()
            } else {
                format!("{parent}.{name}")
            }
        };
        match schema {
            Value::Array(branches) => {
                for branch in branches {
                    self.visit(branch, parent);
                }
            }
            Value::Object(node) => match node.get("type").and_then(Value::as_str) {
                Some("record") => {
                    if let Some(fields) = schema["fields"].as_array() {
                        for field in fields {
                            if let Some(name) = field["name"].as_str() {
                                let name = child(name);
                                self.fields.push((name.clone(), field.get("field-id")));
                                self.visit(&field["type"], &name);
                            }
                        }
                    }
                }
                Some("array") if schema["logicalType"] == "map" => {
                    self.visit(&schema["items"], parent)
                }
                Some("array") => {
                    let element = child("element");
                    self.fields.push((element.clone(), node.get("element-id")));
                    self.visit(&schema["items"], &element);
                }
                Some("map") => {
                    self.fields.push((child("key"), node.get("key-id")));
                    let value = child("value");
                    self.fields.push((value.clone(), node.get("value-id")));
                    self.visit(&schema["values"], &value);
                }
                Some("enum" | "fixed") => {}
                _ => self.visit(&schema["type"], parent),
            },
            Value::String(name) => {
                self.named_reference |= !matches!(
                    name.as_str(),
                    "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string"
                );
            }
            _ => {}
        }
    }
}

// Files without IDs use the table's name mapping. Once IDs are present, check every
// physical field against the same named field in the target, including nested groups.
fn has_field_ids(field: &parquet::schema::types::Type) -> bool {
    field.get_basic_info().has_id()
        || (field.is_group() && field.get_fields().iter().any(|child| has_field_ids(child)))
}

fn validate_field_ids(
    field: &parquet::schema::types::Type,
    expected: Option<&parquet::schema::types::Type>,
    path: &object_store::path::Path,
) -> Result<()> {
    let id = |field: &parquet::schema::types::Type| {
        let info = field.get_basic_info();
        info.has_id().then(|| info.id())
    };
    if id(field) != expected.and_then(id) {
        return plan_err!(
            "Incompatible Parquet field ID for {} in {path}",
            field.name()
        );
    }
    if field.is_group() {
        for child in field.get_fields() {
            let target = expected
                .filter(|f| f.is_group())
                .and_then(|f| f.get_fields().iter().find(|e| e.name() == child.name()));
            validate_field_ids(child, target.map(AsRef::as_ref), path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn validate_avro_collection_ids() -> Result<(), Box<dyn std::error::Error>> {
        let target = ImportSchema::new(&serde_json::from_value(json!({
            "type": "struct", "schema-id": 0,
            "fields": [{"id": 1, "name": "m", "required": false, "type": {
                "type": "map", "key-id": 2, "key": "string", "value-id": 3, "value-required": false,
                "value": {"type": "list", "element-id": 4, "element-required": false,
                    "element": {"type": "struct", "fields": [
                        {"id": 5, "name": "x", "type": "long", "required": true}
                    ]}}
            }}]
        }))?)?;
        let writer = json!({"type": "record", "name": "row", "fields": [
            {"name": "m", "field-id": 1, "type": ["null", {
                "type": "map", "key-id": 2, "value-id": 3, "values": {
                    "type": "array", "element-id": 4, "items": {
                        "type": "record", "name": "item", "fields": [{"name": "x", "type": "long", "field-id": 5}]
                    }
                }
            }]}
        ]});
        let path = Path::from("part.avro");
        target.validate_avro(&serde_json::to_vec(&writer)?, &path)?;
        for attribute in [
            "/fields/0/type/1/key-id",
            "/fields/0/type/1/value-id",
            "/fields/0/type/1/values/element-id",
            "/fields/0/type/1/values/items/fields/0/field-id",
        ] {
            let mut invalid = writer.clone();
            *invalid
                .pointer_mut(attribute)
                .ok_or("missing fixture field")? = json!(9);
            assert!(
                target
                    .validate_avro(&serde_json::to_vec(&invalid)?, &path)
                    .is_err()
            );
        }
        Ok(())
    }
}
