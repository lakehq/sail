// SPDX-License-Identifier: Apache-2.0

use arrow_schema::{DataType, Fields, Schema as ArrowSchema};
use datafusion_common::{DataFusionError, Result, plan_err};
use object_store::path::Path;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::schema::types::{SchemaDescriptor, TypePtr};
use parquet::schema::visitor::TypeVisitor;
use serde_json::Value;

use crate::spec::{NestedField, Schema, StructType, Type};

pub(super) struct ImportSchema {
    iceberg: Schema,
}

impl ImportSchema {
    pub(super) fn new(schema: &Schema) -> Self {
        Self {
            iceberg: schema.clone(),
        }
    }

    pub(super) fn validate_parquet(&self, actual: &SchemaDescriptor, path: &Path) -> Result<()> {
        let arrow = parquet::arrow::parquet_to_arrow_schema(actual, None)?;
        if has_physical_ids(actual.root_schema()) {
            let mut check = ParquetIds(path);
            for child in actual.root_schema().get_fields() {
                check.field(
                    child,
                    self.iceberg
                        .as_struct()
                        .field_by_name(child.name())
                        .map(AsRef::as_ref),
                )?;
            }
        }
        validate_struct_names(arrow.fields(), self.iceberg.as_struct(), path, "Parquet")
    }

    pub(super) fn validate_avro(
        &self,
        header: &[u8],
        arrow: &ArrowSchema,
        path: &Path,
    ) -> Result<()> {
        self.validate_avro_ids(header, path)?;
        validate_struct_names(arrow.fields(), self.iceberg.as_struct(), path, "Avro")
    }

    fn validate_avro_ids(&self, header: &[u8], path: &Path) -> Result<()> {
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

fn has_physical_ids(field: &parquet::schema::types::Type) -> bool {
    field.get_basic_info().has_id()
        || (field.is_group()
            && field
                .get_fields()
                .iter()
                .any(|child| has_physical_ids(child)))
}

// The Parquet visitor unwraps both standard and legacy LIST encodings. Read
// IDs from the physical element: Arrow can synthesize a legacy element without its ID.
struct ParquetIds<'a>(&'a Path);

impl ParquetIds<'_> {
    fn field(&mut self, actual: &TypePtr, expected: Option<&NestedField>) -> ParquetResult<()> {
        let info = actual.get_basic_info();
        if info.has_id().then(|| info.id()) != expected.map(|f| f.id) {
            return Err(ParquetError::General(format!(
                "Incompatible Parquet field ID for {} in {}",
                actual.name(),
                self.0
            )));
        }
        self.dispatch(actual.clone(), expected.map(|f| f.field_type.as_ref()))
    }
}

impl TypeVisitor<(), Option<&Type>> for ParquetIds<'_> {
    fn visit_primitive(&mut self, _: TypePtr, _: Option<&Type>) -> ParquetResult<()> {
        Ok(())
    }

    fn visit_struct(&mut self, group: TypePtr, expected: Option<&Type>) -> ParquetResult<()> {
        for child in group.get_fields() {
            let field = match expected {
                Some(Type::Struct(s)) => s.field_by_name(child.name()),
                _ => None,
            };
            self.field(child, field.map(AsRef::as_ref))?;
        }
        Ok(())
    }

    fn visit_list_with_item(
        &mut self,
        _: TypePtr,
        item: TypePtr,
        expected: Option<&Type>,
    ) -> ParquetResult<()> {
        let element = match expected {
            Some(Type::List(list)) => Some(list.element_field.as_ref()),
            _ => None,
        };
        self.field(&item, element)
    }

    fn visit_map(&mut self, group: TypePtr, expected: Option<&Type>) -> ParquetResult<()> {
        let entries = &group.get_fields()[0];
        let [key, value] = entries.get_fields() else {
            // Arrow exposes a key-only Parquet map as a list.
            return self.visit_list(group, expected);
        };
        let map = match expected {
            Some(Type::Map(map)) => Some(map),
            _ => None,
        };
        self.field(key, map.map(|m| m.key_field.as_ref()))?;
        self.field(value, map.map(|m| m.value_field.as_ref()))
    }
}

fn validate_struct_names(
    actual: &Fields,
    expected: &StructType,
    path: &Path,
    format: &str,
) -> Result<()> {
    for field in actual {
        if let Some(target) = expected.field_by_name(field.name()) {
            validate_nested_names(field.data_type(), &target.field_type, path, format)?;
        } else if let Some(target) = expected
            .fields()
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(field.name()))
        {
            return plan_err!(
                "{format} field name {} differs in case from {} in {path}",
                field.name(),
                target.name
            );
        }
    }
    Ok(())
}

fn validate_nested_names(
    actual: &DataType,
    expected: &Type,
    path: &Path,
    format: &str,
) -> Result<()> {
    match (actual, expected) {
        (DataType::Struct(fields), Type::Struct(s)) => {
            validate_struct_names(fields, s, path, format)
        }
        (
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _),
            Type::List(list),
        ) => validate_nested_names(f.data_type(), &list.element_field.field_type, path, format),
        (DataType::Map(entries, _), Type::Map(map)) => {
            if let DataType::Struct(pair) = entries.data_type() {
                validate_nested_names(
                    pair[0].data_type(),
                    &map.key_field.field_type,
                    path,
                    format,
                )?;
                validate_nested_names(
                    pair[1].data_type(),
                    &map.value_field.field_type,
                    path,
                    format,
                )?;
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parquet_collection_ids_follow_logical_roles() -> Result<(), Box<dyn std::error::Error>> {
        use parquet::schema::parser::parse_message_type;

        let target = ImportSchema::new(&serde_json::from_value(json!({
            "type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "xs", "required": false, "type": {
                    "type": "list", "element-id": 3, "element-required": true, "element": "long"
                }},
                {"id": 2, "name": "m", "required": false, "type": {
                    "type": "map", "key-id": 4, "key": "long", "value-id": 5, "value": "long", "value-required": false
                }}
            ]
        }))?);
        let path = Path::from("part.parquet");
        for element in [
            "repeated int64 item = 3;",
            "repeated group bag { required int64 element = 3; }",
        ] {
            let message = format!(
                "message row {{
                optional group xs (LIST) = 1 {{ {element} }}
                optional group m (MAP) = 2 {{ repeated group pairs {{
                    required int64 k = 4; optional int64 v = 5;
                }} }}
            }}"
            );
            for (physical, valid) in [
                (message.clone(), true),
                (message.replace("= 3;", "= 8;"), false),
                (
                    message.replace("k = 4", "k = 5").replace("v = 5", "v = 4"),
                    false,
                ),
            ] {
                let schema = SchemaDescriptor::new(parse_message_type(&physical)?.into());
                assert_eq!(
                    target.validate_parquet(&schema, &path).is_ok(),
                    valid,
                    "{physical}"
                );
            }
        }
        Ok(())
    }

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
        }))?);
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
        target.validate_avro_ids(&serde_json::to_vec(&writer)?, &path)?;
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
                    .validate_avro_ids(&serde_json::to_vec(&invalid)?, &path)
                    .is_err()
            );
        }
        Ok(())
    }
}
