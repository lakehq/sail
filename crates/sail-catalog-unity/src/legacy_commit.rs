// SPDX-License-Identifier: Apache-2.0

//! The table metadata of a commit in the legacy Delta commit API of Unity Catalog,
//! derived from the metadata action of the commit in the Delta log.

use std::collections::HashSet;

use sail_common_datafusion::catalog::delta::DeltaMetadataInfo;

use crate::data_type::flatten_spark_field_metadata;

type JsonObject = serde_json::Map<String, serde_json::Value>;

/// The type of a column in the legacy commit API.
struct ColumnType {
    type_text: String,
    type_name: &'static str,
    type_precision: Option<i32>,
    type_scale: Option<i32>,
}

/// A field of a struct type in the schema of the Delta log.
struct Field<'a> {
    name: &'a str,
    data_type: &'a serde_json::Value,
    nullable: bool,
    metadata: &'a JsonObject,
}

/// Returns the `metadata` of a commit in the legacy commit API, with the columns, the
/// comment, and the properties of the table.
pub(crate) fn legacy_commit_metadata(
    metadata: &DeltaMetadataInfo,
) -> Result<serde_json::Value, String> {
    let schema = serde_json::from_str::<serde_json::Value>(&metadata.schema_string)
        .map_err(|e| format!("invalid Delta table schema: {e}"))?;
    let columns = struct_fields(&schema)?
        .iter()
        .enumerate()
        .map(|(position, field)| column_info(metadata, field, position))
        .collect::<Result<Vec<_>, _>>()?;

    let mut out = JsonObject::new();
    if let Some(description) = &metadata.description {
        out.insert(
            "description".to_string(),
            serde_json::Value::String(description.clone()),
        );
    }
    out.insert(
        "schema".to_string(),
        serde_json::json!({
            "columns": columns,
        }),
    );
    if !metadata.configuration.is_empty() {
        out.insert(
            "properties".to_string(),
            serde_json::json!({
                "properties": metadata.configuration,
            }),
        );
    }
    Ok(serde_json::Value::Object(out))
}

/// Returns the fields of a struct type, which have unique names.
fn struct_fields(data_type: &serde_json::Value) -> Result<Vec<Field<'_>>, String> {
    let invalid = |message: &str| format!("invalid Delta struct type: {message}: {data_type}");
    let fields = data_type
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid("missing fields"))?;
    let mut names = HashSet::new();
    fields
        .iter()
        .map(|field| {
            let field = Field {
                name: field
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| invalid("missing field name"))?,
                data_type: field
                    .get("type")
                    .ok_or_else(|| invalid("missing field type"))?,
                nullable: field
                    .get("nullable")
                    .and_then(serde_json::Value::as_bool)
                    .ok_or_else(|| invalid("missing field nullability"))?,
                metadata: field
                    .get("metadata")
                    .and_then(serde_json::Value::as_object)
                    .ok_or_else(|| invalid("missing field metadata"))?,
            };
            if !names.insert(field.name) {
                return Err(format!("duplicate Delta field name: {}", field.name));
            }
            Ok(field)
        })
        .collect()
}

fn column_info(
    metadata: &DeltaMetadataInfo,
    field: &Field,
    position: usize,
) -> Result<serde_json::Value, String> {
    let column_type = column_type(field.data_type)?;
    let mut column = serde_json::json!({
        "name": field.name,
        "nullable": field.nullable,
        "position": i32::try_from(position).unwrap_or(i32::MAX),
        "type_text": column_type.type_text,
        "type_json": struct_field_type_json(field)?.to_string(),
        "type_name": column_type.type_name,
    });

    if let Some(precision) = column_type.type_precision {
        column["type_precision"] = serde_json::json!(precision);
    }
    if let Some(scale) = column_type.type_scale {
        column["type_scale"] = serde_json::json!(scale);
    }
    if let Some(partition_index) = metadata
        .partition_columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(field.name))
    {
        column["partition_index"] =
            serde_json::json!(i32::try_from(partition_index).unwrap_or(i32::MAX));
    }
    if let Some(serde_json::Value::String(comment)) = field.metadata.get("comment") {
        column["comment"] = serde_json::Value::String(comment.clone());
    }

    Ok(column)
}

/// Returns the field in the form of a Spark struct field, with the entries of the Spark
/// field metadata as entries of the field metadata.
fn struct_field_type_json(field: &Field) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!({
        "name": field.name,
        "type": spark_data_type_json(field.data_type)?,
        "nullable": field.nullable,
        "metadata": flatten_spark_field_metadata(field.metadata)?,
    }))
}

fn spark_data_type_json(data_type: &serde_json::Value) -> Result<serde_json::Value, String> {
    match data_type {
        serde_json::Value::String(name) => {
            Ok(serde_json::Value::String(match primitive_type(name)?.0 {
                Primitive::Named { .. } => name.clone(),
                decimal @ Primitive::Decimal { .. } => decimal.type_text(),
            }))
        }
        _ => match complex_type(data_type)? {
            ComplexType::Array {
                element_type,
                contains_null,
            } => Ok(serde_json::json!({
                "type": "array",
                "elementType": spark_data_type_json(element_type)?,
                "containsNull": contains_null,
            })),
            ComplexType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(struct_field_type_json)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(serde_json::json!({
                    "type": "struct",
                    "fields": fields,
                }))
            }
            ComplexType::Map {
                key_type,
                value_type,
                value_contains_null,
            } => Ok(serde_json::json!({
                "type": "map",
                "keyType": spark_data_type_json(key_type)?,
                "valueType": spark_data_type_json(value_type)?,
                "valueContainsNull": value_contains_null,
            })),
        },
    }
}

fn column_type(data_type: &serde_json::Value) -> Result<ColumnType, String> {
    match data_type {
        serde_json::Value::String(name) => {
            let (primitive, type_name) = primitive_type(name)?;
            let (type_precision, type_scale) = match primitive {
                Primitive::Decimal { precision, scale } => {
                    (Some(i32::from(precision)), Some(i32::from(scale)))
                }
                Primitive::Named { .. } => (None, None),
            };
            Ok(ColumnType {
                type_text: primitive.type_text(),
                type_name,
                type_precision,
                type_scale,
            })
        }
        _ => match complex_type(data_type)? {
            ComplexType::Array { element_type, .. } => Ok(ColumnType {
                type_text: format!("array<{}>", column_type(element_type)?.type_text),
                type_name: "ARRAY",
                type_precision: None,
                type_scale: None,
            }),
            ComplexType::Struct(fields) => {
                let mut type_text_parts = Vec::new();
                for field in &fields {
                    let field_type = column_type(field.data_type)?;
                    type_text_parts.push(format!("{}:{}", field.name, field_type.type_text));
                }
                Ok(ColumnType {
                    type_text: format!("struct<{}>", type_text_parts.join(",")),
                    type_name: "STRUCT",
                    type_precision: None,
                    type_scale: None,
                })
            }
            ComplexType::Map {
                key_type,
                value_type,
                ..
            } => Ok(ColumnType {
                type_text: format!(
                    "map<{},{}>",
                    column_type(key_type)?.type_text,
                    column_type(value_type)?.type_text
                ),
                type_name: "MAP",
                type_precision: None,
                type_scale: None,
            }),
        },
    }
}

/// A primitive type of the Delta log.
#[derive(Clone, Copy)]
enum Primitive {
    Named {
        /// The name of the type in the type text of a column.
        text: &'static str,
    },
    Decimal {
        precision: u8,
        scale: u8,
    },
}

impl Primitive {
    fn type_text(self) -> String {
        match self {
            Self::Named { text } => text.to_string(),
            Self::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        }
    }
}

/// Returns a primitive type of the Delta log and its type name in the legacy commit API.
fn primitive_type(name: &str) -> Result<(Primitive, &'static str), String> {
    let named = |text, type_name| Ok((Primitive::Named { text }, type_name));
    match name {
        "string" => named("string", "STRING"),
        "long" => named("long", "LONG"),
        "integer" => named("int", "INT"),
        "short" => named("short", "SHORT"),
        "byte" => named("byte", "BYTE"),
        "float" => named("float", "FLOAT"),
        "double" => named("double", "DOUBLE"),
        "boolean" => named("boolean", "BOOLEAN"),
        "binary" => named("binary", "BINARY"),
        "date" => named("date", "DATE"),
        "timestamp" => named("timestamp", "TIMESTAMP"),
        "timestamp_ntz" => named("timestamp_ntz", "TIMESTAMP_NTZ"),
        "variant" => Err("Unity Catalog metadata payloads for Delta variant columns".to_string()),
        _ => {
            let (precision, scale) =
                decimal_type(name).ok_or_else(|| format!("invalid Delta data type: {name}"))?;
            Ok((Primitive::Decimal { precision, scale }, "DECIMAL"))
        }
    }
}

/// Parses a decimal type of the Delta log, such as `decimal(10,2)`.
fn decimal_type(name: &str) -> Option<(u8, u8)> {
    let inner = name.strip_prefix("decimal(")?.strip_suffix(')')?;
    let mut parts = inner.split(',');
    let precision = parts.next()?.trim().parse::<u8>().ok()?;
    let scale = parts.next()?.trim().parse::<u8>().ok()?;
    ((1..=38).contains(&precision) && scale <= precision).then_some((precision, scale))
}

enum ComplexType<'a> {
    Array {
        element_type: &'a serde_json::Value,
        contains_null: bool,
    },
    Struct(Vec<Field<'a>>),
    Map {
        key_type: &'a serde_json::Value,
        value_type: &'a serde_json::Value,
        value_contains_null: bool,
    },
}

fn complex_type(data_type: &serde_json::Value) -> Result<ComplexType<'_>, String> {
    let invalid = || format!("invalid Delta data type: {data_type}");
    let get = |key: &str| data_type.get(key).ok_or_else(invalid);
    let flag = |key: &str| get(key)?.as_bool().ok_or_else(invalid);
    match data_type.get("type").and_then(serde_json::Value::as_str) {
        Some("array") => Ok(ComplexType::Array {
            element_type: get("elementType")?,
            contains_null: flag("containsNull")?,
        }),
        Some("struct") => Ok(ComplexType::Struct(struct_fields(data_type)?)),
        Some("map") => Ok(ComplexType::Map {
            key_type: get("keyType")?,
            value_type: get("valueType")?,
            value_contains_null: flag("valueContainsNull")?,
        }),
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(fields: serde_json::Value, partition_columns: &[&str]) -> DeltaMetadataInfo {
        DeltaMetadataInfo {
            schema_string: serde_json::json!({"type": "struct", "fields": fields}).to_string(),
            partition_columns: partition_columns.iter().map(|x| x.to_string()).collect(),
            configuration: Default::default(),
            description: None,
        }
    }

    fn columns(metadata: &DeltaMetadataInfo) -> Result<serde_json::Value, String> {
        Ok(legacy_commit_metadata(metadata)?["schema"]["columns"].clone())
    }

    #[test]
    fn test_legacy_commit_metadata_table() -> Result<(), String> {
        let mut metadata = metadata(
            serde_json::json!([{"name": "id", "type": "long", "nullable": false, "metadata": {}}]),
            &[],
        );
        assert_eq!(
            legacy_commit_metadata(&metadata)?,
            serde_json::json!({"schema": {"columns": [{
                "name": "id",
                "nullable": false,
                "position": 0,
                "type_text": "long",
                "type_json": r#"{"name":"id","type":"long","nullable":false,"metadata":{}}"#,
                "type_name": "LONG"
            }]}})
        );
        metadata.description = Some("events".to_string());
        metadata.configuration = [("owner".to_string(), "a".to_string())].into();
        let out = legacy_commit_metadata(&metadata)?;
        assert_eq!(out["description"], serde_json::json!("events"));
        assert_eq!(
            out["properties"],
            serde_json::json!({"properties": {"owner": "a"}})
        );
        Ok(())
    }

    #[test]
    fn test_legacy_commit_metadata_primitive_types() -> Result<(), String> {
        for (delta, type_text, type_name) in [
            ("string", "string", "STRING"),
            ("long", "long", "LONG"),
            ("integer", "int", "INT"),
            ("short", "short", "SHORT"),
            ("byte", "byte", "BYTE"),
            ("float", "float", "FLOAT"),
            ("double", "double", "DOUBLE"),
            ("boolean", "boolean", "BOOLEAN"),
            ("binary", "binary", "BINARY"),
            ("date", "date", "DATE"),
            ("timestamp", "timestamp", "TIMESTAMP"),
            ("timestamp_ntz", "timestamp_ntz", "TIMESTAMP_NTZ"),
        ] {
            let metadata = metadata(
                serde_json::json!([{"name": "c", "type": delta, "nullable": true, "metadata": {}}]),
                &[],
            );
            assert_eq!(
                columns(&metadata)?,
                serde_json::json!([{
                    "name": "c",
                    "nullable": true,
                    "position": 0,
                    "type_text": type_text,
                    "type_json": format!(
                        r#"{{"name":"c","type":"{delta}","nullable":true,"metadata":{{}}}}"#
                    ),
                    "type_name": type_name
                }]),
                "{delta}"
            );
        }
        // A decimal type is written in its canonical form.
        let metadata = metadata(
            serde_json::json!([{"name": "d", "type": "decimal(10, 2)", "nullable": true, "metadata": {}}]),
            &[],
        );
        assert_eq!(
            columns(&metadata)?,
            serde_json::json!([{
                "name": "d",
                "nullable": true,
                "position": 0,
                "type_text": "decimal(10,2)",
                "type_json": r#"{"name":"d","type":"decimal(10,2)","nullable":true,"metadata":{}}"#,
                "type_name": "DECIMAL",
                "type_precision": 10,
                "type_scale": 2
            }])
        );
        Ok(())
    }

    #[test]
    fn test_legacy_commit_metadata_nested_types() -> Result<(), String> {
        let metadata = metadata(
            serde_json::json!([
                {"name": "x", "type": "integer", "nullable": true, "metadata": {}},
                {
                    "name": "M",
                    "type": {
                        "type": "map",
                        "valueContainsNull": false,
                        "keyType": "string",
                        "valueType": {
                            "containsNull": true,
                            "type": "array",
                            "elementType": "decimal(5,1)"
                        }
                    },
                    "nullable": true,
                    "metadata": {
                        "comment": "raw",
                        "SPARK::metadata::json": r#"{"comment":"spark","k":1}"#,
                        "delta.columnMapping.id": 2
                    }
                },
                {
                    "name": "s",
                    "type": {"type": "struct", "fields": [
                        {"name": "a", "type": "integer", "nullable": false, "metadata": {}},
                        {"name": "b", "type": {"type": "struct", "fields": [
                            {
                                "name": "c",
                                "type": "timestamp_ntz",
                                "nullable": true,
                                "metadata": {"SPARK::metadata::json": {"k": true}}
                            }
                        ]}, "nullable": true, "metadata": {}}
                    ]},
                    "nullable": false,
                    "metadata": {}
                }
            ]),
            &["s", "m"],
        );
        let columns = columns(&metadata)?;
        assert_eq!(columns[1]["type_text"], "map<string,array<decimal(5,1)>>");
        assert_eq!(columns[1]["type_name"], "MAP");
        assert_eq!(columns[1]["comment"], "raw");
        assert_eq!(columns[1]["partition_index"], 1);
        assert_eq!(columns[1].get("type_precision"), None);
        assert_eq!(
            columns[1]["type_json"],
            r#"{"name":"M","type":{"type":"map","keyType":"string","valueType":{"type":"array","elementType":"decimal(5,1)","containsNull":true},"valueContainsNull":false},"nullable":true,"metadata":{"comment":"spark","k":1,"delta.columnMapping.id":2}}"#
        );
        assert_eq!(
            columns[2]["type_text"],
            "struct<a:int,b:struct<c:timestamp_ntz>>"
        );
        assert_eq!(columns[2]["type_name"], "STRUCT");
        assert_eq!(columns[2]["position"], 2);
        assert_eq!(columns[2]["partition_index"], 0);
        assert_eq!(
            columns[2]["type_json"],
            r#"{"name":"s","type":{"type":"struct","fields":[{"name":"a","type":"integer","nullable":false,"metadata":{}},{"name":"b","type":{"type":"struct","fields":[{"name":"c","type":"timestamp_ntz","nullable":true,"metadata":{"k":true}}]},"nullable":true,"metadata":{}}]},"nullable":false,"metadata":{}}"#
        );
        Ok(())
    }

    #[test]
    fn test_legacy_commit_metadata_errors() {
        let field = |data_type: serde_json::Value, field_metadata: serde_json::Value| {
            metadata(
                serde_json::json!([{"name": "c", "type": data_type, "nullable": true, "metadata": field_metadata}]),
                &[],
            )
        };
        for (metadata, expected) in [
            (
                field(serde_json::json!("variant"), serde_json::json!({})),
                "variant columns",
            ),
            (
                field(
                    serde_json::json!({"type": "array", "elementType": "variant", "containsNull": true}),
                    serde_json::json!({}),
                ),
                "variant columns",
            ),
            (
                field(serde_json::json!("decimal(39,0)"), serde_json::json!({})),
                "invalid Delta data type",
            ),
            (
                field(serde_json::json!("interval"), serde_json::json!({})),
                "invalid Delta data type",
            ),
            (
                field(
                    serde_json::json!("string"),
                    serde_json::json!({"SPARK::metadata::json": "{"}),
                ),
                "contains invalid JSON",
            ),
            (
                field(
                    serde_json::json!("string"),
                    serde_json::json!({"SPARK::metadata::json": "[]"}),
                ),
                "must contain a JSON object",
            ),
            (
                metadata(
                    serde_json::json!([
                        {"name": "c", "type": "long", "nullable": true, "metadata": {}},
                        {"name": "c", "type": "long", "nullable": true, "metadata": {}}
                    ]),
                    &[],
                ),
                "duplicate Delta field name",
            ),
            (
                metadata(
                    serde_json::json!([{"name": "c", "type": "long", "nullable": true}]),
                    &[],
                ),
                "missing field metadata",
            ),
        ] {
            let result = legacy_commit_metadata(&metadata);
            assert!(
                matches!(&result, Err(error) if error.contains(expected)),
                "{result:?}"
            );
        }
    }
}
