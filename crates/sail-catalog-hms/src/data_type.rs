use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_common_datafusion::column_features::ColumnFeatureKey;
use serde_json::{json, Value};

pub fn arrow_to_hive_type(data_type: &DataType) -> CatalogResult<String> {
    match data_type {
        DataType::Null => Ok("void".to_string()),
        DataType::Boolean => Ok("boolean".to_string()),
        DataType::Int8 => Ok("tinyint".to_string()),
        DataType::Int16 => Ok("smallint".to_string()),
        DataType::Int32 => Ok("int".to_string()),
        DataType::Int64 => Ok("bigint".to_string()),
        // Note: Hive has no unsigned integer types. Unsigned Arrow types map to
        // the equivalent signed Hive type. Values exceeding the signed range will
        // be misinterpreted (lossy but intentional mapping).
        DataType::UInt8 => Ok("tinyint".to_string()),
        DataType::UInt16 => Ok("smallint".to_string()),
        DataType::UInt32 => Ok("int".to_string()),
        DataType::UInt64 => Ok("bigint".to_string()),
        DataType::Float16 | DataType::Float32 => Ok("float".to_string()),
        DataType::Float64 => Ok("double".to_string()),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            if precision > 38 {
                return Err(CatalogError::InvalidArgument(format!(
                    "Hive Metastore supports decimal precision up to 38, got {precision}"
                )));
            }
            if scale < 0 || scale as u8 > precision {
                return Err(CatalogError::InvalidArgument(format!(
                    "Invalid decimal scale {scale} for precision {precision}"
                )));
            }
            Ok(format!("decimal({precision},{scale})"))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("string".to_string()),
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => Ok("binary".to_string()),
        // HMS DATE is days since epoch. Arrow Date32 is days, Date64 is milliseconds.
        // Both map to the same HMS type since HMS has no sub-day date precision.
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Timestamp(_, _) => Ok("timestamp".to_string()),
        DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => Ok("string".to_string()),
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
            Ok(format!("array<{}>", arrow_to_hive_type(field.data_type())?))
        }
        DataType::Struct(fields) => {
            let fields: CatalogResult<Vec<String>> = fields
                .iter()
                .map(|field| {
                    Ok(format!(
                        "{}:{}",
                        field.name(),
                        arrow_to_hive_type(field.data_type())?
                    ))
                })
                .collect();
            Ok(format!("struct<{}>", fields?.join(",")))
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                if fields.len() == 2 {
                    return Ok(format!(
                        "map<{},{}>",
                        arrow_to_hive_type(fields[0].data_type())?,
                        arrow_to_hive_type(fields[1].data_type())?
                    ));
                }
            }
            Err(CatalogError::InvalidArgument(
                "Map type must have key and value fields".to_string(),
            ))
        }
        DataType::Dictionary(_, value_type) => arrow_to_hive_type(value_type),
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => Err(CatalogError::NotSupported(
            format!("Data type {data_type:?} is not supported by Hive Metastore catalog"),
        )),
    }
}

pub fn hive_type_to_arrow(type_str: &str) -> CatalogResult<DataType> {
    let type_str = type_str.trim().to_lowercase();

    if type_str.starts_with("decimal") {
        return parse_decimal_type(&type_str);
    }
    if type_str.starts_with("array<") {
        return parse_array_type(&type_str);
    }
    if type_str.starts_with("map<") {
        return parse_map_type(&type_str);
    }
    if type_str.starts_with("struct<") {
        return parse_struct_type(&type_str);
    }
    if type_str.starts_with("char(") || type_str.starts_with("varchar(") {
        return Ok(DataType::Utf8);
    }

    match type_str.as_str() {
        "void" | "null" => Ok(DataType::Null),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "tinyint" | "byte" => Ok(DataType::Int8),
        "smallint" | "short" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "bigint" | "long" => Ok(DataType::Int64),
        "float" | "real" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "string" | "varchar" | "char" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("UTC")),
        )),
        "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        other => Err(CatalogError::InvalidArgument(format!(
            "Unknown Hive type: {other}"
        ))),
    }
}

pub fn spark_struct_json_from_fields(fields: &[Field]) -> CatalogResult<Value> {
    let fields = fields
        .iter()
        .map(spark_field_json)
        .collect::<CatalogResult<Vec<_>>>()?;
    Ok(json!({
        "type": "struct",
        "fields": fields,
    }))
}

fn spark_struct_json_value_to_fields(value: &Value) -> CatalogResult<Vec<Field>> {
    let schema_type = value.get("type").and_then(Value::as_str);
    if schema_type != Some("struct") {
        return Err(CatalogError::External(
            "Spark schema JSON must be a struct".to_string(),
        ));
    }
    let fields = value
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| CatalogError::External("Spark schema JSON is missing fields".to_string()))?;
    fields
        .iter()
        .map(|field| {
            let name = field.get("name").and_then(Value::as_str).ok_or_else(|| {
                CatalogError::External("Spark schema field is missing name".to_string())
            })?;
            let nullable = field
                .get("nullable")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let data_type_value = field.get("type").ok_or_else(|| {
                CatalogError::External(format!("Spark schema field {name} is missing type"))
            })?;
            // Restore the full metadata map. String values are taken verbatim;
            // other JSON values (e.g. numbers or booleans written by foreign
            // engines) are kept as their JSON text.
            let mut metadata = std::collections::HashMap::new();
            if let Some(object) = field.get("metadata").and_then(Value::as_object) {
                for (key, value) in object {
                    let value = match value {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    metadata.insert(key.clone(), value);
                }
            }
            Ok(Field::new(
                name,
                spark_json_to_arrow_data_type(data_type_value)?,
                nullable,
            )
            .with_metadata(metadata))
        })
        .collect()
}

pub fn spark_struct_json_to_fields(schema: &str) -> CatalogResult<Vec<Field>> {
    let value: Value = serde_json::from_str(schema)
        .map_err(|e| CatalogError::External(format!("Failed to parse Spark schema JSON: {e}")))?;
    spark_struct_json_value_to_fields(&value)
}

/// The column feature keys that are serialized into the Spark schema JSON
/// so that they round-trip through the HMS table properties. This matches
/// the metadata produced by [`TableColumnStatus::field`] and excludes
/// sail-internal pipeline markers.
///
/// [`TableColumnStatus::field`]: sail_common_datafusion::catalog::TableColumnStatus::field
const SERIALIZED_COLUMN_FEATURE_KEYS: [ColumnFeatureKey; 6] = [
    ColumnFeatureKey::CurrentDefault,
    ColumnFeatureKey::GenerationExpression,
    ColumnFeatureKey::IdentityStart,
    ColumnFeatureKey::IdentityStep,
    ColumnFeatureKey::IdentityHighWaterMark,
    ColumnFeatureKey::IdentityAllowExplicitInsert,
];

fn spark_field_json(field: &Field) -> CatalogResult<Value> {
    // Serialize the comment and the column features (default values,
    // generation expressions, identity columns); other metadata keys are
    // not part of the catalog contract and are omitted. Entries are sorted
    // for deterministic output.
    let mut metadata = serde_json::Map::new();
    let mut entries: Vec<_> = field
        .metadata()
        .iter()
        .filter(|(key, _)| {
            *key == "comment"
                || SERIALIZED_COLUMN_FEATURE_KEYS
                    .iter()
                    .any(|k| k.as_str() == *key)
        })
        .collect();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (key, value) in entries {
        metadata.insert(key.clone(), Value::String(value.clone()));
    }
    Ok(json!({
        "name": field.name(),
        "type": spark_data_type_json(field.data_type())?,
        "nullable": field.is_nullable(),
        "metadata": metadata,
    }))
}

fn spark_data_type_json(data_type: &DataType) -> CatalogResult<Value> {
    let value = match data_type {
        DataType::Null => json!("void"),
        DataType::Boolean => json!("boolean"),
        DataType::Int8 => json!("byte"),
        DataType::Int16 => json!("short"),
        DataType::Int32 => json!("integer"),
        DataType::Int64 => json!("long"),
        // Note: Spark has no unsigned integer types. Unsigned Arrow types map
        // to the equivalent signed Spark JSON type. Values exceeding the signed
        // range will be misinterpreted. This matches Spark's behavior — Spark
        // reads unsigned Parquet columns as signed types (bug-for-bug parity).
        DataType::UInt8 => json!("byte"),
        DataType::UInt16 => json!("short"),
        DataType::UInt32 => json!("integer"),
        DataType::UInt64 => json!("long"),
        DataType::Float16 | DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => json!(format!("decimal({precision},{scale})")),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => json!("string"),
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => json!("binary"),
        DataType::Date32 | DataType::Date64 => json!("date"),
        DataType::Timestamp(_, timezone) => {
            if timezone.is_some() {
                json!("timestamp")
            } else {
                json!("timestamp_ntz")
            }
        }
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => json!({
            "type": "array",
            "elementType": spark_data_type_json(field.data_type())?,
            "containsNull": field.is_nullable(),
        }),
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| spark_field_json(field.as_ref()))
                .collect::<CatalogResult<Vec<_>>>()?;
            json!({
                "type": "struct",
                "fields": fields,
            })
        }
        DataType::Map(field, _) => {
            let DataType::Struct(fields) = field.data_type() else {
                return Err(CatalogError::InvalidArgument(
                    "Map type must have key and value fields".to_string(),
                ));
            };
            if fields.len() != 2 {
                return Err(CatalogError::InvalidArgument(
                    "Map type must have key and value fields".to_string(),
                ));
            }
            json!({
                "type": "map",
                "keyType": spark_data_type_json(fields[0].data_type())?,
                "valueType": spark_data_type_json(fields[1].data_type())?,
                "valueContainsNull": fields[1].is_nullable(),
            })
        }
        DataType::Dictionary(_, value_type) => spark_data_type_json(value_type)?,
        DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Union(_, _)
        | DataType::RunEndEncoded(_, _) => {
            return Err(CatalogError::NotSupported(format!(
                "Data type {data_type:?} is not supported by Spark schema JSON"
            )));
        }
    };
    Ok(value)
}

fn spark_json_to_arrow_data_type(value: &Value) -> CatalogResult<DataType> {
    if let Some(name) = value.as_str() {
        return match name {
            "void" => Ok(DataType::Null),
            "boolean" => Ok(DataType::Boolean),
            "byte" => Ok(DataType::Int8),
            "short" => Ok(DataType::Int16),
            "integer" => Ok(DataType::Int32),
            "long" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "string" => Ok(DataType::Utf8),
            "binary" => Ok(DataType::Binary),
            "date" => Ok(DataType::Date32),
            "timestamp" => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            )),
            "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            decimal if decimal.starts_with("decimal") => hive_type_to_arrow(decimal),
            other => Err(CatalogError::External(format!(
                "Unsupported Spark schema type: {other}"
            ))),
        };
    }

    let type_name = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| CatalogError::External("Spark complex type is missing type".to_string()))?;
    match type_name {
        "array" => {
            let element_type = value.get("elementType").ok_or_else(|| {
                CatalogError::External("Spark array type is missing elementType".to_string())
            })?;
            let contains_null = value
                .get("containsNull")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            Ok(DataType::List(Arc::new(Field::new_list_field(
                spark_json_to_arrow_data_type(element_type)?,
                contains_null,
            ))))
        }
        "struct" => {
            let fields = spark_struct_json_value_to_fields(value)?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        "map" => {
            let key_type = value.get("keyType").ok_or_else(|| {
                CatalogError::External("Spark map type is missing keyType".to_string())
            })?;
            let value_type = value.get("valueType").ok_or_else(|| {
                CatalogError::External("Spark map type is missing valueType".to_string())
            })?;
            let value_contains_null = value
                .get("valueContainsNull")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", spark_json_to_arrow_data_type(key_type)?, false),
                        Field::new(
                            "values",
                            spark_json_to_arrow_data_type(value_type)?,
                            value_contains_null,
                        ),
                    ])),
                    false,
                )),
                false,
            ))
        }
        other => Err(CatalogError::External(format!(
            "Unsupported Spark complex schema type: {other}"
        ))),
    }
}

fn parse_decimal_type(type_str: &str) -> CatalogResult<DataType> {
    if type_str == "decimal" {
        return Ok(DataType::Decimal128(10, 0));
    }

    let inner = type_str
        .strip_prefix("decimal(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| {
            CatalogError::InvalidArgument(format!("Invalid decimal type: {type_str}"))
        })?;

    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    match parts.as_slice() {
        [precision] => Ok(DataType::Decimal128(
            precision.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid decimal precision: {precision}"))
            })?,
            0,
        )),
        [precision, scale] => Ok(DataType::Decimal128(
            precision.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid decimal precision: {precision}"))
            })?,
            scale.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid decimal scale: {scale}"))
            })?,
        )),
        _ => Err(CatalogError::InvalidArgument(format!(
            "Invalid decimal type: {type_str}"
        ))),
    }
}

fn parse_array_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("array<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid array type: {type_str}")))?;
    Ok(DataType::List(Arc::new(Field::new_list_field(
        hive_type_to_arrow(inner)?,
        true,
    ))))
}

fn parse_map_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("map<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid map type: {type_str}")))?;
    let (key_type, value_type) = split_top_level_two(inner)?;
    Ok(DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", hive_type_to_arrow(key_type)?, false),
                Field::new("values", hive_type_to_arrow(value_type)?, true),
            ])),
            false,
        )),
        false,
    ))
}

fn parse_struct_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("struct<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid struct type: {type_str}")))?;

    let mut fields = Vec::new();
    for field_str in split_top_level(inner)? {
        let (name, field_type) = field_str.split_once(':').ok_or_else(|| {
            CatalogError::InvalidArgument(format!("Invalid struct field: {field_str}"))
        })?;
        fields.push(Field::new(name, hive_type_to_arrow(field_type)?, true));
    }
    Ok(DataType::Struct(Fields::from(fields)))
}

fn split_top_level_two(input: &str) -> CatalogResult<(&str, &str)> {
    let parts = split_top_level(input)?;
    if parts.len() != 2 {
        return Err(CatalogError::InvalidArgument(format!(
            "Expected exactly two type parameters: {input}"
        )));
    }
    Ok((parts[0], parts[1]))
}

fn split_top_level(input: &str) -> CatalogResult<Vec<&str>> {
    let mut angle_depth = 0;
    let mut paren_depth = 0;
    let mut start = 0;
    let mut parts = Vec::new();

    for (idx, ch) in input.char_indices() {
        match ch {
            '<' => angle_depth += 1,
            '>' => {
                if angle_depth == 0 {
                    return Err(CatalogError::InvalidArgument(format!(
                        "Unbalanced brackets in type string (negative depth): {input}"
                    )));
                }
                angle_depth -= 1;
            }
            '(' => paren_depth += 1,
            ')' => {
                if paren_depth == 0 {
                    return Err(CatalogError::InvalidArgument(format!(
                        "Unbalanced brackets in type string (negative depth): {input}"
                    )));
                }
                paren_depth -= 1;
            }
            ',' if angle_depth == 0 && paren_depth == 0 => {
                parts.push(input[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }

    if angle_depth != 0 || paren_depth != 0 {
        return Err(CatalogError::InvalidArgument(format!(
            "Unbalanced brackets in type string (non-zero final depth: angle={angle_depth}, paren={paren_depth}): {input}"
        )));
    }

    if start < input.len() {
        parts.push(input[start..].trim());
    }

    Ok(parts)
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::panic)]

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields};
    use serde_json::json;

    use super::{arrow_to_hive_type, hive_type_to_arrow, spark_struct_json_from_fields};

    #[test]
    fn test_arrow_to_hive_struct() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        assert_eq!(
            arrow_to_hive_type(&data_type).unwrap(),
            "struct<id:bigint,name:string>"
        );
    }

    #[test]
    fn test_hive_to_arrow_nested_types() {
        let data_type = hive_type_to_arrow("array<struct<id:int,name:string>>").unwrap();
        assert!(matches!(data_type, DataType::List(_)));
    }

    #[test]
    fn test_hive_to_arrow_nested_decimal_types() {
        let data_type = hive_type_to_arrow(
            "struct<items:array<struct<label:string,weight:decimal(5,2)>>,attrs:map<string,array<int>>>",
        )
        .unwrap();
        assert!(matches!(data_type, DataType::Struct(_)));
    }

    #[test]
    fn test_arrow_to_spark_json_struct_type() {
        let schema = spark_struct_json_from_fields(&[
            Field::new("Id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
        ])
        .unwrap();

        assert_eq!(
            schema,
            json!({
                "type": "struct",
                "fields": [
                    {
                        "name": "Id",
                        "type": "long",
                        "nullable": false,
                        "metadata": {}
                    },
                    {
                        "name": "tags",
                        "type": {
                            "type": "array",
                            "elementType": "string",
                            "containsNull": true
                        },
                        "nullable": true,
                        "metadata": {}
                    }
                ]
            })
        );
    }

    #[test]
    fn test_dual_serialization_consistency_all_types() {
        // For each supported Arrow type, verify that arrow_to_hive_type and
        // spark_data_type_json produce compatible representations.
        let cases: Vec<(DataType, &str, &str)> = vec![
            // (Arrow type, expected Hive type, expected Spark JSON type string)
            (DataType::Null, "void", "void"),
            (DataType::Boolean, "boolean", "boolean"),
            (DataType::Int8, "tinyint", "byte"),
            (DataType::Int16, "smallint", "short"),
            (DataType::Int32, "int", "integer"),
            (DataType::Int64, "bigint", "long"),
            (DataType::UInt8, "tinyint", "byte"),
            (DataType::UInt16, "smallint", "short"),
            (DataType::UInt32, "int", "integer"),
            (DataType::UInt64, "bigint", "long"),
            (DataType::Float32, "float", "float"),
            (DataType::Float64, "double", "double"),
            (DataType::Utf8, "string", "string"),
            (DataType::LargeUtf8, "string", "string"),
            (DataType::Binary, "binary", "binary"),
            (DataType::Date32, "date", "date"),
            (DataType::Date64, "date", "date"),
            (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                "timestamp",
                "timestamp_ntz",
            ),
            (
                DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    Some(Arc::from("UTC")),
                ),
                "timestamp",
                "timestamp",
            ),
            (
                DataType::Decimal128(10, 2),
                "decimal(10,2)",
                "decimal(10,2)",
            ),
        ];

        for (arrow_type, expected_hive, expected_spark_str) in cases {
            let hive = arrow_to_hive_type(&arrow_type)
                .unwrap_or_else(|e| panic!("arrow_to_hive_type failed for {arrow_type:?}: {e}"));
            assert_eq!(hive, expected_hive, "Hive type mismatch for {arrow_type:?}");

            // Verify Spark JSON round-trip: write field, extract type string
            let field = Field::new("test_col", arrow_type.clone(), true);
            let schema = spark_struct_json_from_fields(&[field]).unwrap();
            let fields = schema.get("fields").unwrap().as_array().unwrap();
            let spark_type = fields[0].get("type").unwrap();
            if let Some(spark_str) = spark_type.as_str() {
                assert_eq!(
                    spark_str, expected_spark_str,
                    "Spark JSON type mismatch for {arrow_type:?}"
                );
            }
            // For complex types the JSON is an object; just verify it doesn't error
        }
    }

    #[test]
    fn test_spark_json_round_trip_complex_types() {
        // Struct with nested array, struct, and map -- all round-trip correctly
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "nested",
                DataType::Struct(Fields::from(vec![
                    Field::new("inner_id", DataType::Int32, false),
                    Field::new("value", DataType::Float64, true),
                ])),
                true,
            ),
            Field::new(
                "data",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Int32, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                false,
            ),
        ];

        let json = spark_struct_json_from_fields(&fields).unwrap();
        let json_str = serde_json::to_string(&json).unwrap();

        // Parse back
        let parsed = super::spark_struct_json_to_fields(&json_str).unwrap();
        assert_eq!(parsed.len(), 4);
        assert_eq!(parsed[0].name(), "id");
        assert_eq!(parsed[0].data_type(), &DataType::Int64);
        assert!(!parsed[0].is_nullable());
        assert_eq!(parsed[1].name(), "tags");
        assert!(matches!(parsed[1].data_type(), DataType::List(_)));
        assert!(parsed[1].is_nullable());
        assert_eq!(parsed[2].name(), "nested");
        assert!(matches!(parsed[2].data_type(), DataType::Struct(_)));
        assert!(parsed[2].is_nullable());
        assert_eq!(parsed[3].name(), "data");
        assert!(matches!(parsed[3].data_type(), DataType::Map(_, _)));
        assert!(!parsed[3].is_nullable());
    }

    #[test]
    fn test_spark_struct_json_rejects_malformed_json() {
        let error =
            super::spark_struct_json_to_fields("{\"type\":\"struct\",\"fields\":[").unwrap_err();
        assert!(error
            .to_string()
            .contains("Failed to parse Spark schema JSON"));
    }

    #[test]
    fn test_spark_json_preserves_exact_case_and_nullable() {
        let fields = vec![
            Field::new("CustomerID", DataType::Int64, false),
            Field::new("email_address", DataType::Utf8, true),
            Field::new("IsActive", DataType::Boolean, false),
        ];

        let json = spark_struct_json_from_fields(&fields).unwrap();
        let json_str = serde_json::to_string(&json).unwrap();
        let parsed = super::spark_struct_json_to_fields(&json_str).unwrap();

        assert_eq!(parsed[0].name(), "CustomerID");
        assert!(!parsed[0].is_nullable());
        assert_eq!(parsed[1].name(), "email_address");
        assert!(parsed[1].is_nullable());
        assert_eq!(parsed[2].name(), "IsActive");
        assert!(!parsed[2].is_nullable());
    }

    #[test]
    fn test_spark_json_omits_default_and_generated_always_as_but_preserves_comment() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("default".to_string(), "42".to_string());
        metadata.insert("generated_always_as".to_string(), "expr".to_string());
        metadata.insert("comment".to_string(), "a description".to_string());

        let field = Field::new("col", DataType::Int32, true).with_metadata(metadata);
        let schema = spark_struct_json_from_fields(&[field]).unwrap();
        let fields = schema.get("fields").unwrap().as_array().unwrap();
        let field_json = &fields[0];
        let meta = field_json.get("metadata").unwrap();

        // Only "comment" is serialized; default and generated_always_as are omitted
        assert_eq!(meta, &serde_json::json!({"comment": "a description"}));
    }

    #[test]
    fn test_spark_json_preserves_column_feature_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("CURRENT_DEFAULT".to_string(), "42".to_string());
        metadata.insert(
            "delta.generationExpression".to_string(),
            "id + 1".to_string(),
        );
        metadata.insert("comment".to_string(), "a description".to_string());

        let field = Field::new("col", DataType::Int32, true).with_metadata(metadata);
        let schema = spark_struct_json_from_fields(&[field]).unwrap();
        let fields = schema.get("fields").unwrap().as_array().unwrap();
        let field_json = &fields[0];
        let meta = field_json.get("metadata").unwrap();

        // Column features (defaults, generation expressions, identity columns)
        // are serialized so that they round-trip through the HMS Spark schema
        // properties.
        assert_eq!(
            meta,
            &serde_json::json!({
                "CURRENT_DEFAULT": "42",
                "comment": "a description",
                "delta.generationExpression": "id + 1",
            })
        );
    }

    #[test]
    fn test_spark_json_round_trip_field_metadata() {
        let nested = Field::new("inner", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([("comment".to_string(), "inner comment".to_string())]),
        );
        let fields = vec![
            Field::new("id", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([
                    ("CURRENT_DEFAULT".to_string(), "42".to_string()),
                    ("delta.identity.start".to_string(), "1".to_string()),
                ]),
            ),
            Field::new("nested", DataType::Struct(Fields::from(vec![nested])), true),
        ];

        let json = spark_struct_json_from_fields(&fields).unwrap();
        let json_str = serde_json::to_string(&json).unwrap();
        let parsed = super::spark_struct_json_to_fields(&json_str).unwrap();

        assert_eq!(
            parsed[0].metadata().get("CURRENT_DEFAULT"),
            Some(&"42".to_string())
        );
        assert_eq!(
            parsed[0].metadata().get("delta.identity.start"),
            Some(&"1".to_string())
        );
        let DataType::Struct(nested_fields) = parsed[1].data_type() else {
            panic!("expected struct data type");
        };
        assert_eq!(
            nested_fields[0].metadata().get("comment"),
            Some(&"inner comment".to_string())
        );
    }

    #[test]
    fn test_spark_json_foreign_metadata_values_to_strings() {
        // Metadata written by foreign engines may contain non-string JSON
        // values; they are restored as their JSON text.
        let json_str = concat!(
            r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"#,
            r#""metadata":{"CURRENT_DEFAULT":"'hello'","count":1,"flag":true}}]}"#,
        );
        let parsed = super::spark_struct_json_to_fields(json_str).unwrap();
        assert_eq!(
            parsed[0].metadata().get("CURRENT_DEFAULT"),
            Some(&"'hello'".to_string())
        );
        assert_eq!(parsed[0].metadata().get("count"), Some(&"1".to_string()));
        assert_eq!(parsed[0].metadata().get("flag"), Some(&"true".to_string()));
    }

    #[test]
    fn test_decimal_round_trip() {
        let dt = DataType::Decimal128(10, 2);
        let hive = arrow_to_hive_type(&dt).unwrap();
        assert_eq!(hive, "decimal(10,2)");

        let field = Field::new("price", dt, true);
        let schema = spark_struct_json_from_fields(&[field]).unwrap();
        let json_str = serde_json::to_string(&schema).unwrap();
        let parsed = super::spark_struct_json_to_fields(&json_str).unwrap();
        assert_eq!(parsed[0].data_type(), &DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_split_top_level_rejects_unbalanced_brackets() {
        assert!(super::split_top_level("struct<a:int>>").is_err());
        assert!(super::split_top_level("struct<a:int,<<b:int>>").is_err());
        assert!(super::split_top_level("map<string,int").is_err());
    }

    #[test]
    fn test_timestamp_non_utc_timezone_normalized_to_utc_through_hive_round_trip() {
        let dt = DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some(Arc::from("America/New_York")),
        );
        let hive = arrow_to_hive_type(&dt).unwrap();
        assert_eq!(hive, "timestamp");
        let back = hive_type_to_arrow(&hive).unwrap();
        assert_eq!(
            back,
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            )
        );
    }

    #[test]
    fn test_timestamp_without_timezone_gains_utc_through_hive_round_trip() {
        let dt = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
        let hive = arrow_to_hive_type(&dt).unwrap();
        assert_eq!(hive, "timestamp");
        let back = hive_type_to_arrow(&hive).unwrap();
        assert_eq!(
            back,
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            )
        );
    }

    #[test]
    fn test_decimal256_round_trip() {
        let dt = DataType::Decimal256(10, 2);
        let hive = arrow_to_hive_type(&dt).unwrap();
        assert_eq!(hive, "decimal(10,2)");
    }

    #[test]
    fn test_decimal_precision_exceeds_38_is_error() {
        let dt = DataType::Decimal128(39, 2);
        let result = arrow_to_hive_type(&dt);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("precision up to 38"),
            "Expected precision error, got: {err}"
        );

        let dt256 = DataType::Decimal256(39, 2);
        let result256 = arrow_to_hive_type(&dt256);
        assert!(
            result256.is_err(),
            "Expected error for Decimal256 with precision > 38"
        );
    }

    #[test]
    fn test_hive_decimal_bare_type_defaults_to_10_0() {
        let dt = hive_type_to_arrow("decimal").unwrap();
        assert_eq!(dt, DataType::Decimal128(10, 0));
    }

    #[test]
    fn test_hive_decimal_with_precision_only() {
        let dt = hive_type_to_arrow("decimal(15)").unwrap();
        assert_eq!(dt, DataType::Decimal128(15, 0));
    }
}
