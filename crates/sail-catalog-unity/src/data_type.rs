use arrow::datatypes::{
    DataType, Field, Fields, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use sail_catalog::error::{CatalogError, CatalogResult};
use std::collections::HashMap;
use std::sync::Arc;

use crate::unity::types;

pub(crate) struct UnityColumnType {
    pub type_text: String,
    pub type_json: serde_json::Value,
    pub type_name: types::ColumnTypeName,
}

pub(crate) fn data_type_to_unity_type(data_type: &DataType) -> CatalogResult<UnityColumnType> {
    // There is no clarity on the expected format for `type_json` and `type_text`.
    // Open source code has various and docs seem to contradict each other.
    // TODO: UserDefinedType
    match data_type {
        DataType::Null => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Null.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Null.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Null,
        }),
        DataType::Boolean => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Boolean.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Boolean.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Boolean,
        }),
        DataType::Int8 => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Byte.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Byte.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Byte,
        }),
        DataType::Int16 | DataType::UInt8 => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Short.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Short.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Short,
        }),
        DataType::Int32 | DataType::UInt16 => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Int.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Int.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Int,
        }),
        DataType::Int64 | DataType::UInt32 => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Long.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Long.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Long,
        }),
        DataType::Float32 => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Float.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Float.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Float,
        }),
        DataType::Float64 => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Double.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Double.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Double,
        }),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Timestamp.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Timestamp.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Timestamp,
        }),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::TimestampNtz
                .to_string()
                .to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::TimestampNtz
                    .to_string()
                    .to_lowercase(),
            ),
            type_name: types::ColumnTypeName::TimestampNtz,
        }),
        DataType::Date32 => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Date.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Date.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Date,
        }),
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::Binary.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::Binary.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::Binary,
        }),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(UnityColumnType {
            type_text: types::ColumnTypeName::String.to_string().to_lowercase(),
            type_json: serde_json::Value::String(
                types::ColumnTypeName::String.to_string().to_lowercase(),
            ),
            type_name: types::ColumnTypeName::String,
        }),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => {
            let type_str = format!("decimal({},{})", precision, scale);
            Ok(UnityColumnType {
                type_text: type_str.clone(),
                type_json: serde_json::Value::String(type_str),
                type_name: types::ColumnTypeName::Decimal,
            })
        }
        DataType::Decimal256(precision, scale) => {
            if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                let type_str = format!("decimal({},{})", precision, scale);
                Ok(UnityColumnType {
                    type_text: type_str.clone(),
                    type_json: serde_json::Value::String(type_str),
                    type_name: types::ColumnTypeName::Decimal,
                })
            } else {
                Err(CatalogError::External(format!(
                    "Decimal with precision > {} and scale > {} is not supported in Unity Catalog",
                    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE
                )))
            }
        }
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field) => {
            let field_type = data_type_to_unity_type(field.data_type())?;
            let type_text = format!("array<{}>", field_type.type_text);
            let mut metadata_map = serde_json::Map::new();
            for (k, v) in field.metadata() {
                metadata_map.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            let type_json = serde_json::json!({
                "type": serde_json::json!({
                    "type": "array",
                    "elementType": field_type.type_json,
                    "containsNull": field.is_nullable()
                }),
                "metadata": metadata_map
            });
            Ok(UnityColumnType {
                type_text,
                type_json,
                type_name: types::ColumnTypeName::Array,
            })
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                if fields.len() == 2 {
                    let key_type = data_type_to_unity_type(fields[0].data_type())?;
                    let value_type = data_type_to_unity_type(fields[1].data_type())?;
                    let type_text = format!("map<{},{}>", key_type.type_text, value_type.type_text);
                    let mut metadata_map = serde_json::Map::new();
                    for (k, v) in field.metadata() {
                        metadata_map.insert(k.clone(), serde_json::Value::String(v.clone()));
                    }
                    let type_json = serde_json::json!({
                        "type": serde_json::json!({
                            "type": "map",
                            "keyType": key_type.type_json,
                            "valueType": value_type.type_json,
                            "valueContainsNull": fields[1].is_nullable()
                        }),
                        "metadata": metadata_map
                    });
                    Ok(UnityColumnType {
                        type_text,
                        type_json,
                        type_name: types::ColumnTypeName::Map,
                    })
                } else {
                    Err(CatalogError::External(format!(
                        "Map type struct must have exactly two fields, found {fields:?}"
                    )))
                }
            } else {
                Err(CatalogError::External(format!(
                    "Map type must be a struct with key and value fields, found {field:?}"
                )))
            }
        }
        DataType::Struct(fields) => {
            let mut type_text_parts = Vec::new();
            let mut json_fields = Vec::new();
            for field in fields.iter() {
                let field_type = data_type_to_unity_type(field.data_type())?;
                type_text_parts.push(format!("{}:{}", field.name(), field_type.type_text));
                let mut metadata_map = serde_json::Map::new();
                for (k, v) in field.metadata() {
                    metadata_map.insert(k.clone(), serde_json::Value::String(v.clone()));
                }
                json_fields.push(serde_json::json!({
                    "name": field.name(),
                    "type": field_type.type_json,
                    "nullable": field.is_nullable(),
                    "metadata": metadata_map,
                }));
            }
            let type_text = format!("struct<{}>", type_text_parts.join(","));
            let type_json = serde_json::json!({
                "type": serde_json::json!({
                    "type": "struct",
                    "fields": json_fields
                }),
            });
            Ok(UnityColumnType {
                type_text,
                type_json,
                type_name: types::ColumnTypeName::Struct,
            })
        }
        DataType::UInt64
        | DataType::Float16
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _) => Err(CatalogError::External(format!(
            "{data_type:?} type is not supported in Unity Catalog",
        ))),
    }
}

pub(crate) fn unity_type_to_data_type(
    type_name: Option<types::ColumnTypeName>,
    type_json: Option<String>,
    type_text: Option<String>,
    type_precision: Option<i32>,
    type_scale: Option<i32>,
    _type_interval_type: Option<String>,
) -> CatalogResult<DataType> {
    // There is no clarity on the expected format for `type_json` and `type_text`.
    // Open source code has various and docs seem to contradict each other.
    // So, we parse with flexibility here.
    // TODO:
    //  1. Handle `Interval`, `UserDefinedType` and `TableType`.
    //  2. Parse precision and scale for `Decimal` if type_precision and type_scale aren't provided.
    if let Some(type_name) = type_name {
        match type_name {
            types::ColumnTypeName::Boolean => Ok(DataType::Boolean),
            types::ColumnTypeName::Byte => Ok(DataType::Int8),
            types::ColumnTypeName::Short => Ok(DataType::Int16),
            types::ColumnTypeName::Int => Ok(DataType::Int32),
            types::ColumnTypeName::Long => Ok(DataType::Int64),
            types::ColumnTypeName::Float => Ok(DataType::Float32),
            types::ColumnTypeName::Double => Ok(DataType::Float64),
            types::ColumnTypeName::Date => Ok(DataType::Date32),
            types::ColumnTypeName::Timestamp => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC".to_string())),
            )),
            types::ColumnTypeName::TimestampNtz => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            types::ColumnTypeName::String | types::ColumnTypeName::Char => Ok(DataType::Utf8),
            types::ColumnTypeName::Binary => Ok(DataType::Binary),
            types::ColumnTypeName::Decimal => {
                if let (Some(precision), Some(scale)) = (type_precision, type_scale) {
                    Ok(DataType::Decimal128(precision as u8, scale as i8))
                } else {
                    // TODO: Parse from type_text or type_json if precision and scale are not provided
                    Err(CatalogError::External(
                        "Decimal type requires precision and scale".to_string(),
                    ))
                }
            }
            types::ColumnTypeName::Null => Ok(DataType::Null),
            types::ColumnTypeName::Array
            | types::ColumnTypeName::Struct
            | types::ColumnTypeName::Map => {
                parse_unity_type_json(type_json.as_deref().ok_or_else(|| {
                    CatalogError::External("type_json is required for complex types".to_string())
                })?)
            }
            types::ColumnTypeName::Interval
            | types::ColumnTypeName::UserDefinedType
            | types::ColumnTypeName::TableType => Err(CatalogError::External(format!(
                "{type_name:?} type is not supported yet",
            ))),
        }
    } else {
        parse_from_text_or_json(type_text, type_json)
    }
}

pub(crate) fn parse_from_text_or_json(
    type_text: Option<String>,
    type_json: Option<String>,
) -> CatalogResult<DataType> {
    if let Some(text) = type_text {
        let result = parse_simple_type_from_string(&text);
        match result {
            Ok(data_type) => Ok(data_type),
            Err(_) => {
                if let Some(json) = type_json {
                    parse_unity_type_json(&json)
                } else {
                    Err(CatalogError::External(format!(
                        "Unable to parse type from type_text: {text}"
                    )))
                }
            }
        }
    } else if let Some(json) = type_json {
        parse_unity_type_json(&json)
    } else {
        Err(CatalogError::External(
            "Type information missing: no `type_text` or `type_json` provided".to_string(),
        ))
    }
}

pub(crate) fn parse_simple_type_from_string(type_str: &str) -> CatalogResult<DataType> {
    match type_str.trim().to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "BYTE" | "TINYINT" | "INT8" => Ok(DataType::Int8),
        "SHORT" | "SMALLINT" | "INT16" => Ok(DataType::Int16),
        "INT" | "INTEGER" | "INT32" => Ok(DataType::Int32),
        "LONG" | "BIGINT" | "INT64" => Ok(DataType::Int64),
        "FLOAT" | "REAL" | "FLOAT32" => Ok(DataType::Float32),
        "DOUBLE" | "FLOAT64" => Ok(DataType::Float64),
        "DATE" | "DATE32" => Ok(DataType::Date32),
        "TIMESTAMP" | "TIMESTAMP_LTZ" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("UTC".to_string())),
        )),
        "TIMESTAMP_NTZ" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "STRING" | "CHAR" | "CHARACTER" | "VARCHAR" => Ok(DataType::Utf8),
        "TEXT" => Ok(DataType::LargeUtf8),
        "BINARY" | "BYTEA" => Ok(DataType::Binary),
        "NULL" | "VOID" => Ok(DataType::Null),
        _ => Err(CatalogError::External(format!(
            "Unable to parse simple type from string: {type_str}"
        ))),
    }
}

pub(crate) fn parse_unity_type_json(json_str: &str) -> CatalogResult<DataType> {
    let value: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| CatalogError::External(format!("Failed to parse type_json: {e}")))?;

    match value {
        serde_json::Value::String(s) => parse_simple_type_from_string(&s),
        serde_json::Value::Object(obj) => parse_complex_type_from_json_object(&obj),
        _ => Err(CatalogError::External(format!(
            "Invalid type_json format: expected string or object, got {value:?}"
        ))),
    }
}

pub(crate) fn parse_complex_type_from_json_object(
    serde_object: &serde_json::Map<String, serde_json::Value>,
) -> CatalogResult<DataType> {
    let type_field = serde_object.get("type").ok_or_else(|| {
        CatalogError::External("Complex type_json missing 'type' field".to_string())
    })?;
    let metadata = get_field_metadata(serde_object);

    let (type_info, type_field, metadata) = match type_field {
        serde_json::Value::String(s) => Ok((serde_object, s.trim().to_lowercase(), metadata)),
        serde_json::Value::Object(inner_obj) => {
            let type_field = inner_obj
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    CatalogError::External("Complex type_json missing 'type' field".to_string())
                })?
                .trim()
                .to_lowercase();
            let inner_metadata = get_field_metadata(inner_obj);
            let metadata = if metadata.is_empty() {
                inner_metadata
            } else {
                let mut combined = metadata;
                combined.extend(inner_metadata);
                combined
            };
            Ok((inner_obj, type_field, metadata))
        }
        other => Err(CatalogError::External(format!(
            "Invalid type_json format: expected string or object, got {other:?}"
        ))),
    }?;

    match type_field.as_str() {
        "array" => {
            let element_type_json = type_info
                .get("elementType")
                .or(type_info.get("element_type"))
                .or(type_info.get("itemType"))
                .or(type_info.get("item_type"))
                .ok_or_else(|| {
                    CatalogError::External(
                        "Array type missing 'elementType', 'element_type', 'itemType', or 'item_type' field".to_string(),
                    )
                })?;
            let contains_null = type_info
                .get("containsNull")
                .or(type_info.get("contains_null"))
                .or(type_info.get("elementNullable"))
                .or(type_info.get("element_nullable"))
                .or(type_info.get("itemNullable"))
                .or(type_info.get("item_nullable"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let element_type = match element_type_json {
                serde_json::Value::String(s) => parse_simple_type_from_string(s)?,
                serde_json::Value::Object(o) => parse_complex_type_from_json_object(o)?,
                _ => {
                    return Err(CatalogError::External(
                        "Invalid elementType format".to_string(),
                    ))
                }
            };
            Ok(DataType::List(Arc::new(
                Field::new("item", element_type, contains_null).with_metadata(metadata),
            )))
        }
        "map" => {
            let key_type_json = type_info
                .get("keyType")
                .or(type_info.get("key_type"))
                .ok_or_else(|| {
                    CatalogError::External(
                        "Map type missing 'keyType' or 'key_type' field".to_string(),
                    )
                })?;
            let value_type_json = type_info
                .get("valueType")
                .or(type_info.get("value_type"))
                .ok_or_else(|| {
                    CatalogError::External(
                        "Map type missing 'valueType' or 'value_type' field".to_string(),
                    )
                })?;
            let value_contains_null = type_info
                .get("valueContainsNull")
                .or(type_info.get("value_contains_null"))
                .or(type_info.get("elementNullable"))
                .or(type_info.get("element_nullable"))
                .or(type_info.get("itemNullable"))
                .or(type_info.get("item_nullable"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            let key_type = match key_type_json {
                serde_json::Value::String(s) => parse_simple_type_from_string(s)?,
                serde_json::Value::Object(o) => parse_complex_type_from_json_object(o)?,
                _ => return Err(CatalogError::External("Invalid keyType format".to_string())),
            };
            let value_type = match value_type_json {
                serde_json::Value::String(s) => parse_simple_type_from_string(s)?,
                serde_json::Value::Object(o) => parse_complex_type_from_json_object(o)?,
                _ => {
                    return Err(CatalogError::External(
                        "Invalid valueType format".to_string(),
                    ))
                }
            };

            Ok(DataType::Map(
                Arc::new(
                    Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", key_type, false),
                            Field::new("value", value_type, value_contains_null),
                        ])),
                        false,
                    )
                    .with_metadata(metadata),
                ),
                false,
            ))
        }
        "struct" => {
            let fields_json = type_info
                .get("fields")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    CatalogError::External("Struct type missing 'fields' array".to_string())
                })?;

            let mut fields = Vec::new();
            for field_json in fields_json {
                let field_obj = field_json.as_object().ok_or_else(|| {
                    CatalogError::External("Struct field must be an object".to_string())
                })?;

                let field_name =
                    field_obj
                        .get("name")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            CatalogError::External("Struct field missing 'name'".to_string())
                        })?;

                let field_type_json = field_obj.get("type").ok_or_else(|| {
                    CatalogError::External("Struct field missing 'type'".to_string())
                })?;

                let nullable = field_obj
                    .get("nullable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);

                let field_metadata = get_field_metadata(field_obj);

                let field_type = match field_type_json {
                    serde_json::Value::String(s) => parse_simple_type_from_string(s)?,
                    serde_json::Value::Object(o) => parse_complex_type_from_json_object(o)?,
                    _ => {
                        return Err(CatalogError::External(
                            "Invalid field type format".to_string(),
                        ))
                    }
                };

                fields.push(
                    Field::new(field_name, field_type, nullable).with_metadata(field_metadata),
                );
            }

            Ok(DataType::Struct(Fields::from(fields)))
        }
        _ => Err(CatalogError::External(format!(
            "Unsupported complex type: {type_field}"
        ))),
    }
}

fn get_field_metadata(
    field_obj: &serde_json::map::Map<String, serde_json::value::Value>,
) -> HashMap<String, String> {
    if let Some(metadata_json) = field_obj.get("metadata") {
        if let serde_json::Value::Object(map) = metadata_json {
            map.into_iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.to_string(), s.to_string())))
                .collect()
        } else {
            HashMap::new()
        }
    } else {
        HashMap::new()
    }
}
