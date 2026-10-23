//! Schema-literal analysis shared by query resolution and serialization kernels.
use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::*;
use datafusion_common::arrow::datatypes::{Decimal128Type, validate_decimal_precision_and_scale};
use datafusion_common::{DataFusionError, Result};
use sail_common::spec::{
    self, SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME,
    SAIL_MAP_VALUE_FIELD_NAME,
};
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser as sail_parser;
use serde_json::Value;

use crate::data_type::TypeResolver;

pub fn type_resolver(session_timezone: &str) -> TypeResolver {
    TypeResolver {
        session_timezone: Arc::from(session_timezone),
        default_timestamp_ltz: true,
        use_large_var_types: false,
    }
}
pub fn spec_to_arrow_data_type(dt: &spec::DataType, session_timezone: &str) -> Result<DataType> {
    let resolver = type_resolver(session_timezone);
    let dt = match dt {
        spec::DataType::UserDefined { sql_type, .. } => sql_type,
        dt => dt,
    };
    resolver
        .resolve_data_type(dt)
        .map_err(|e| DataFusionError::Plan(e.to_string()))
}
pub fn parse_fields(schema: &str, session_timezone: &str) -> Result<Fields> {
    let schema = schema.trim();
    let type_str = if schema
        .get(..6)
        .is_some_and(|p| p.eq_ignore_ascii_case("struct"))
        && schema.get(6..).is_some_and(|p| p.starts_with('<'))
        && schema.ends_with('>')
    {
        schema.to_string()
    } else {
        // Schema string is a list of fields. Wrap it into `STRUCT<...>`.
        format!("STRUCT<{schema}>")
    };

    let ast = sail_parser::parse_data_type(&type_str)
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse schema '{schema}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze schema '{schema}': {e}")))?;
    let spec::DataType::Struct { fields } = spec_dt else {
        return Err(DataFusionError::Plan(format!(
            "Expected STRUCT schema, got: {spec_dt:?}"
        )));
    };

    let mut out: Vec<Arc<Field>> = Vec::with_capacity(fields.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(fields.len());
    for f in fields.iter() {
        let name = f.name.clone();
        if !seen.insert(name.clone()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate field name '{name}'"
            )));
        }
        let dt = spec_to_arrow_data_type(&f.data_type, session_timezone)?;
        out.push(Arc::new(Field::new(name, dt, f.nullable)));
    }
    Ok(Fields::from(out))
}

pub fn parse_data_type_with_session_timezone(
    raw: &str,
    session_timezone: &str,
) -> Result<DataType> {
    let ast = sail_parser::parse_data_type(raw)
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse SQL type '{raw}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze SQL type '{raw}': {e}")))?;
    spec_to_arrow_data_type(&spec_dt, session_timezone)
}

pub fn parse_schema_to_data_type(schema: &str, session_timezone: &str) -> Result<DataType> {
    let schema = schema.trim();

    // Try parsing as Spark JSON schema format (e.g., {"type":"struct","fields":[...]}).
    // PySpark serializes DataType objects to JSON format via schema.json().
    // For inputs that clearly look like JSON, return the JSON parsing result directly
    // so callers see the real error instead of a later DDL parsing failure.
    if schema.starts_with('{') || schema.starts_with('"') {
        return parse_spark_json_schema(schema, session_timezone);
    }

    // Try parsing as a full type first (STRUCT<...>, ARRAY<...>, MAP<...>).
    let ast = sail_parser::parse_data_type(schema).or_else(|_| {
        // If that fails, treat as a bare field list and wrap in STRUCT<...>.
        sail_parser::parse_data_type(&format!("STRUCT<{schema}>"))
    });

    let ast =
        ast.map_err(|e| DataFusionError::Plan(format!("Failed to parse schema '{schema}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze schema '{schema}': {e}")))?;
    spec_to_arrow_data_type(&spec_dt, session_timezone)
}

pub fn parse_spark_json_schema(schema: &str, session_timezone: &str) -> Result<DataType> {
    let json: Value = serde_json::from_str(schema)
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse JSON schema: {e}")))?;
    json_value_to_data_type(&json, session_timezone)
}

pub fn json_value_to_data_type(value: &Value, session_timezone: &str) -> Result<DataType> {
    match value {
        Value::String(s) => json_type_name_to_data_type(s, session_timezone),
        Value::Object(map) => {
            let type_str = map.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
                DataFusionError::Plan("JSON schema object missing 'type' field".to_string())
            })?;
            match type_str {
                "struct" => {
                    let fields = map
                        .get("fields")
                        .and_then(|v| v.as_array())
                        .ok_or_else(|| {
                            DataFusionError::Plan("Struct type missing 'fields' array".to_string())
                        })?;
                    let mut arrow_fields = Vec::with_capacity(fields.len());
                    for field in fields {
                        let name = field.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                            DataFusionError::Plan("Struct field missing 'name'".to_string())
                        })?;
                        let nullable = field
                            .get("nullable")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true);
                        let field_type = field.get("type").ok_or_else(|| {
                            DataFusionError::Plan("Struct field missing 'type'".to_string())
                        })?;
                        let dt = json_value_to_data_type(field_type, session_timezone)?;
                        arrow_fields.push(Arc::new(Field::new(name, dt, nullable)));
                    }
                    Ok(DataType::Struct(Fields::from(arrow_fields)))
                }
                "array" => {
                    let element_type = map.get("elementType").ok_or_else(|| {
                        DataFusionError::Plan("Array type missing 'elementType'".to_string())
                    })?;
                    let contains_null = map
                        .get("containsNull")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    let dt = json_value_to_data_type(element_type, session_timezone)?;
                    Ok(DataType::List(Arc::new(Field::new(
                        SAIL_LIST_FIELD_NAME,
                        dt,
                        contains_null,
                    ))))
                }
                "map" => {
                    let key_type = map.get("keyType").ok_or_else(|| {
                        DataFusionError::Plan("Map type missing 'keyType'".to_string())
                    })?;
                    let value_type = map.get("valueType").ok_or_else(|| {
                        DataFusionError::Plan("Map type missing 'valueType'".to_string())
                    })?;
                    let value_contains_null = map
                        .get("valueContainsNull")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    let key_dt = json_value_to_data_type(key_type, session_timezone)?;
                    let value_dt = json_value_to_data_type(value_type, session_timezone)?;
                    let fields = Fields::from(vec![
                        Arc::new(Field::new(SAIL_MAP_KEY_FIELD_NAME, key_dt, false)),
                        Arc::new(Field::new(
                            SAIL_MAP_VALUE_FIELD_NAME,
                            value_dt,
                            value_contains_null,
                        )),
                    ]);
                    Ok(DataType::Map(
                        Arc::new(Field::new(
                            SAIL_MAP_FIELD_NAME,
                            DataType::Struct(fields),
                            false,
                        )),
                        false,
                    ))
                }
                "udt" => {
                    let sql_type = map
                        .get("sqlType")
                        .or_else(|| map.get("sql_type"))
                        .ok_or_else(|| {
                            DataFusionError::Plan("UDT type missing 'sqlType' field".to_string())
                        })?;
                    json_value_to_data_type(sql_type, session_timezone)
                }
                other => {
                    // Some types might be expressed as {"type": "simple_name"}
                    json_type_name_to_data_type(other, session_timezone)
                }
            }
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unexpected JSON schema value: {value}"
        ))),
    }
}

pub fn json_type_name_to_data_type(type_name: &str, session_timezone: &str) -> Result<DataType> {
    match type_name {
        "null" | "void" => Ok(DataType::Null),
        "boolean" => Ok(DataType::Boolean),
        "byte" | "tinyint" => Ok(DataType::Int8),
        "short" | "smallint" => Ok(DataType::Int16),
        "integer" | "int" => Ok(DataType::Int32),
        "long" | "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "decimal" => Ok(DataType::Decimal128(10, 0)),
        "string" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" | "timestamp_ltz" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from(session_timezone)),
        )),
        "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "interval" => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        other => {
            if let Some(args) = other.strip_circumfix("decimal(", ')') {
                let mut parts = args.split(',').map(str::trim);
                if let (Some(precision), Some(scale), None) =
                    (parts.next(), parts.next(), parts.next())
                    && let (Ok(precision), Ok(scale)) =
                        (precision.parse::<u8>(), scale.parse::<i8>())
                {
                    validate_decimal_precision_and_scale::<Decimal128Type>(precision, scale)
                        .map_err(|e| {
                            DataFusionError::Plan(format!(
                                "Invalid decimal precision/scale in '{other}': {e}"
                            ))
                        })?;
                    return Ok(DataType::Decimal128(precision, scale));
                }
            }
            if let Some(args) = other.strip_circumfix("geometry(", ')') {
                let srid = args.trim();
                if srid == "ANY" || srid.parse::<i32>().is_ok() {
                    return Ok(DataType::Binary);
                }
                return Err(DataFusionError::Plan(format!(
                    "Unsupported JSON schema type: '{other}'"
                )));
            }
            if let Some(args) = other.strip_circumfix("geography(", ')') {
                let mut parts = args.split(',').map(str::trim);
                if let (Some(srid), algorithm, None) = (parts.next(), parts.next(), parts.next()) {
                    let valid_srid = srid == "ANY" || srid.parse::<i32>().is_ok();
                    let valid_algorithm = algorithm.is_none_or(|x| x == "spherical");
                    if valid_srid && valid_algorithm {
                        return Ok(DataType::Binary);
                    }
                }
                return Err(DataFusionError::Plan(format!(
                    "Unsupported JSON schema type: '{other}'"
                )));
            }
            // Parameterized Spark JSON type strings such as "decimal(10,2)",
            // "time(0)", "char(10)", and "varchar(20)" use the same syntax
            // as Spark DDL type strings, so reuse the existing DDL analyzer.
            let ast = sail_parser::parse_data_type(other).map_err(|_| {
                DataFusionError::Plan(format!("Unsupported JSON schema type: '{other}'"))
            })?;
            let spec_dt = from_ast_data_type(ast).map_err(|e| {
                DataFusionError::Plan(format!("Failed to analyze JSON schema type '{other}': {e}"))
            })?;
            spec_to_arrow_data_type(&spec_dt, session_timezone)
        }
    }
}

pub fn parse_xml_schema(schema: &str, session_timezone: &str) -> Result<DataType> {
    let schema = schema.trim();
    let ast_result = sail_parser::parse_data_type(schema)
        .or_else(|_| sail_parser::parse_data_type(&format!("STRUCT<{schema}>")));
    let ast = ast_result
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse schema '{schema}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze schema '{schema}': {e}")))?;
    spec_to_arrow_data_type(&spec_dt, session_timezone)
}
