use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::common::{DataFusionError, Result as DataFusionResult};

use crate::spec::FieldIndex;

pub const METADATA_COLUMN_ID: &str = "ducklake:column_id";
pub const METADATA_COLUMN_ORDER: &str = "ducklake:column_order";
pub const METADATA_DEFAULT_VALUE: &str = "ducklake:default_value";
pub const METADATA_INITIAL_DEFAULT: &str = "ducklake:initial_default";

pub fn build_arrow_field(
    name: &str,
    type_str: &str,
    nullable: bool,
    column_id: FieldIndex,
    column_order: u64,
    default_value: Option<&str>,
    initial_default: Option<&str>,
) -> DataFusionResult<Field> {
    let data_type = parse_ducklake_type(type_str)?;
    let mut metadata = HashMap::new();
    metadata.insert(METADATA_COLUMN_ID.to_string(), column_id.0.to_string());
    metadata.insert(METADATA_COLUMN_ORDER.to_string(), column_order.to_string());
    if let Some(def) = default_value {
        metadata.insert(METADATA_DEFAULT_VALUE.to_string(), def.to_string());
    }
    if let Some(initial) = initial_default {
        metadata.insert(METADATA_INITIAL_DEFAULT.to_string(), initial.to_string());
    }
    Ok(Field::new(name, data_type, nullable).with_metadata(metadata))
}

pub fn parse_ducklake_type(type_str: &str) -> DataFusionResult<DataType> {
    let type_str = type_str.trim().to_uppercase();

    match type_str.as_str() {
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "TINYINT" | "INT8" => Ok(DataType::Int8),
        "SMALLINT" | "INT16" => Ok(DataType::Int16),
        "INTEGER" | "INT" | "INT32" => Ok(DataType::Int32),
        "BIGINT" | "INT64" => Ok(DataType::Int64),
        "UTINYINT" | "UINT8" => Ok(DataType::UInt8),
        "USMALLINT" | "UINT16" => Ok(DataType::UInt16),
        "UINTEGER" | "UINT" | "UINT32" => Ok(DataType::UInt32),
        "UBIGINT" | "UINT64" => Ok(DataType::UInt64),
        "FLOAT" | "FLOAT32" => Ok(DataType::Float32),
        "DOUBLE" | "FLOAT64" => Ok(DataType::Float64),
        "DATE" | "DATE32" => Ok(DataType::Date32),
        "TIMESTAMP" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "TIME" | "TIME64" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "INTERVAL" => Ok(DataType::Interval(
            datafusion::arrow::datatypes::IntervalUnit::MonthDayNano,
        )),
        "VARCHAR" | "TEXT" | "STRING" | "UTF8" => Ok(DataType::Utf8),
        "BLOB" | "BYTEA" | "BINARY" => Ok(DataType::Binary),
        "UUID" => Ok(DataType::Utf8),
        s if s.starts_with("DECIMAL(") => {
            let inner = s
                .strip_prefix("DECIMAL(")
                .and_then(|s| s.strip_suffix(")"))
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Invalid DECIMAL type: {}", type_str))
                })?;
            let parts: Vec<&str> = inner.split(',').collect();
            match parts.as_slice() {
                [precision, scale] => {
                    let precision = precision.trim().parse::<u8>().map_err(|_| {
                        DataFusionError::Plan(format!("Invalid DECIMAL precision: {}", precision))
                    })?;
                    let scale = scale.trim().parse::<i8>().map_err(|_| {
                        DataFusionError::Plan(format!("Invalid DECIMAL scale: {}", scale))
                    })?;
                    Ok(DataType::Decimal128(precision, scale))
                }
                _ => Err(DataFusionError::Plan(format!(
                    "Invalid DECIMAL format: {}",
                    type_str
                ))),
            }
        }
        s if s.ends_with("[]") => {
            #[allow(clippy::unwrap_used)]
            let element_type_str = s.strip_suffix("[]").unwrap();
            let element_type = parse_ducklake_type(element_type_str)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                element_type,
                true,
            ))))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported DuckLake type: {}",
            type_str
        ))),
    }
}

pub fn field_column_id(field: &Field) -> Option<FieldIndex> {
    field
        .metadata()
        .get(METADATA_COLUMN_ID)
        .and_then(|v| v.parse::<u64>().ok())
        .map(FieldIndex)
}

pub fn schema_column_name_by_id(schema: &ArrowSchema, column_id: FieldIndex) -> Option<String> {
    for field in schema.fields() {
        if field_column_id(field) == Some(column_id) {
            return Some(field.name().clone());
        }
    }
    None
}
