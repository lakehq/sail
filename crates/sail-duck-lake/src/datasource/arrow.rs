use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::common::{DataFusionError, Result as DataFusionResult};

use crate::spec::ColumnInfo;

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

pub fn columns_to_arrow_schema(columns: &[ColumnInfo]) -> DataFusionResult<ArrowSchema> {
    let top_level_columns: Vec<&ColumnInfo> = columns
        .iter()
        .filter(|c| c.parent_column.is_none())
        .collect();

    let fields: Vec<Field> = top_level_columns
        .into_iter()
        .map(|col| {
            let data_type = parse_ducklake_type(&col.column_type)?;
            Ok(Field::new(
                col.column_name.clone(),
                data_type,
                col.nulls_allowed,
            ))
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    Ok(ArrowSchema::new(fields))
}
