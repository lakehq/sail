// Similar to https://github.com/lakehq/sail/blob/f64ccb2233473282f9d3ae2c95e2b90848a6f249/crates/sail-spark-connect/src/proto/data_type.rs
// but copied to avoid circular imports
// Also `parse_spark_json_data_type` was rewritten to parse directly to sail data types
// instead of types defined in the proto files

use datafusion_common::{DataFusionError, Result};
use monostate::MustBe;
use sail_common::spec;
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser::parse_data_type;
use serde::{Deserialize, Serialize};

/// Parse a Spark data type string of various forms.
/// Reference: org.apache.spark.sql.connect.planner.SparkConnectPlanner#parseDatatypeString
pub(crate) fn parse_spark_data_type(schema: &str) -> Result<spec::DataType> {
    if let Ok(dt) = parse_data_type(schema).and_then(from_ast_data_type) {
        Ok(dt)
    } else if let Ok(dt) =
        parse_data_type(format!("struct<{schema}>").as_str()).and_then(from_ast_data_type)
    {
        match dt {
            spec::DataType::Struct { fields } if fields.is_empty() => {
                Err(DataFusionError::Plan("empty data type".to_string()))
            }
            // The SQL parser supports both `struct<name: type, ...>` and `struct<name type, ...>` syntax.
            // Therefore, by wrapping the input with `struct<...>`, we do not need separate logic
            // to parse table schema input (`name type, ...`).
            _ => Ok(dt),
        }
    } else {
        parse_spark_json_data_type(schema)
    }
}

fn parse_spark_json_data_type(schema: &str) -> Result<spec::DataType> {
    let json_type: JsonDataType =
        serde_json::from_str(schema).map_err(|e| DataFusionError::Plan(e.to_string()))?;
    from_spark_json_data_type(json_type)
}

fn from_spark_json_data_type(data_type: JsonDataType) -> Result<spec::DataType> {
    Ok(match data_type {
        JsonDataType::String => spec::DataType::Utf8,
        JsonDataType::Integer => spec::DataType::Int32,
        JsonDataType::Float => spec::DataType::Float32,
        JsonDataType::Decimal => spec::DataType::Decimal128 {
            precision: 10,
            scale: 0,
        },
        JsonDataType::Boolean => spec::DataType::Boolean,
        JsonDataType::Timestamp => spec::DataType::Timestamp {
            time_unit: spec::TimeUnit::Microsecond,
            timestamp_type: spec::TimestampType::WithoutTimeZone,
        },
        JsonDataType::Struct { r#type: _, fields } => spec::DataType::Struct {
            fields: fields
                .into_iter()
                .map(|field| {
                    Ok(spec::Field {
                        name: field.name,
                        data_type: from_spark_json_data_type(field.r#type)?,
                        nullable: field.nullable,
                        metadata: field
                            .metadata
                            .map(|m| serde_json::to_string(&m))
                            .transpose()
                            .map_err(|e| DataFusionError::Plan(e.to_string()))?
                            .map(|s| vec![("metadata".to_string(), s)])
                            .unwrap_or_default(),
                    })
                })
                .collect::<Result<_>>()?,
        },
        JsonDataType::Array {
            element_type,
            contains_null,
            ..
        } => spec::DataType::List {
            data_type: Box::new(from_spark_json_data_type(*element_type)?),
            nullable: contains_null,
        },
        JsonDataType::Map {
            r#type: _,
            key_type,
            value_type,
            value_contains_null,
        } => spec::DataType::Map {
            key_type: Box::new(from_spark_json_data_type(*key_type)?),
            value_type: Box::new(from_spark_json_data_type(*value_type)?),
            value_type_nullable: value_contains_null,
            keys_sorted: false,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonDataType {
    Integer,
    Float,
    Decimal,
    String,
    Boolean,
    #[serde(alias = "timestamp_ltz")]
    Timestamp,
    #[serde(untagged, rename_all = "camelCase")]
    Struct {
        r#type: MustBe!("struct"),
        fields: Vec<JsonStructField>,
    },
    #[serde(untagged, rename_all = "camelCase")]
    Array {
        r#type: MustBe!("array"),
        element_type: Box<JsonDataType>,
        contains_null: bool,
    },
    #[serde(untagged, rename_all = "camelCase")]
    Map {
        r#type: MustBe!("map"),
        key_type: Box<JsonDataType>,
        value_type: Box<JsonDataType>,
        value_contains_null: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct JsonStructField {
    pub name: String,
    pub nullable: bool,
    pub r#type: JsonDataType,
    pub metadata: Option<serde_json::Value>,
}
