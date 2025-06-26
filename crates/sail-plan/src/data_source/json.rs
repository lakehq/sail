use std::collections::HashMap;
use std::convert::TryFrom;

use sail_common::config::{JSON_READ_CONFIG, JSON_WRITE_CONFIG};
use serde::Deserialize;

use crate::data_source::DataSourceOptions;
use crate::error::{PlanError, PlanResult};

/// Datasource Options that control the reading of JSON files.
#[derive(Debug, Deserialize)]
// Serde bypasses any individual field deserializers and instead uses the `TryFrom` implementation.
#[serde(try_from = "HashMap<String, String>")]
pub struct JsonReadOptions {
    pub schema_infer_max_records: usize,
    pub compression: String,
}

impl TryFrom<HashMap<String, String>> for JsonReadOptions {
    type Error = PlanError;

    // The options HashMap should already contain all supported keys with their resolved values
    fn try_from(mut options: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(JsonReadOptions {
            schema_infer_max_records: options
                .remove("schema_infer_max_records")
                .ok_or_else(|| {
                    PlanError::internal("JSON `schema_infer_max_records` read option is required")
                })?
                .parse()
                .map_err(|e| {
                    PlanError::internal(format!(
                        "Invalid JSON `schema_infer_max_records` read option: {e}"
                    ))
                })?,
            compression: options
                .remove("compression")
                .ok_or_else(|| PlanError::missing("JSON `compression` read option is required"))?,
        })
    }
}

impl DataSourceOptions for JsonReadOptions {
    const SOURCE_CONFIG: &'static str = JSON_READ_CONFIG;
    fn try_from_options(options: HashMap<String, String>) -> PlanResult<Self> {
        Self::try_from(options)
    }
}

/// Datasource Options that control the writing of JSON files.
#[derive(Debug, Deserialize)]
// Serde bypasses any individual field deserializers and instead uses the `TryFrom` implementation.
#[serde(try_from = "HashMap<String, String>")]
pub struct JsonWriteOptions {
    pub compression: String,
}

impl TryFrom<HashMap<String, String>> for JsonWriteOptions {
    type Error = PlanError;

    // The options HashMap should already contain all supported keys with their resolved values
    fn try_from(mut options: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(JsonWriteOptions {
            compression: options
                .remove("compression")
                .ok_or_else(|| PlanError::missing("JSON `compression` write option is required"))?,
        })
    }
}

impl DataSourceOptions for JsonWriteOptions {
    const SOURCE_CONFIG: &'static str = JSON_WRITE_CONFIG;
    fn try_from_options(options: HashMap<String, String>) -> PlanResult<Self> {
        Self::try_from(options)
    }
}
