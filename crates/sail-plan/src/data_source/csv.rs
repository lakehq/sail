use figment::Figment;
use sail_common::config::{deserialize_non_empty_string, ConfigDefinition, CSV_CONFIG};
use serde::Deserialize;

use crate::error::{PlanError, PlanResult};

/// Datasource Options that control the reading of CSV files.
#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct CsvReadOptions<'a> {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub comment: Option<u8>,
    pub header: bool,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub null_value: Option<String>,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub null_regex: Option<String>,
    pub line_sep: Option<u8>,
    pub schema_infer_max_records: usize,
    pub newlines_in_values: bool,
    pub file_extension: &'a str,
    pub compression: &'a str,
}

impl<'a> CsvReadOptions<'a> {
    pub fn load_default() -> PlanResult<Self> {
        Figment::from(ConfigDefinition::new(CSV_CONFIG))
            .extract()
            .map_err(|e| PlanError::InvalidArgument(e.to_string()))
    }
}

fn merge_json(default: &mut serde_json::Value, patch: serde_json::Value) {
    if let (serde_json::Value::Object(def), serde_json::Value::Object(pat)) = (default, patch) {
        for (k, v) in pat {
            def.insert(k, v);
        }
    }
}
