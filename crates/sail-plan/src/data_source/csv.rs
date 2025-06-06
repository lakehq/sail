use crate::data_source::{deserialize_data_source_bool, deserialize_data_source_usize};
use std::collections::HashMap;
use std::str::FromStr;

use figment::Figment;
use sail_common::config::{deserialize_non_empty_string, ConfigDefinition, CSV_CONFIG};
use serde::{Deserialize, Serialize};

use crate::error::{PlanError, PlanResult};

/// Datasource Options that control the reading of CSV files.
#[derive(Debug, Deserialize)]
// #[serde(rename_all(deserialize = "camelCase"))]
pub struct CsvReadOptions {
    pub delimiter: String,
    pub quote: String,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub escape: Option<String>,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub comment: Option<String>,
    #[serde(deserialize_with = "deserialize_data_source_bool")]
    pub header: bool,
    #[serde(alias = "nullValue", deserialize_with = "deserialize_non_empty_string")]
    pub null_value: Option<String>,
    #[serde(alias = "nullRegex", deserialize_with = "deserialize_non_empty_string")]
    pub null_regex: Option<String>,
    #[serde(alias = "lineSep", deserialize_with = "deserialize_non_empty_string")]
    pub line_sep: Option<String>,
    #[serde(
        alias = "schemaInferMaxRecords",
        deserialize_with = "deserialize_data_source_usize"
    )]
    pub schema_infer_max_records: usize,
    #[serde(
        alias = "newlinesInValues",
        deserialize_with = "deserialize_data_source_bool"
    )]
    pub newlines_in_values: bool,
    #[serde(alias = "fileExtension")]
    pub file_extension: String,
    pub compression: String,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        let options: Vec<ConfigItem> = serde_yaml::from_str(CSV_CONFIG).unwrap();
        let options: HashMap<String, String> = options
            .into_iter()
            .filter(|item| item.supported)
            .map(|item| (item.key.to_lowercase(), item.default))
            .collect();
        serde_json::from_value(serde_json::to_value(options).unwrap()).unwrap()
    }
}

impl CsvReadOptions {
    pub fn csv_options(options: HashMap<String, String>) -> PlanResult<Self> {
        let options =
            serde_json::to_value(options).map_err(|e| PlanError::internal(e.to_string()))?;
        let options: CsvReadOptions =
            serde_json::from_value(options).map_err(|e| PlanError::internal(e.to_string()))?;
        Ok(options)
    }

    pub fn load_csv_options(user_options: HashMap<String, String>) -> PlanResult<Self> {
        let options: Vec<ConfigItem> =
            serde_yaml::from_str(CSV_CONFIG).map_err(|e| PlanError::internal(e.to_string()))?;
        let options: HashMap<String, String> = options
            .into_iter()
            .filter(|item| item.supported)
            .map(|item| {
                let key = item.key.to_lowercase();
                let aliases = item.alias;
                let value = if user_options.contains_key(&key) {
                    user_options
                        .get(&key)
                        .ok_or_else(|| PlanError::missing(format!("Missing value for key: {key}")))?
                        .to_string()
                } else if aliases.iter().any(|alias| user_options.contains_key(alias)) {
                    aliases
                        .iter()
                        .find_map(|alias| user_options.get(alias).cloned())
                        .ok_or_else(|| {
                            PlanError::missing(format!("Missing value for key: {key}"))
                        })?
                } else {
                    item.default
                };
                (key, value)
            })
            .collect();
        let options =
            serde_json::to_value(options).map_err(|e| PlanError::internal(e.to_string()))?;
        let options: CsvReadOptions =
            serde_json::from_value(options).map_err(|e| PlanError::internal(e.to_string()))?;
        Ok(options)
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ConfigType {
    String,
    Number,
    Boolean,
    Array,
    Map,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigItem {
    key: String,
    #[serde(default)]
    alias: Vec<String>,
    #[expect(unused)]
    r#type: ConfigType,
    default: String,
    /// The description of the configuration item.
    ///
    /// The text should be written in Markdown format.
    #[expect(unused)]
    description: String,
    #[expect(unused)]
    #[serde(default)]
    experimental: bool,
    #[expect(unused)]
    #[serde(default)]
    hidden: bool,
    #[expect(unused)]
    #[serde(default = "default_true")]
    supported: bool,
}
