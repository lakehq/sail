use std::collections::HashMap;
use std::convert::TryFrom;

use sail_common::config::CSV_CONFIG;
use serde::Deserialize;

use crate::data_source::{parse_bool, parse_non_empty_char, parse_non_empty_string, ConfigItem};
use crate::error::{PlanError, PlanResult};

/// Datasource Options that control the reading of CSV files.
#[derive(Debug, Deserialize)]
// Serde bypasses any individual field deserializers and instead uses the `TryFrom` implementation.
#[serde(try_from = "HashMap<String, String>")]
pub struct CsvReadOptions {
    pub delimiter: char,
    pub quote: char,
    pub escape: Option<char>,
    pub comment: Option<char>,
    pub header: bool,
    pub null_value: Option<String>,
    pub null_regex: Option<String>,
    pub line_sep: Option<char>,
    pub schema_infer_max_records: usize,
    pub newlines_in_values: bool,
    pub file_extension: String,
    pub compression: String,
}

impl TryFrom<HashMap<String, String>> for CsvReadOptions {
    type Error = PlanError;

    // The options HashMap should already contain all supported keys with their resolved values
    fn try_from(mut options: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(CsvReadOptions {
            delimiter: options
                .remove("delimiter")
                .ok_or_else(|| PlanError::internal("CSV `delimiter` read option is required"))
                .and_then(|v| parse_non_empty_char(&v))?
                .ok_or_else(|| {
                    PlanError::internal("CSV `delimiter` read option cannot be empty")
                })?,
            quote: options
                .remove("quote")
                .ok_or_else(|| PlanError::internal("CSV `quote` read option is required"))
                .and_then(|v| parse_non_empty_char(&v))?
                .ok_or_else(|| PlanError::internal("CSV `quote` read option cannot be empty"))?,
            escape: options
                .remove("escape")
                .ok_or_else(|| PlanError::internal("CSV `escape` read option is required"))
                .and_then(|v| parse_non_empty_char(&v))?,
            comment: options
                .remove("comment")
                .ok_or_else(|| PlanError::internal("CSV `comment` read option is required"))
                .and_then(|v| parse_non_empty_char(&v))?,
            header: options
                .remove("header")
                .ok_or_else(|| PlanError::missing("CSV `header` read option is required"))
                .and_then(|v| parse_bool(&v))?,
            null_value: options
                .remove("null_value")
                .ok_or_else(|| PlanError::internal("CSV `null_value` read option is required"))
                .map(parse_non_empty_string)?,
            null_regex: options
                .remove("null_regex")
                .ok_or_else(|| PlanError::internal("CSV `null_regex` read option is required"))
                .map(parse_non_empty_string)?,
            line_sep: options
                .remove("line_sep")
                .ok_or_else(|| PlanError::internal("CSV `line_sep` read option is required"))
                .and_then(|v| parse_non_empty_char(&v))?,
            schema_infer_max_records: options
                .remove("schema_infer_max_records")
                .ok_or_else(|| {
                    PlanError::internal("CSV `schema_infer_max_records` read option is required")
                })?
                .parse()
                .map_err(|e| {
                    PlanError::internal(format!(
                        "Invalid CSV `schema_infer_max_records` read option: {e}"
                    ))
                })?,
            newlines_in_values: options
                .remove("newlines_in_values")
                .ok_or_else(|| {
                    PlanError::missing("CSV `newlines_in_values` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            file_extension: options.remove("file_extension").ok_or_else(|| {
                PlanError::missing("CSV `file_extension` read option is required")
            })?,
            compression: options
                .remove("compression")
                .ok_or_else(|| PlanError::missing("CSV `compression` read option is required"))?,
        })
    }
}

impl CsvReadOptions {
    pub fn load(user_options: HashMap<String, String>) -> PlanResult<Self> {
        let user_options_normalized: HashMap<String, String> = user_options
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
        let config_items: Vec<ConfigItem> =
            serde_yaml::from_str(CSV_CONFIG).map_err(|e| PlanError::internal(e.to_string()))?;
        let options: HashMap<String, String> = config_items
            .into_iter()
            .filter(|item| item.supported)
            .map(|item| {
                let value = item.resolve_value(&user_options_normalized);
                let key = item.key;
                (key, value)
            })
            .collect();
        CsvReadOptions::try_from(options)
    }
}
