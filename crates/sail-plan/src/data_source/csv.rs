use std::collections::HashMap;
use std::convert::TryFrom;

use sail_common::config::{CSV_READ_CONFIG, CSV_WRITE_CONFIG};
use serde::Deserialize;

use crate::data_source::{
    parse_bool, parse_non_empty_char, parse_non_empty_string, DataSourceOptions,
};
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
    pub multi_line: bool,
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
            multi_line: options
                .remove("multi_line")
                .ok_or_else(|| PlanError::missing("CSV `multi_line` read option is required"))
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

impl DataSourceOptions for CsvReadOptions {
    const SOURCE_CONFIG: &'static str = CSV_READ_CONFIG;
    fn try_from_options(options: HashMap<String, String>) -> PlanResult<Self> {
        Self::try_from(options)
    }
}

/// Datasource Options that control the writing of CSV files.
#[derive(Debug, Deserialize)]
// Serde bypasses any individual field deserializers and instead uses the `TryFrom` implementation.
#[serde(try_from = "HashMap<String, String>")]
pub struct CsvWriteOptions {
    pub delimiter: char,
    pub quote: char,
    pub escape: Option<char>,
    pub escape_quotes: bool,
    pub header: bool,
    pub null_value: Option<String>,
    pub compression: String,
}

impl TryFrom<HashMap<String, String>> for CsvWriteOptions {
    type Error = PlanError;

    // The options HashMap should already contain all supported keys with their resolved values
    fn try_from(mut options: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(CsvWriteOptions {
            delimiter: options
                .remove("delimiter")
                .ok_or_else(|| PlanError::internal("CSV `delimiter` write option is required"))
                .and_then(|v| parse_non_empty_char(&v))?
                .ok_or_else(|| {
                    PlanError::internal("CSV `delimiter` write option cannot be empty")
                })?,
            quote: options
                .remove("quote")
                .ok_or_else(|| PlanError::internal("CSV `quote` write option is required"))
                .and_then(|v| parse_non_empty_char(&v))?
                .ok_or_else(|| PlanError::internal("CSV `quote` write option cannot be empty"))?,
            escape: options
                .remove("escape")
                .ok_or_else(|| PlanError::internal("CSV `escape` write option is required"))
                .and_then(|v| parse_non_empty_char(&v))?,
            escape_quotes: options
                .remove("escape_quotes")
                .ok_or_else(|| PlanError::internal("CSV `escape_quotes` write option is required"))
                .and_then(|v| parse_bool(&v))?,
            header: options
                .remove("header")
                .ok_or_else(|| PlanError::missing("CSV `header` write option is required"))
                .and_then(|v| parse_bool(&v))?,
            null_value: options
                .remove("null_value")
                .ok_or_else(|| PlanError::internal("CSV `null_value` write option is required"))
                .map(parse_non_empty_string)?,
            compression: options
                .remove("compression")
                .ok_or_else(|| PlanError::missing("CSV `compression` write option is required"))?,
        })
    }
}

impl DataSourceOptions for CsvWriteOptions {
    const SOURCE_CONFIG: &'static str = CSV_WRITE_CONFIG;
    fn try_from_options(options: HashMap<String, String>) -> PlanResult<Self> {
        Self::try_from(options)
    }
}
