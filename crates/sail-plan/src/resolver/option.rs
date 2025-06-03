use std::collections::HashMap;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::utils::spark_datetime_format_to_chrono_strftime;

impl PlanResolver<'_> {
    /// CSV read options: [`datafusion::datasource::file_format::options::CsvReadOptions`]
    fn resolve_data_reader_option(
        format: &str,
        key: &str,
        value: &str,
    ) -> PlanResult<(String, String)> {
        let format = format.to_lowercase();
        let key = key.to_lowercase();
        let (key, value): (String, String) = match (format.as_str(), key.as_str()) {
            ("csv", "header") | ("csv", "has_header") => {
                ("format.has_header".to_string(), value.to_string())
            }
            ("csv", "sep") | ("csv", "delimiter") => {
                ("format.delimiter".to_string(), value.to_string())
            }
            ("csv", "quote") => ("format.quote".to_string(), value.to_string()),
            ("csv", "linesep") | ("csv", "terminator") => {
                ("format.terminator".to_string(), value.to_string())
            }
            ("csv", "escape") => ("format.escape".to_string(), value.to_string()),
            ("csv", "escapequotes") | ("csv", "double_quote") => {
                ("format.double_quote".to_string(), value.to_string())
            }
            ("csv", "newlines_in_values") => {
                ("format.newlines_in_values".to_string(), value.to_string())
            }
            ("csv", "compression") | ("csv", "file_compression_type") => (
                "format.file_compression_type".to_string(),
                value.to_string(),
            ),
            ("csv", "schema_infer_max_rec") => {
                ("format.schema_infer_max_rec".to_string(), value.to_string())
            }
            ("csv", "dateformat") | ("csv", "date_format") => (
                "format.date_format".to_string(),
                spark_datetime_format_to_chrono_strftime(value)?,
            ),
            ("csv", "datetime_format") => ("format.datetime_format".to_string(), value.to_string()),
            ("csv", "timestampntzformat") | ("csv", "timestamp_format") => (
                "format.timestamp_format".to_string(),
                spark_datetime_format_to_chrono_strftime(value)?,
            ),
            ("csv", "timestampformat") | ("csv", "timestamp_tz_format") => (
                "format.timestamp_tz_format".to_string(),
                spark_datetime_format_to_chrono_strftime(value)?,
            ),
            ("csv", "time_format") => ("format.time_format".to_string(), value.to_string()),
            ("csv", "nullvalue") | ("csv", "null_value") => {
                ("format.null_value".to_string(), value.to_string())
            }
            ("csv", "null_regex") => ("format.null_regex".to_string(), value.to_string()),
            ("csv", "comment") => ("format.comment".to_string(), value.to_string()),
            _ => return Err(PlanError::unsupported(format!("data reader option: {key}"))),
        };
        Ok((key, value))
    }

    pub(crate) fn resolve_data_reader_options(
        format: &str,
        options: Vec<(String, String)>,
    ) -> PlanResult<HashMap<String, String>> {
        let mut output = HashMap::new();
        for (key, value) in options {
            let (k, v) = PlanResolver::resolve_data_reader_option(format, &key, &value)?;
            if output.insert(k, v).is_some() {
                return Err(PlanError::invalid(format!(
                    "duplicated data reader option key: {key}"
                )));
            }
        }
        Ok(output)
    }

    /// CSV write options: [`datafusion_common::file_options::csv_writer::CsvWriterOptions`]
    fn resolve_data_writer_option(
        format: &str,
        key: &str,
        value: &str,
    ) -> PlanResult<(String, String)> {
        let format = format.to_lowercase();
        let key = key.to_lowercase();
        let (key, value): (String, String) = match (format.as_str(), key.as_str()) {
            ("csv", "header") | ("csv", "has_header") => {
                ("format.has_header".to_string(), value.to_string())
            }
            ("csv", "sep") | ("csv", "delimiter") => {
                ("format.delimiter".to_string(), value.to_string())
            }
            ("csv", "quote") => ("format.quote".to_string(), value.to_string()),
            ("csv", "linesep") | ("csv", "terminator") => {
                return Err(PlanError::todo("CSV writer line seperator"))
            }
            ("csv", "escape") => ("format.escape".to_string(), value.to_string()),
            ("csv", "escapequotes") | ("csv", "double_quote") => {
                ("format.double_quote".to_string(), value.to_string())
            }
            ("csv", "newlines_in_values") => {
                ("format.newlines_in_values".to_string(), value.to_string())
            }
            ("csv", "compression") => ("format.compression".to_string(), value.to_string()),
            ("csv", "schema_infer_max_rec") => {
                ("format.schema_infer_max_rec".to_string(), value.to_string())
            }
            ("csv", "dateformat") | ("csv", "date_format") => (
                "format.date_format".to_string(),
                spark_datetime_format_to_chrono_strftime(value)?,
            ),
            ("csv", "datetime_format") => ("format.datetime_format".to_string(), value.to_string()),
            ("csv", "timestampntzformat") | ("csv", "timestamp_format") => (
                "format.timestamp_format".to_string(),
                spark_datetime_format_to_chrono_strftime(value)?,
            ),
            ("csv", "timestampformat") | ("csv", "timestamp_tz_format") => (
                "format.timestamp_tz_format".to_string(),
                spark_datetime_format_to_chrono_strftime(value)?,
            ),
            ("csv", "time_format") => ("format.time_format".to_string(), value.to_string()),
            ("csv", "nullvalue") | ("csv", "null_value") => {
                ("format.null_value".to_string(), value.to_string())
            }
            ("csv", "null_regex") => ("format.null_regex".to_string(), value.to_string()),
            ("csv", "comment") => ("format.comment".to_string(), value.to_string()),
            _ => return Err(PlanError::unsupported(format!("data writer option: {key}"))),
        };
        Ok((key, value))
    }

    pub(crate) fn resolve_data_writer_options(
        format: &str,
        options: Vec<(String, String)>,
    ) -> PlanResult<HashMap<String, String>> {
        let mut output = HashMap::new();
        for (key, value) in options {
            let (k, v) = PlanResolver::resolve_data_writer_option(format, &key, &value)?;
            if output.insert(k, v).is_some() {
                return Err(PlanError::invalid(format!(
                    "duplicated data writer option key: {key}"
                )));
            }
        }
        Ok(output)
    }
}
