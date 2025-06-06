use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::ListingOptions;

use crate::data_source::csv::CsvReadOptions;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(crate) fn resolve_csv_read_options(options: CsvReadOptions) -> PlanResult<ListingOptions> {
        let null_regex = match (options.null_value, options.null_regex) {
            (Some(null_value), Some(null_regex))
                if !null_value.is_empty() && !null_regex.is_empty() =>
            {
                Err(PlanError::internal(
                    "CSV `null_value` and `null_regex` cannot be both set",
                ))
            }
            (Some(null_value), _) if !null_value.is_empty() => {
                // Convert null_value to regex by escaping special characters
                Ok(Some(regex::escape(&null_value)))
            }
            (_, Some(null_regex)) if !null_regex.is_empty() => Ok(Some(null_regex)),
            _ => Ok(None),
        }?;

        let file_format = CsvFormat::default()
            .with_has_header(options.header)
            .with_delimiter(options.delimiter.parse().map_err(|e| {
                PlanError::internal(format!("Invalid CSV `delimiter` read option: {e}"))
            })?)
            .with_quote(options.quote.parse().map_err(|e| {
                PlanError::internal(format!("Invalid CSV `quote` read option: {e}"))
            })?)
            .with_terminator(
                options
                    .line_sep
                    .map(|s| s.parse())
                    .transpose()
                    .map_err(|e| {
                        PlanError::internal(format!("Invalid CSV `line_sep` read option: {e}"))
                    })?,
            )
            .with_escape(options.escape.map(|s| s.parse()).transpose().map_err(|e| {
                PlanError::internal(format!("Invalid CSV `escape` read option: {e}"))
            })?)
            .with_comment(
                options
                    .comment
                    .map(|s| s.parse())
                    .transpose()
                    .map_err(|e| {
                        PlanError::internal(format!("Invalid CSV `comment` read option: {e}"))
                    })?,
            )
            .with_newlines_in_values(options.newlines_in_values)
            .with_schema_infer_max_rec(options.schema_infer_max_records)
            .with_file_compression_type(FileCompressionType::from_str(&options.compression)?)
            .with_null_regex(null_regex);

        Ok(ListingOptions::new(Arc::new(file_format)).with_file_extension(".csv"))
    }

    fn resolve_data_writer_option(
        format: &str,
        key: &str,
        value: &str,
    ) -> PlanResult<(String, String)> {
        let format = format.to_lowercase();
        let key = key.to_lowercase();
        let (key, value) = match (format.as_str(), key.as_str()) {
            ("csv", "header") => ("format.has_header", value),
            ("csv", "sep") => ("format.delimiter", value),
            ("csv", "linesep") => return Err(PlanError::todo("CSV writer line seperator")),
            _ => return Err(PlanError::unsupported(format!("data writer option: {key}"))),
        };
        Ok((key.to_string(), value.to_string()))
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
