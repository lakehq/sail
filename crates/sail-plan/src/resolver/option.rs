use crate::data_source::csv::CsvReadOptions;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::utils::spark_datetime_format_to_chrono_strftime;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::ListingOptions;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

impl PlanResolver<'_> {
    pub(crate) fn resolve_csv_read_options(options: CsvReadOptions) -> PlanResult<ListingOptions> {
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
            .with_null_regex(options.null_regex);

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
