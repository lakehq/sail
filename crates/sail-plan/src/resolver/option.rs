use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion_common::config::{ConfigField, TableOptions};

use crate::data_source::csv::{CsvReadOptions, CsvWriteOptions};
use crate::data_source::json::{JsonReadOptions, JsonWriteOptions};
use crate::data_source::parquet::ParquetReadOptions;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    fn char_to_u8(c: char, field_name: &str) -> PlanResult<u8> {
        if c.is_ascii() {
            Ok(c as u8)
        } else {
            Err(PlanError::internal(format!(
                "Invalid character '{c}' for {field_name}: must be an ASCII character"
            )))
        }
    }

    /// Ref: [`datafusion::datasource::file_format::options::NdJsonReadOptions`]
    pub(crate) fn resolve_json_read_options(
        options: JsonReadOptions,
        mut table_options: TableOptions,
    ) -> PlanResult<ListingOptions> {
        table_options.set_config_format(datafusion_common::config::ConfigFileType::JSON);
        let file_format = JsonFormat::default()
            .with_options(table_options.json)
            .with_schema_infer_max_rec(options.schema_infer_max_records)
            .with_file_compression_type(FileCompressionType::from_str(&options.compression)?);
        Ok(ListingOptions::new(Arc::new(file_format)).with_file_extension(".json"))
    }

    /// Ref: [`datafusion_common::file_options::json_writer::JsonWriterOptions`]
    pub(crate) fn resolve_json_write_options(
        options: JsonWriteOptions,
        mut table_options: TableOptions,
    ) -> PlanResult<(JsonFormat, Vec<(String, String)>)> {
        table_options.set_config_format(datafusion_common::config::ConfigFileType::JSON);
        let json_format = JsonFormat::default()
            .with_options(table_options.json)
            .with_file_compression_type(FileCompressionType::from_str(&options.compression)?);
        let json_options: Vec<(String, String)> =
            vec![("format.compression".to_string(), options.compression)];
        Ok((json_format, json_options))
    }

    /// Ref: [`datafusion::datasource::file_format::options::CsvReadOptions`]
    pub(crate) fn resolve_csv_read_options(
        options: CsvReadOptions,
        mut table_options: TableOptions,
    ) -> PlanResult<ListingOptions> {
        table_options.set_config_format(datafusion_common::config::ConfigFileType::CSV);
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
            .with_options(table_options.csv)
            .with_has_header(options.header)
            .with_delimiter(Self::char_to_u8(options.delimiter, "delimiter")?)
            .with_quote(Self::char_to_u8(options.quote, "quote")?)
            .with_terminator(
                options
                    .line_sep
                    .map(|line_sep| Self::char_to_u8(line_sep, "line_sep"))
                    .transpose()?,
            )
            .with_escape(
                options
                    .escape
                    .map(|escape| Self::char_to_u8(escape, "escape"))
                    .transpose()?,
            )
            .with_comment(
                options
                    .comment
                    .map(|comment| Self::char_to_u8(comment, "comment"))
                    .transpose()?,
            )
            .with_newlines_in_values(options.multi_line)
            .with_schema_infer_max_rec(options.schema_infer_max_records)
            .with_file_compression_type(FileCompressionType::from_str(&options.compression)?)
            .with_null_regex(null_regex);

        Ok(ListingOptions::new(Arc::new(file_format)).with_file_extension(".csv"))
    }

    /// Ref: [`datafusion_common::file_options::csv_writer::CsvWriterOptions`]
    pub(crate) fn resolve_csv_write_options(
        options: CsvWriteOptions,
        mut table_options: TableOptions,
    ) -> PlanResult<(CsvFormat, Vec<(String, String)>)> {
        table_options.set_config_format(datafusion_common::config::ConfigFileType::CSV);
        table_options.set(
            "format.double_quote",
            options.escape_quotes.to_string().as_str(),
        )?;
        if let Some(null_value) = &options.null_value {
            table_options.set("format.null_value", null_value)?;
        }

        let csv_format = CsvFormat::default()
            .with_options(table_options.csv)
            .with_delimiter(Self::char_to_u8(options.delimiter, "delimiter")?)
            .with_quote(Self::char_to_u8(options.quote, "quote")?)
            .with_escape(
                options
                    .escape
                    .map(|escape| Self::char_to_u8(escape, "escape"))
                    .transpose()?,
            )
            .with_has_header(options.header)
            .with_file_compression_type(FileCompressionType::from_str(&options.compression)?);

        let mut csv_options: Vec<(String, String)> = vec![];
        csv_options.push((
            "format.delimiter".to_string(),
            options.delimiter.to_string(),
        ));
        csv_options.push(("format.quote".to_string(), options.quote.to_string()));
        if let Some(escape) = options.escape {
            csv_options.push(("format.escape".to_string(), escape.to_string()));
        }
        csv_options.push((
            "format.double_quote".to_string(),
            options.escape_quotes.to_string(),
        ));
        csv_options.push(("format.has_header".to_string(), options.header.to_string()));
        if let Some(null_value) = options.null_value {
            csv_options.push(("format.null_value".to_string(), null_value));
        }
        csv_options.push(("format.compression".to_string(), options.compression));
        Ok((csv_format, csv_options))
    }

    /// Ref: [`datafusion_common::config:ParquetOptions`]
    pub(crate) fn resolve_parquet_read_options(
        options: ParquetReadOptions,
        mut table_options: TableOptions,
    ) -> PlanResult<ListingOptions> {
        let mut option_map: HashMap<String, String> = HashMap::new();
        option_map.insert(
            "format.enable_page_index".to_owned(),
            options.enable_page_index.to_string(),
        );
        option_map.insert("format.pruning".to_owned(), options.pruning.to_string());
        option_map.insert(
            "format.skip_metadata".to_owned(),
            options.skip_metadata.to_string(),
        );
        if let Some(metadata_size_hint) = options.metadata_size_hint {
            option_map.insert(
                "format.metadata_size_hint".to_owned(),
                metadata_size_hint.to_string(),
            );
        }
        option_map.insert(
            "format.pushdown_filters".to_owned(),
            options.pushdown_filters.to_string(),
        );
        option_map.insert(
            "format.reorder_filters".to_owned(),
            options.reorder_filters.to_string(),
        );
        option_map.insert(
            "format.schema_force_view_types".to_owned(),
            options.schema_force_view_types.to_string(),
        );
        option_map.insert(
            "format.binary_as_string".to_owned(),
            options.binary_as_string.to_string(),
        );
        option_map.insert("format.coerce_int96".to_owned(), options.coerce_int96);
        option_map.insert(
            "format.bloom_filter_on_read".to_owned(),
            options.bloom_filter_on_read.to_string(),
        );
        table_options.set_config_format(datafusion_common::config::ConfigFileType::PARQUET);
        table_options.alter_with_string_hash_map(&option_map)?;
        let file_format = ParquetFormat::new().with_options(table_options.parquet);
        Ok(ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet"))
    }
}
