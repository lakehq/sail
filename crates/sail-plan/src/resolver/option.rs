use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;

use crate::data_source::csv::{CsvReadOptions, CsvWriteOptions};
use crate::data_source::json::{JsonReadOptions, JsonWriteOptions};
use crate::data_source::parquet::{ParquetReadOptions, ParquetWriteOptions};
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
        &self,
        options: JsonReadOptions,
    ) -> PlanResult<ListingOptions> {
        let mut table_options = self.ctx.copied_table_options();
        table_options.set_config_format(datafusion_common::config::ConfigFileType::JSON);
        let file_format = JsonFormat::default()
            .with_options(table_options.json)
            .with_schema_infer_max_rec(options.schema_infer_max_records)
            .with_file_compression_type(FileCompressionType::from_str(&options.compression)?);
        Ok(ListingOptions::new(Arc::new(file_format)).with_file_extension(".json"))
    }

    /// Ref: [`datafusion_common::file_options::json_writer::JsonWriterOptions`]
    pub(crate) fn resolve_json_write_options(
        &self,
        options: JsonWriteOptions,
    ) -> PlanResult<(JsonFormat, Vec<(String, String)>)> {
        let mut table_options = self.ctx.copied_table_options();
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
        &self,
        options: CsvReadOptions,
    ) -> PlanResult<ListingOptions> {
        let mut table_options = self.ctx.copied_table_options();
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

        let file_extension = if options.file_extension.is_empty() {
            ".csv".to_string()
        } else if options.file_extension.starts_with('.') {
            options.file_extension
        } else {
            format!(".{}", options.file_extension)
        };

        Ok(ListingOptions::new(Arc::new(file_format)).with_file_extension(file_extension))
    }

    /// Ref: [`datafusion_common::file_options::csv_writer::CsvWriterOptions`]
    pub(crate) fn resolve_csv_write_options(
        &self,
        options: CsvWriteOptions,
    ) -> PlanResult<(CsvFormat, Vec<(String, String)>)> {
        let mut table_options = self.ctx.copied_table_options();
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
        &self,
        options: ParquetReadOptions,
    ) -> PlanResult<ListingOptions> {
        let mut parquet_options: HashMap<String, String> = HashMap::new();
        let mut table_options = self.ctx.copied_table_options();
        table_options.set_config_format(datafusion_common::config::ConfigFileType::PARQUET);
        parquet_options.insert(
            "format.enable_page_index".to_owned(),
            options.enable_page_index.to_string(),
        );
        parquet_options.insert("format.pruning".to_owned(), options.pruning.to_string());
        parquet_options.insert(
            "format.skip_metadata".to_owned(),
            options.skip_metadata.to_string(),
        );
        if let Some(metadata_size_hint) = options.metadata_size_hint {
            parquet_options.insert(
                "format.metadata_size_hint".to_owned(),
                metadata_size_hint.to_string(),
            );
        }
        parquet_options.insert(
            "format.pushdown_filters".to_owned(),
            options.pushdown_filters.to_string(),
        );
        parquet_options.insert(
            "format.reorder_filters".to_owned(),
            options.reorder_filters.to_string(),
        );
        parquet_options.insert(
            "format.schema_force_view_types".to_owned(),
            options.schema_force_view_types.to_string(),
        );
        parquet_options.insert(
            "format.binary_as_string".to_owned(),
            options.binary_as_string.to_string(),
        );
        parquet_options.insert("format.coerce_int96".to_owned(), options.coerce_int96);
        parquet_options.insert(
            "format.bloom_filter_on_read".to_owned(),
            options.bloom_filter_on_read.to_string(),
        );
        table_options.alter_with_string_hash_map(&parquet_options)?;
        let parquet_format = ParquetFormat::new().with_options(table_options.parquet);
        Ok(ListingOptions::new(Arc::new(parquet_format)).with_file_extension(".parquet"))
    }

    /// Ref: [`datafusion_common::config:ParquetOptions`]
    pub(crate) fn resolve_parquet_write_options(
        &self,
        options: ParquetWriteOptions,
    ) -> PlanResult<(ParquetFormat, Vec<(String, String)>)> {
        let mut parquet_options: HashMap<String, String> = HashMap::new();
        let mut table_options = self.ctx.copied_table_options();
        table_options.set_config_format(datafusion_common::config::ConfigFileType::PARQUET);
        parquet_options.insert(
            "format.data_pagesize_limit".to_owned(),
            options.data_page_size_limit.to_string(),
        );
        parquet_options.insert(
            "format.write_batch_size".to_owned(),
            options.write_batch_size.to_string(),
        );
        parquet_options.insert("format.writer_version".to_owned(), options.writer_version);
        parquet_options.insert(
            "format.skip_arrow_metadata".to_owned(),
            options.skip_arrow_metadata.to_string(),
        );
        parquet_options.insert("format.compression".to_owned(), options.compression);
        parquet_options.insert(
            "format.dictionary_enabled".to_owned(),
            options.dictionary_enabled.to_string(),
        );
        parquet_options.insert(
            "format.dictionary_page_size_limit".to_owned(),
            options.dictionary_page_size_limit.to_string(),
        );
        parquet_options.insert(
            "format.statistics_enabled".to_owned(),
            options.statistics_enabled,
        );
        parquet_options.insert(
            "format.max_row_group_size".to_owned(),
            options.max_row_group_size.to_string(),
        );
        if let Some(column_index_truncate_length) = options.column_index_truncate_length {
            parquet_options.insert(
                "format.column_index_truncate_length".to_owned(),
                column_index_truncate_length.to_string(),
            );
        }
        if let Some(statistics_truncate_length) = options.statistics_truncate_length {
            parquet_options.insert(
                "format.statistics_truncate_length".to_owned(),
                statistics_truncate_length.to_string(),
            );
        }
        parquet_options.insert(
            "format.data_page_row_count_limit".to_owned(),
            options.data_page_row_count_limit.to_string(),
        );
        if let Some(encoding) = options.encoding {
            parquet_options.insert("format.encoding".to_owned(), encoding);
        }
        parquet_options.insert(
            "format.bloom_filter_on_write".to_owned(),
            options.bloom_filter_on_write.to_string(),
        );
        parquet_options.insert(
            "format.bloom_filter_fpp".to_owned(),
            options.bloom_filter_fpp.to_string(),
        );
        parquet_options.insert(
            "format.bloom_filter_ndv".to_owned(),
            options.bloom_filter_ndv.to_string(),
        );
        parquet_options.insert(
            "format.allow_single_file_parallelism".to_owned(),
            options.allow_single_file_parallelism.to_string(),
        );
        parquet_options.insert(
            "format.maximum_parallel_row_group_writers".to_owned(),
            options.maximum_parallel_row_group_writers.to_string(),
        );
        parquet_options.insert(
            "format.maximum_buffered_record_batches_per_stream".to_owned(),
            options
                .maximum_buffered_record_batches_per_stream
                .to_string(),
        );
        table_options.alter_with_string_hash_map(&parquet_options)?;
        let parquet_format = ParquetFormat::new().with_options(table_options.parquet);
        Ok((parquet_format, parquet_options.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use super::*;
    use crate::config::PlanConfig;

    #[test]
    fn test_resolve_json_read_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = JsonReadOptions {
            schema_infer_max_records: 100,
            compression: "bzip2".to_string(),
        };
        let listing_options = resolver.resolve_json_read_options(options)?;
        let format = listing_options
            .format
            .as_any()
            .downcast_ref::<JsonFormat>()
            .expect("Expected JsonFormat");

        assert_eq!(listing_options.file_extension, ".json");
        assert_eq!(format.options().schema_infer_max_rec, Some(100));
        assert_eq!(format.options().compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_json_write_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = JsonWriteOptions {
            compression: "bzip2".to_string(),
        };
        let (format, json_options) = resolver.resolve_json_write_options(options)?;
        let json_options: HashMap<String, String> = json_options.into_iter().collect();

        assert_eq!(json_options.len(), 1);
        assert_eq!(
            json_options.get("format.compression"),
            Some(&"bzip2".to_string())
        );

        assert_eq!(format.options().compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_csv_read_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = CsvReadOptions {
            delimiter: '!',
            quote: '(',
            escape: Some('*'),
            comment: Some('^'),
            header: true,
            null_value: Some("MEOW".to_string()),
            null_regex: None,
            line_sep: Some('@'),
            schema_infer_max_records: 100,
            multi_line: true,
            file_extension: ".lol".to_string(),
            compression: "bzip2".to_string(),
        };
        let listing_options = resolver.resolve_csv_read_options(options)?;
        let format = listing_options
            .format
            .as_any()
            .downcast_ref::<CsvFormat>()
            .expect("Expected CsvFormat");

        assert_eq!(listing_options.file_extension, ".lol");
        assert_eq!(format.options().delimiter, b'!');
        assert_eq!(format.options().quote, b'(');
        assert_eq!(format.options().escape, Some(b'*'));
        assert_eq!(format.options().comment, Some(b'^'));
        assert_eq!(format.options().has_header, Some(true));
        assert_eq!(format.options().null_value, None); // This is for the writer
        assert_eq!(format.options().null_regex, Some("MEOW".to_string())); // null_value
        assert_eq!(format.options().terminator, Some(b'@')); // line_sep
        assert_eq!(format.options().schema_infer_max_rec, Some(100));
        assert_eq!(format.options().newlines_in_values, Some(true)); // multi_line
        assert_eq!(format.options().compression, CompressionTypeVariant::BZIP2);

        let options = CsvReadOptions {
            delimiter: '!',
            quote: '(',
            escape: Some('*'),
            comment: Some('^'),
            header: true,
            null_value: Some("MEOW".to_string()),
            null_regex: Some("MEOW".to_string()),
            line_sep: Some('@'),
            schema_infer_max_records: 100,
            multi_line: true,
            file_extension: ".lol".to_string(),
            compression: "bzip2".to_string(),
        };
        // null_value and null_regex cannot both be set
        let result = resolver.resolve_csv_read_options(options);
        assert!(result.is_err());

        let options = CsvReadOptions {
            delimiter: '!',
            quote: '(',
            escape: Some('*'),
            comment: Some('^'),
            header: true,
            null_value: None,
            null_regex: Some("MEOW".to_string()),
            line_sep: Some('@'),
            schema_infer_max_records: 100,
            multi_line: true,
            file_extension: "lol".to_string(), // No leading dot
            compression: "bzip2".to_string(),
        };
        let listing_options = resolver.resolve_csv_read_options(options)?;
        let format = listing_options
            .format
            .as_any()
            .downcast_ref::<CsvFormat>()
            .expect("Expected CsvFormat");
        assert_eq!(listing_options.file_extension, ".lol");
        assert_eq!(format.options().null_value, None); // This is for the writer
        assert_eq!(format.options().null_regex, Some("MEOW".to_string())); // null_regex

        Ok(())
    }

    #[test]
    fn test_resolve_csv_write_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = CsvWriteOptions {
            delimiter: '!',
            quote: '(',
            escape: Some('*'),
            escape_quotes: true,
            header: true,
            null_value: Some("MEOW".to_string()),
            compression: "bzip2".to_string(),
        };

        let (format, csv_options) = resolver.resolve_csv_write_options(options)?;
        let csv_options: HashMap<String, String> = csv_options.into_iter().collect();

        assert_eq!(csv_options.len(), 7);
        assert_eq!(csv_options.get("format.delimiter"), Some(&"!".to_string()));
        assert_eq!(csv_options.get("format.quote"), Some(&"(".to_string()));
        assert_eq!(csv_options.get("format.escape"), Some(&"*".to_string()));
        assert_eq!(
            csv_options.get("format.double_quote"),
            Some(&"true".to_string())
        );
        assert_eq!(
            csv_options.get("format.has_header"),
            Some(&"true".to_string())
        );
        assert_eq!(
            csv_options.get("format.null_value"),
            Some(&"MEOW".to_string())
        );
        assert_eq!(
            csv_options.get("format.compression"),
            Some(&"bzip2".to_string())
        );

        assert_eq!(format.options().delimiter, b'!');
        assert_eq!(format.options().quote, b'(');
        assert_eq!(format.options().escape, Some(b'*'));
        assert_eq!(format.options().double_quote, Some(true)); // escape_quotes
        assert_eq!(format.options().has_header, Some(true));
        assert_eq!(format.options().null_value, Some("MEOW".to_string()));
        assert_eq!(format.options().compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_read_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        // pub coerce_int96: String,
        // pub bloom_filter_on_read: bool,
        let options = ParquetReadOptions {
            enable_page_index: true,
            pruning: true,
            skip_metadata: false,
            metadata_size_hint: Some(1024),
            pushdown_filters: true,
            reorder_filters: false,
            schema_force_view_types: true,
            binary_as_string: true,
            coerce_int96: "ms".to_string(),
            bloom_filter_on_read: true,
        };
        let listing_options = resolver.resolve_parquet_read_options(options)?;
        let format = listing_options
            .format
            .as_any()
            .downcast_ref::<ParquetFormat>()
            .expect("Expected ParquetFormat");

        assert_eq!(listing_options.file_extension, ".parquet");
        assert!(format.options().global.enable_page_index);
        assert!(format.options().global.pruning);
        assert!(!format.options().global.skip_metadata);
        assert_eq!(format.options().global.metadata_size_hint, Some(1024));
        assert!(format.options().global.pushdown_filters);
        assert!(!format.options().global.reorder_filters);
        assert!(format.options().global.schema_force_view_types);
        assert!(format.options().global.binary_as_string);
        assert_eq!(format.options().global.coerce_int96, Some("ms".to_string()));
        assert!(format.options().global.bloom_filter_on_read);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = ParquetWriteOptions {
            data_page_size_limit: 1024,
            write_batch_size: 1000,
            writer_version: "2.0".to_string(),
            skip_arrow_metadata: true,
            compression: "snappy".to_string(),
            dictionary_enabled: true,
            dictionary_page_size_limit: 2048,
            statistics_enabled: "chunk".to_string(),
            max_row_group_size: 5000,
            column_index_truncate_length: Some(100),
            statistics_truncate_length: Some(200),
            data_page_row_count_limit: 10000,
            encoding: Some("delta_binary_packed".to_string()),
            bloom_filter_on_write: true,
            bloom_filter_fpp: 0.01,
            bloom_filter_ndv: 1000,
            allow_single_file_parallelism: false,
            maximum_parallel_row_group_writers: 4,
            maximum_buffered_record_batches_per_stream: 10,
        };
        let (format, parquet_options) = resolver.resolve_parquet_write_options(options)?;
        let parquet_options: HashMap<String, String> = parquet_options.into_iter().collect();

        assert_eq!(parquet_options.len(), 19);
        assert_eq!(
            parquet_options.get("format.data_pagesize_limit"),
            Some(&"1024".to_string())
        );
        assert_eq!(
            parquet_options.get("format.write_batch_size"),
            Some(&"1000".to_string())
        );
        assert_eq!(
            parquet_options.get("format.writer_version"),
            Some(&"2.0".to_string())
        );
        assert_eq!(
            parquet_options.get("format.skip_arrow_metadata"),
            Some(&"true".to_string())
        );
        assert_eq!(
            parquet_options.get("format.compression"),
            Some(&"snappy".to_string())
        );
        assert_eq!(
            parquet_options.get("format.dictionary_enabled"),
            Some(&"true".to_string())
        );
        assert_eq!(
            parquet_options.get("format.dictionary_page_size_limit"),
            Some(&"2048".to_string())
        );
        assert_eq!(
            parquet_options.get("format.statistics_enabled"),
            Some(&"chunk".to_string())
        );
        assert_eq!(
            parquet_options.get("format.max_row_group_size"),
            Some(&"5000".to_string())
        );
        assert_eq!(
            parquet_options.get("format.column_index_truncate_length"),
            Some(&"100".to_string())
        );
        assert_eq!(
            parquet_options.get("format.statistics_truncate_length"),
            Some(&"200".to_string())
        );
        assert_eq!(
            parquet_options.get("format.data_page_row_count_limit"),
            Some(&"10000".to_string())
        );
        assert_eq!(
            parquet_options.get("format.encoding"),
            Some(&"delta_binary_packed".to_string())
        );
        assert_eq!(
            parquet_options.get("format.bloom_filter_on_write"),
            Some(&"true".to_string())
        );
        assert_eq!(
            parquet_options.get("format.bloom_filter_fpp"),
            Some(&"0.01".to_string())
        );
        assert_eq!(
            parquet_options.get("format.bloom_filter_ndv"),
            Some(&"1000".to_string())
        );
        assert_eq!(
            parquet_options.get("format.allow_single_file_parallelism"),
            Some(&"false".to_string())
        );
        assert_eq!(
            parquet_options.get("format.maximum_parallel_row_group_writers"),
            Some(&"4".to_string())
        );
        assert_eq!(
            parquet_options.get("format.maximum_buffered_record_batches_per_stream"),
            Some(&"10".to_string())
        );

        assert_eq!(format.options().global.data_pagesize_limit, 1024);
        assert_eq!(format.options().global.write_batch_size, 1000);
        assert_eq!(format.options().global.writer_version, "2.0");
        assert!(format.options().global.skip_arrow_metadata);
        assert_eq!(
            format.options().global.compression,
            Some("snappy".to_string())
        );
        assert_eq!(format.options().global.dictionary_enabled, Some(true));
        assert_eq!(format.options().global.dictionary_page_size_limit, 2048);
        assert_eq!(
            format.options().global.statistics_enabled,
            Some("chunk".to_string())
        );
        assert_eq!(format.options().global.max_row_group_size, 5000);
        assert_eq!(
            format.options().global.column_index_truncate_length,
            Some(100)
        );
        assert_eq!(
            format.options().global.statistics_truncate_length,
            Some(200)
        );
        assert_eq!(format.options().global.data_page_row_count_limit, 10000);
        assert_eq!(
            format.options().global.encoding,
            Some("delta_binary_packed".to_string())
        );
        assert!(format.options().global.bloom_filter_on_write);
        assert_eq!(format.options().global.bloom_filter_fpp, Some(0.01));
        assert_eq!(format.options().global.bloom_filter_ndv, Some(1000));
        assert!(!format.options().global.allow_single_file_parallelism);
        assert_eq!(
            format.options().global.maximum_parallel_row_group_writers,
            4
        );
        assert_eq!(
            format
                .options()
                .global
                .maximum_buffered_record_batches_per_stream,
            10
        );

        Ok(())
    }
}
