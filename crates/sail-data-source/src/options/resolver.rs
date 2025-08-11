use std::collections::HashMap;
use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion_common::config::{CsvOptions, JsonOptions, TableParquetOptions};
use datafusion_common::{plan_err, Result};
use sail_common_datafusion::datasource::TableDeltaOptions;

use crate::options::{
    load_default_options, load_options, CsvReadOptions, CsvWriteOptions, DeltaReadOptions,
    DeltaWriteOptions, JsonReadOptions, JsonWriteOptions, ParquetReadOptions, ParquetWriteOptions,
};

fn char_to_u8(c: char, option: &str) -> Result<u8> {
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        plan_err!("invalid {option} character '{c}': must be an ASCII character")
    }
}

fn apply_json_read_options(from: JsonReadOptions, to: &mut JsonOptions) -> Result<()> {
    let JsonReadOptions {
        schema_infer_max_records,
        compression,
    } = from;
    if let Some(v) = schema_infer_max_records {
        to.schema_infer_max_rec = Some(v);
    }
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    Ok(())
}

fn apply_json_write_options(from: JsonWriteOptions, to: &mut JsonOptions) -> Result<()> {
    let JsonWriteOptions { compression } = from;
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    Ok(())
}

fn apply_csv_read_options(from: CsvReadOptions, to: &mut CsvOptions) -> Result<()> {
    let CsvReadOptions {
        delimiter,
        quote,
        escape,
        comment,
        header,
        null_value,
        null_regex,
        line_sep,
        schema_infer_max_records,
        multi_line,
        compression,
    } = from;
    let null_regex = match (null_value, null_regex) {
        (Some(null_value), Some(null_regex))
            if !null_value.is_empty() && !null_regex.is_empty() =>
        {
            return plan_err!("CSV `null_value` and `null_regex` cannot be both set");
        }
        (Some(null_value), _) if !null_value.is_empty() => {
            // Convert null value to regex by escaping special characters
            Some(regex::escape(&null_value))
        }
        (_, Some(null_regex)) if !null_regex.is_empty() => Some(null_regex),
        _ => None,
    };
    if let Some(null_regex) = null_regex {
        to.null_regex = Some(null_regex);
    }
    if let Some(delimiter) = delimiter {
        to.delimiter = char_to_u8(delimiter, "delimiter")?;
    }
    // TODO: support no quote
    if let Some(Some(quote)) = quote {
        to.quote = char_to_u8(quote, "quote")?;
    }
    // TODO: support no escape
    if let Some(Some(escape)) = escape {
        to.escape = Some(char_to_u8(escape, "escape")?);
    }
    // TODO: support no comment
    if let Some(Some(comment)) = comment {
        to.comment = Some(char_to_u8(comment, "comment")?);
    }
    if let Some(header) = header {
        to.has_header = Some(header);
    }
    if let Some(Some(sep)) = line_sep {
        to.terminator = Some(char_to_u8(sep, "line_sep")?);
    }
    if let Some(n) = schema_infer_max_records {
        to.schema_infer_max_rec = Some(n);
    }
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    if let Some(multi_line) = multi_line {
        to.newlines_in_values = Some(multi_line);
    }
    Ok(())
}

fn apply_csv_write_options(from: CsvWriteOptions, to: &mut CsvOptions) -> Result<()> {
    let CsvWriteOptions {
        delimiter,
        quote,
        escape,
        escape_quotes,
        header,
        null_value,
        compression,
    } = from;
    if let Some(delimiter) = delimiter {
        to.delimiter = char_to_u8(delimiter, "delimiter")?;
    }
    // TODO: support no quote
    if let Some(Some(quote)) = quote {
        to.quote = char_to_u8(quote, "quote")?;
    }
    // TODO: support no escape
    if let Some(Some(escape)) = escape {
        to.escape = Some(char_to_u8(escape, "escape")?);
    }
    if let Some(escape_quotes) = escape_quotes {
        to.double_quote = Some(escape_quotes);
    }
    if let Some(header) = header {
        to.has_header = Some(header);
    }
    if let Some(null_value) = null_value {
        to.null_value = Some(null_value);
    }
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    Ok(())
}

fn apply_parquet_read_options(
    from: ParquetReadOptions,
    to: &mut TableParquetOptions,
) -> Result<()> {
    let ParquetReadOptions {
        enable_page_index,
        pruning,
        skip_metadata,
        metadata_size_hint,
        pushdown_filters,
        reorder_filters,
        schema_force_view_types,
        binary_as_string,
        coerce_int96,
        bloom_filter_on_read,
    } = from;
    if let Some(v) = enable_page_index {
        to.global.enable_page_index = v;
    }
    if let Some(v) = pruning {
        to.global.pruning = v;
    }
    if let Some(v) = skip_metadata {
        to.global.skip_metadata = v;
    }
    if let Some(v) = metadata_size_hint {
        to.global.metadata_size_hint = Some(v);
    }
    if let Some(v) = pushdown_filters {
        to.global.pushdown_filters = v;
    }
    if let Some(v) = reorder_filters {
        to.global.reorder_filters = v;
    }
    if let Some(v) = schema_force_view_types {
        to.global.schema_force_view_types = v;
    }
    if let Some(v) = binary_as_string {
        to.global.binary_as_string = v;
    }
    if let Some(v) = coerce_int96 {
        to.global.coerce_int96 = Some(v);
    }
    if let Some(v) = bloom_filter_on_read {
        to.global.bloom_filter_on_read = v;
    }
    Ok(())
}

fn apply_parquet_write_options(
    from: ParquetWriteOptions,
    to: &mut TableParquetOptions,
) -> Result<()> {
    let ParquetWriteOptions {
        data_page_size_limit,
        write_batch_size,
        writer_version,
        skip_arrow_metadata,
        compression,
        dictionary_enabled,
        dictionary_page_size_limit,
        statistics_enabled,
        max_row_group_size,
        column_index_truncate_length,
        statistics_truncate_length,
        data_page_row_count_limit,
        encoding,
        bloom_filter_on_write,
        bloom_filter_fpp,
        bloom_filter_ndv,
        allow_single_file_parallelism,
        maximum_parallel_row_group_writers,
        maximum_buffered_record_batches_per_stream,
    } = from;
    if let Some(v) = data_page_size_limit {
        to.global.data_pagesize_limit = v;
    }
    if let Some(v) = write_batch_size {
        to.global.write_batch_size = v;
    }
    if let Some(v) = writer_version {
        to.global.writer_version = v;
    }
    if let Some(v) = skip_arrow_metadata {
        to.global.skip_arrow_metadata = v;
    }
    if let Some(v) = compression {
        to.global.compression = Some(v);
    }
    if let Some(v) = dictionary_enabled {
        to.global.dictionary_enabled = Some(v);
    }
    if let Some(v) = dictionary_page_size_limit {
        to.global.dictionary_page_size_limit = v;
    }
    if let Some(v) = statistics_enabled {
        to.global.statistics_enabled = Some(v);
    }
    if let Some(v) = max_row_group_size {
        to.global.max_row_group_size = v;
    }
    if let Some(v) = column_index_truncate_length {
        to.global.column_index_truncate_length = Some(v);
    }
    if let Some(v) = statistics_truncate_length {
        to.global.statistics_truncate_length = Some(v);
    }
    if let Some(v) = data_page_row_count_limit {
        to.global.data_page_row_count_limit = v;
    }
    if let Some(v) = encoding {
        to.global.encoding = Some(v);
    }
    if let Some(v) = bloom_filter_on_write {
        to.global.bloom_filter_on_write = v;
    }
    if let Some(v) = bloom_filter_fpp {
        to.global.bloom_filter_fpp = Some(v);
    }
    if let Some(v) = bloom_filter_ndv {
        to.global.bloom_filter_ndv = Some(v);
    }
    if let Some(v) = allow_single_file_parallelism {
        to.global.allow_single_file_parallelism = v;
    }
    if let Some(v) = maximum_parallel_row_group_writers {
        to.global.maximum_parallel_row_group_writers = v;
    }
    if let Some(v) = maximum_buffered_record_batches_per_stream {
        to.global.maximum_buffered_record_batches_per_stream = v;
    }
    Ok(())
}

fn apply_delta_read_options(from: DeltaReadOptions, to: &mut TableDeltaOptions) -> Result<()> {
    let _ = (from, to);
    Ok(())
}

fn apply_delta_write_options(from: DeltaWriteOptions, to: &mut TableDeltaOptions) -> Result<()> {
    if let Some(replace_where) = from.replace_where {
        to.replace_where = Some(replace_where);
    }
    if let Some(merge_schema) = from.merge_schema {
        to.merge_schema = merge_schema;
    }
    if let Some(overwrite_schema) = from.overwrite_schema {
        to.overwrite_schema = overwrite_schema;
    }
    if let Some(target_file_size) = from.target_file_size {
        to.target_file_size = target_file_size;
    }
    if let Some(write_batch_size) = from.write_batch_size {
        to.write_batch_size = write_batch_size;
    }
    Ok(())
}

pub struct DataSourceOptionsResolver<'a> {
    ctx: &'a dyn Session,
}

impl<'a> DataSourceOptionsResolver<'a> {
    pub fn new(ctx: &'a dyn Session) -> Self {
        Self { ctx }
    }

    pub fn resolve_json_read_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<JsonOptions> {
        let mut json_options = self.ctx.default_table_options().json;
        apply_json_read_options(load_default_options()?, &mut json_options)?;
        for opt in options {
            apply_json_read_options(load_options(opt)?, &mut json_options)?;
        }
        Ok(json_options)
    }

    pub fn resolve_json_write_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<JsonOptions> {
        let mut json_options = self.ctx.default_table_options().json;
        apply_json_write_options(load_default_options()?, &mut json_options)?;
        for opt in options {
            apply_json_write_options(load_options(opt)?, &mut json_options)?;
        }
        Ok(json_options)
    }

    pub fn resolve_csv_read_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<CsvOptions> {
        let mut csv_options = self.ctx.default_table_options().csv;
        apply_csv_read_options(load_default_options()?, &mut csv_options)?;
        for opt in options {
            apply_csv_read_options(load_options(opt)?, &mut csv_options)?;
        }
        Ok(csv_options)
    }

    pub fn resolve_csv_write_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<CsvOptions> {
        let mut csv_options = self.ctx.default_table_options().csv;
        apply_csv_write_options(load_default_options()?, &mut csv_options)?;
        for opt in options {
            apply_csv_write_options(load_options(opt)?, &mut csv_options)?;
        }
        Ok(csv_options)
    }

    pub fn resolve_parquet_read_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<TableParquetOptions> {
        let mut parquet_options = self.ctx.default_table_options().parquet;
        apply_parquet_read_options(load_default_options()?, &mut parquet_options)?;
        for opt in options {
            apply_parquet_read_options(load_options(opt)?, &mut parquet_options)?;
        }
        Ok(parquet_options)
    }

    pub fn resolve_parquet_write_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<TableParquetOptions> {
        let mut parquet_options = self.ctx.default_table_options().parquet;
        apply_parquet_write_options(load_default_options()?, &mut parquet_options)?;
        for opt in options {
            apply_parquet_write_options(load_options(opt)?, &mut parquet_options)?;
        }
        // When the FPP or NDV is set, the parquet writer will enable bloom filter implicitly.
        // This is not desired since we want those values to take effect only when the bloom filter
        // is explicitly enabled on write.
        // So here we set FPP and NDV to `None` if the bloom filter is not enabled on write.
        if !parquet_options.global.bloom_filter_on_write {
            parquet_options.global.bloom_filter_fpp = None;
            parquet_options.global.bloom_filter_ndv = None;
        }
        Ok(parquet_options)
    }

    pub fn resolve_delta_read_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<TableDeltaOptions> {
        let mut delta_options = TableDeltaOptions::default();
        apply_delta_read_options(load_default_options()?, &mut delta_options)?;
        for opt in options {
            apply_delta_read_options(load_options(opt)?, &mut delta_options)?;
        }
        Ok(delta_options)
    }

    pub fn resolve_delta_write_options(
        &self,
        options: Vec<HashMap<String, String>>,
    ) -> Result<TableDeltaOptions> {
        let mut delta_options = TableDeltaOptions::default();
        apply_delta_write_options(load_default_options()?, &mut delta_options)?;
        for opt in options {
            apply_delta_write_options(load_options(opt)?, &mut delta_options)?;
        }
        Ok(delta_options)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use super::*;

    fn build_options(options: &[(&str, &str)]) -> HashMap<String, String> {
        options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_resolve_json_read_options() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let kv = build_options(&[
            ("schema_infer_max_records", "100"),
            ("compression", "bzip2"),
        ]);
        let options = resolver.resolve_json_read_options(vec![kv])?;
        assert_eq!(options.schema_infer_max_rec, Some(100));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_json_write_options() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let kv = build_options(&[("compression", "bzip2")]);
        let options = resolver.resolve_json_write_options(vec![kv])?;
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_csv_read_options() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let kv = build_options(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("comment", "^"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("line_sep", "@"),
            ("schema_infer_max_records", "100"),
            ("multi_line", "true"),
            ("compression", "bzip2"),
        ]);
        let options = resolver.resolve_csv_read_options(vec![kv])?;
        assert_eq!(options.delimiter, b'!');
        assert_eq!(options.quote, b'(');
        assert_eq!(options.escape, Some(b'*'));
        assert_eq!(options.comment, Some(b'^'));
        assert_eq!(options.has_header, Some(true));
        assert_eq!(options.null_value, None); // This is for the writer
        assert_eq!(options.null_regex, Some("MEOW".to_string())); // null_value
        assert_eq!(options.terminator, Some(b'@')); // line_sep
        assert_eq!(options.schema_infer_max_rec, Some(100));
        assert_eq!(options.newlines_in_values, Some(true)); // multi_line
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        let kv = build_options(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("comment", "^"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("null_regex", "MEOW"),
            ("line_sep", "@"),
            ("schema_infer_max_records", "100"),
            ("multi_line", "true"),
            ("compression", "bzip2"),
        ]);
        // null_value and null_regex cannot both be set
        let result = resolver.resolve_csv_read_options(vec![kv]);
        assert!(result.is_err());

        let kv = build_options(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("comment", "^"),
            ("header", "true"),
            ("null_regex", "MEOW"),
            ("line_sep", "@"),
            ("schema_infer_max_records", "100"),
            ("multi_line", "true"),
            ("compression", "bzip2"),
        ]);
        let options = resolver.resolve_csv_read_options(vec![kv])?;
        assert_eq!(options.null_value, None); // This is for the writer
        assert_eq!(options.null_regex, Some("MEOW".to_string())); // null_regex

        Ok(())
    }

    #[test]
    fn test_resolve_csv_write_options() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let kv = build_options(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("escape_quotes", "true"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("compression", "bzip2"),
        ]);
        let options = resolver.resolve_csv_write_options(vec![kv])?;
        assert_eq!(options.delimiter, b'!');
        assert_eq!(options.quote, b'(');
        assert_eq!(options.escape, Some(b'*'));
        assert_eq!(options.double_quote, Some(true)); // escape_quotes
        assert_eq!(options.has_header, Some(true));
        assert_eq!(options.null_value, Some("MEOW".to_string()));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_read_options() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let mut kv = build_options(&[
            ("enable_page_index", "true"),
            ("pruning", "true"),
            ("skip_metadata", "false"),
            ("metadata_size_hint", "1024"),
            ("pushdown_filters", "true"),
            ("reorder_filters", "false"),
            ("schema_force_view_types", "true"),
            ("binary_as_string", "true"),
            ("coerce_int96", "ms"),
            ("bloom_filter_on_read", "true"),
        ]);
        let options = resolver.resolve_parquet_read_options(vec![kv.clone()])?;
        assert!(options.global.enable_page_index);
        assert!(options.global.pruning);
        assert!(!options.global.skip_metadata);
        assert_eq!(options.global.metadata_size_hint, Some(1024));
        assert!(options.global.pushdown_filters);
        assert!(!options.global.reorder_filters);
        assert!(options.global.schema_force_view_types);
        assert!(options.global.binary_as_string);
        assert_eq!(options.global.coerce_int96, Some("ms".to_string()));
        assert!(options.global.bloom_filter_on_read);

        kv.insert("metadata_size_hint".to_string(), "0".to_string());
        let options = resolver.resolve_parquet_read_options(vec![kv.clone()])?;
        assert_eq!(options.global.metadata_size_hint, None);

        kv.insert("metadata_size_hint".to_string(), "".to_string());
        let options = resolver.resolve_parquet_read_options(vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, None);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_read_options_with_global_default() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .metadata_size_hint = Some(123);
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let kv = build_options(&[]);
        let options = resolver.resolve_parquet_read_options(vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        let kv = build_options(&[("metadata_size_hint", "0")]);
        let options = resolver.resolve_parquet_read_options(vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        let kv = build_options(&[("metadata_size_hint", "")]);
        let options = resolver.resolve_parquet_read_options(vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let mut kv = build_options(&[
            ("data_page_size_limit", "1024"),
            ("write_batch_size", "1000"),
            ("writer_version", "2.0"),
            ("skip_arrow_metadata", "true"),
            ("compression", "snappy"),
            ("dictionary_enabled", "true"),
            ("dictionary_page_size_limit", "2048"),
            ("statistics_enabled", "chunk"),
            ("max_row_group_size", "5000"),
            ("column_index_truncate_length", "100"),
            ("statistics_truncate_length", "200"),
            ("data_page_row_count_limit", "10000"),
            ("encoding", "delta_binary_packed"),
            ("bloom_filter_on_write", "true"),
            ("bloom_filter_fpp", "0.01"),
            ("bloom_filter_ndv", "1000"),
            ("allow_single_file_parallelism", "false"),
            ("maximum_parallel_row_group_writers", "4"),
            ("maximum_buffered_record_batches_per_stream", "10"),
        ]);
        let options = resolver.resolve_parquet_write_options(vec![kv.clone()])?;
        assert_eq!(options.global.data_pagesize_limit, 1024);
        assert_eq!(options.global.write_batch_size, 1000);
        assert_eq!(options.global.writer_version, "2.0");
        assert!(options.global.skip_arrow_metadata);
        assert_eq!(options.global.compression, Some("snappy".to_string()));
        assert_eq!(options.global.dictionary_enabled, Some(true));
        assert_eq!(options.global.dictionary_page_size_limit, 2048);
        assert_eq!(options.global.statistics_enabled, Some("chunk".to_string()));
        assert_eq!(options.global.max_row_group_size, 5000);
        assert_eq!(options.global.column_index_truncate_length, Some(100));
        assert_eq!(options.global.statistics_truncate_length, Some(200));
        assert_eq!(options.global.data_page_row_count_limit, 10000);
        assert_eq!(
            options.global.encoding,
            Some("delta_binary_packed".to_string())
        );
        assert!(options.global.bloom_filter_on_write);
        assert_eq!(options.global.bloom_filter_fpp, Some(0.01));
        assert_eq!(options.global.bloom_filter_ndv, Some(1000));
        assert!(!options.global.allow_single_file_parallelism);
        assert_eq!(options.global.maximum_parallel_row_group_writers, 4);
        assert_eq!(
            options.global.maximum_buffered_record_batches_per_stream,
            10
        );

        kv.insert("column_index_truncate_length".to_string(), "0".to_string());
        kv.insert("statistics_truncate_length".to_string(), "0".to_string());
        kv.insert("encoding".to_string(), "".to_string());
        let options = resolver.resolve_parquet_write_options(vec![kv.clone()])?;
        assert_eq!(options.global.column_index_truncate_length, Some(64));
        assert_eq!(options.global.statistics_truncate_length, None);
        assert_eq!(options.global.encoding, None,);

        kv.insert("column_index_truncate_length".to_string(), "".to_string());
        kv.insert("statistics_truncate_length".to_string(), "".to_string());
        let options = resolver.resolve_parquet_write_options(vec![kv])?;
        assert_eq!(options.global.column_index_truncate_length, Some(64));
        assert_eq!(options.global.statistics_truncate_length, None);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options_with_global_default() -> Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .max_row_group_size = 1234;
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .column_index_truncate_length = Some(32);
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .statistics_truncate_length = Some(99);
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .encoding = Some("bit_packed".to_string());
        let state = ctx.state();
        let resolver = DataSourceOptionsResolver::new(&state);

        let kv = build_options(&[]);
        let options = resolver.resolve_parquet_write_options(vec![kv])?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some("bit_packed".to_string()));

        let kv = build_options(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
            ("encoding", ""),
        ]);
        let options = resolver.resolve_parquet_write_options(vec![kv])?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some("bit_packed".to_string()));

        let kv = build_options(&[
            ("column_index_truncate_length", ""),
            ("statistics_truncate_length", ""),
        ]);
        let options = resolver.resolve_parquet_write_options(vec![kv])?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some("bit_packed".to_string()));

        Ok(())
    }
}
