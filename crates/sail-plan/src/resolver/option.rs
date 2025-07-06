use std::collections::HashMap;
use std::str::FromStr;

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion_common::config::{CsvOptions, JsonOptions, TableParquetOptions};
use sail_data_source::options::{
    load_default_options, load_options, CsvReadOptions, CsvWriteOptions, JsonReadOptions,
    JsonWriteOptions, ParquetReadOptions, ParquetWriteOptions,
};

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

fn char_to_u8(c: char, option: &str) -> PlanResult<u8> {
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        Err(PlanError::invalid(format!(
            "invalid {option} character '{c}': must be an ASCII character"
        )))
    }
}

fn apply_json_read_options(from: JsonReadOptions, to: &mut JsonOptions) -> PlanResult<()> {
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

fn apply_json_write_options(from: JsonWriteOptions, to: &mut JsonOptions) -> PlanResult<()> {
    let JsonWriteOptions { compression } = from;
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    Ok(())
}

fn apply_csv_read_options(from: CsvReadOptions, to: &mut CsvOptions) -> PlanResult<()> {
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
            return Err(PlanError::invalid(
                "CSV `null_value` and `null_regex` cannot be both set",
            ));
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

fn apply_csv_write_options(from: CsvWriteOptions, to: &mut CsvOptions) -> PlanResult<()> {
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
) -> PlanResult<()> {
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
) -> PlanResult<()> {
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

impl PlanResolver<'_> {
    pub(crate) fn resolve_json_read_options(
        &self,
        options: HashMap<String, String>,
    ) -> PlanResult<JsonOptions> {
        let mut json_options = self.ctx.copied_table_options().json;
        apply_json_read_options(load_default_options()?, &mut json_options)?;
        apply_json_read_options(load_options(options)?, &mut json_options)?;
        Ok(json_options)
    }

    pub(crate) fn resolve_json_write_options(
        &self,
        options: HashMap<String, String>,
    ) -> PlanResult<(JsonOptions, Vec<(String, String)>)> {
        let mut json_options = self.ctx.copied_table_options().json;
        apply_json_write_options(load_default_options()?, &mut json_options)?;
        apply_json_write_options(load_options(options)?, &mut json_options)?;
        let json_options_vec: Vec<(String, String)> = vec![(
            "format.compression".to_string(),
            json_options.compression.to_string(),
        )];
        Ok((json_options, json_options_vec))
    }

    pub(crate) fn resolve_csv_read_options(
        &self,
        options: HashMap<String, String>,
    ) -> PlanResult<CsvOptions> {
        let mut csv_options = self.ctx.copied_table_options().csv;
        apply_csv_read_options(load_default_options()?, &mut csv_options)?;
        apply_csv_read_options(load_options(options)?, &mut csv_options)?;
        Ok(csv_options)
    }

    pub(crate) fn resolve_csv_write_options(
        &self,
        options: HashMap<String, String>,
    ) -> PlanResult<(CsvOptions, Vec<(String, String)>)> {
        let mut csv_options = self.ctx.copied_table_options().csv;
        apply_csv_write_options(load_default_options()?, &mut csv_options)?;
        apply_csv_write_options(load_options(options)?, &mut csv_options)?;

        let mut csv_options_vec: Vec<(String, String)> = vec![];
        csv_options_vec.push((
            "format.delimiter".to_string(),
            (csv_options.delimiter as char).to_string(),
        ));
        csv_options_vec.push((
            "format.quote".to_string(),
            (csv_options.quote as char).to_string(),
        ));
        if let Some(escape) = &csv_options.escape {
            csv_options_vec.push(("format.escape".to_string(), (*escape as char).to_string()));
        }
        if let Some(double_quote) = &csv_options.double_quote {
            csv_options_vec.push(("format.double_quote".to_string(), double_quote.to_string()));
        }
        if let Some(has_header) = &csv_options.has_header {
            csv_options_vec.push(("format.has_header".to_string(), has_header.to_string()));
        }
        if let Some(null_value) = &csv_options.null_value {
            csv_options_vec.push(("format.null_value".to_string(), null_value.to_string()));
        }
        csv_options_vec.push((
            "format.compression".to_string(),
            csv_options.compression.to_string(),
        ));

        Ok((csv_options, csv_options_vec))
    }

    pub(crate) fn resolve_parquet_read_options(
        &self,
        options: HashMap<String, String>,
    ) -> PlanResult<TableParquetOptions> {
        let mut parquet_options = self.ctx.copied_table_options().parquet;
        apply_parquet_read_options(load_default_options()?, &mut parquet_options)?;
        apply_parquet_read_options(load_options(options)?, &mut parquet_options)?;
        Ok(parquet_options)
    }

    pub(crate) fn resolve_parquet_write_options(
        &self,
        options: HashMap<String, String>,
    ) -> PlanResult<(TableParquetOptions, Vec<(String, String)>)> {
        let mut parquet_options = self.ctx.copied_table_options().parquet;
        apply_parquet_write_options(load_default_options()?, &mut parquet_options)?;
        apply_parquet_write_options(load_options(options)?, &mut parquet_options)?;

        let mut parquet_options_map: HashMap<String, String> = HashMap::new();
        parquet_options_map.insert(
            "format.data_pagesize_limit".to_owned(),
            parquet_options.global.data_pagesize_limit.to_string(),
        );
        parquet_options_map.insert(
            "format.write_batch_size".to_owned(),
            parquet_options.global.write_batch_size.to_string(),
        );
        parquet_options_map.insert(
            "format.writer_version".to_owned(),
            parquet_options.global.writer_version.to_string(),
        );
        parquet_options_map.insert(
            "format.skip_arrow_metadata".to_owned(),
            parquet_options.global.skip_arrow_metadata.to_string(),
        );
        if let Some(compression) = &parquet_options.global.compression {
            parquet_options_map.insert("format.compression".to_owned(), compression.to_string());
        }
        if let Some(dictionary_enabled) = &parquet_options.global.dictionary_enabled {
            parquet_options_map.insert(
                "format.dictionary_enabled".to_owned(),
                dictionary_enabled.to_string(),
            );
        }
        parquet_options_map.insert(
            "format.dictionary_page_size_limit".to_owned(),
            parquet_options
                .global
                .dictionary_page_size_limit
                .to_string(),
        );
        if let Some(statistics_enabled) = &parquet_options.global.statistics_enabled {
            parquet_options_map.insert(
                "format.statistics_enabled".to_owned(),
                statistics_enabled.to_string(),
            );
        }
        parquet_options_map.insert(
            "format.max_row_group_size".to_owned(),
            parquet_options.global.max_row_group_size.to_string(),
        );
        if let Some(column_index_truncate_length) =
            &parquet_options.global.column_index_truncate_length
        {
            parquet_options_map.insert(
                "format.column_index_truncate_length".to_owned(),
                column_index_truncate_length.to_string(),
            );
        }
        if let Some(statistics_truncate_length) = &parquet_options.global.statistics_truncate_length
        {
            parquet_options_map.insert(
                "format.statistics_truncate_length".to_owned(),
                statistics_truncate_length.to_string(),
            );
        }
        parquet_options_map.insert(
            "format.data_page_row_count_limit".to_owned(),
            parquet_options.global.data_page_row_count_limit.to_string(),
        );
        if let Some(encoding) = &parquet_options.global.encoding {
            parquet_options_map.insert("format.encoding".to_owned(), encoding.to_string());
        }
        parquet_options_map.insert(
            "format.bloom_filter_on_write".to_owned(),
            parquet_options.global.bloom_filter_on_write.to_string(),
        );
        if let Some(bloom_filter_fpp) = &parquet_options.global.bloom_filter_fpp {
            parquet_options_map.insert(
                "format.bloom_filter_fpp".to_owned(),
                bloom_filter_fpp.to_string(),
            );
        }
        if let Some(bloom_filter_ndv) = &parquet_options.global.bloom_filter_ndv {
            parquet_options_map.insert(
                "format.bloom_filter_ndv".to_owned(),
                bloom_filter_ndv.to_string(),
            );
        }
        parquet_options_map.insert(
            "format.allow_single_file_parallelism".to_owned(),
            parquet_options
                .global
                .allow_single_file_parallelism
                .to_string(),
        );
        parquet_options_map.insert(
            "format.maximum_parallel_row_group_writers".to_owned(),
            parquet_options
                .global
                .maximum_parallel_row_group_writers
                .to_string(),
        );
        parquet_options_map.insert(
            "format.maximum_buffered_record_batches_per_stream".to_owned(),
            parquet_options
                .global
                .maximum_buffered_record_batches_per_stream
                .to_string(),
        );

        Ok((parquet_options, parquet_options_map.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use super::*;
    use crate::config::PlanConfig;

    fn build_options(options: &[(&str, &str)]) -> HashMap<String, String> {
        options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_resolve_json_read_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = build_options(&[
            ("schema_infer_max_records", "100"),
            ("compression", "bzip2"),
        ]);
        let options = resolver.resolve_json_read_options(options)?;
        assert_eq!(options.schema_infer_max_rec, Some(100));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_json_write_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = build_options(&[("compression", "bzip2")]);
        let (options, _json_options_vec) = resolver.resolve_json_write_options(options)?;
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_csv_read_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = build_options(&[
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
        let options = resolver.resolve_csv_read_options(options)?;
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

        let options = build_options(&[
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
        let result = resolver.resolve_csv_read_options(options);
        assert!(result.is_err());

        let options = build_options(&[
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
        let options = resolver.resolve_csv_read_options(options)?;
        assert_eq!(options.null_value, None); // This is for the writer
        assert_eq!(options.null_regex, Some("MEOW".to_string())); // null_regex

        Ok(())
    }

    #[test]
    fn test_resolve_csv_write_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = build_options(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("escape_quotes", "true"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("compression", "bzip2"),
        ]);
        let (options, _csv_options_vec) = resolver.resolve_csv_write_options(options)?;
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
    fn test_resolve_parquet_read_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = build_options(&[
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
        let options = resolver.resolve_parquet_read_options(options)?;
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

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_read_options_with_global_default() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let state = ctx.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .metadata_size_hint = Some(123);
        let options = build_options(&[]);
        let options = resolver.resolve_parquet_read_options(options)?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let options = build_options(&[
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
        let (options, _parquet_options_vec) = resolver.resolve_parquet_write_options(options)?;
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

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options_with_global_default() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

        let state = ctx.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .max_row_group_size = 1234;
        let options = build_options(&[]);
        let options = resolver.resolve_parquet_read_options(options)?;
        assert_eq!(options.global.max_row_group_size, 1234);

        Ok(())
    }
}
