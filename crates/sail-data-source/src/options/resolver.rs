use std::collections::HashMap;
use std::str::FromStr;

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::prelude::SessionContext;
use datafusion_common::config::{CsvOptions, JsonOptions, TableParquetOptions};
use datafusion_common::{plan_err, Result};

use crate::options::{
    load_default_options, load_options, CsvReadOptions, CsvWriteOptions, JsonReadOptions,
    JsonWriteOptions, ParquetReadOptions, ParquetWriteOptions,
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

pub struct DataSourceOptionsResolver<'a> {
    ctx: &'a SessionContext,
}

impl<'a> DataSourceOptionsResolver<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self { ctx }
    }

    pub fn resolve_json_read_options(
        &self,
        options: HashMap<String, String>,
    ) -> Result<JsonOptions> {
        let mut json_options = self.ctx.copied_table_options().json;
        apply_json_read_options(load_default_options()?, &mut json_options)?;
        apply_json_read_options(load_options(options)?, &mut json_options)?;
        Ok(json_options)
    }

    pub fn resolve_json_write_options(
        &self,
        options: HashMap<String, String>,
    ) -> Result<JsonOptions> {
        let mut json_options = self.ctx.copied_table_options().json;
        apply_json_write_options(load_default_options()?, &mut json_options)?;
        apply_json_write_options(load_options(options)?, &mut json_options)?;
        Ok(json_options)
    }

    pub fn resolve_csv_read_options(&self, options: HashMap<String, String>) -> Result<CsvOptions> {
        let mut csv_options = self.ctx.copied_table_options().csv;
        apply_csv_read_options(load_default_options()?, &mut csv_options)?;
        apply_csv_read_options(load_options(options)?, &mut csv_options)?;
        Ok(csv_options)
    }

    pub fn resolve_csv_write_options(
        &self,
        options: HashMap<String, String>,
    ) -> Result<CsvOptions> {
        let mut csv_options = self.ctx.copied_table_options().csv;
        apply_csv_write_options(load_default_options()?, &mut csv_options)?;
        apply_csv_write_options(load_options(options)?, &mut csv_options)?;
        Ok(csv_options)
    }

    pub fn resolve_parquet_read_options(
        &self,
        options: HashMap<String, String>,
    ) -> Result<TableParquetOptions> {
        let mut parquet_options = self.ctx.copied_table_options().parquet;
        apply_parquet_read_options(load_default_options()?, &mut parquet_options)?;
        apply_parquet_read_options(load_options(options)?, &mut parquet_options)?;
        Ok(parquet_options)
    }

    pub fn resolve_parquet_write_options(
        &self,
        options: HashMap<String, String>,
    ) -> Result<TableParquetOptions> {
        let mut parquet_options = self.ctx.copied_table_options().parquet;
        apply_parquet_write_options(load_default_options()?, &mut parquet_options)?;
        apply_parquet_write_options(load_options(options)?, &mut parquet_options)?;
        Ok(parquet_options)
    }
}
