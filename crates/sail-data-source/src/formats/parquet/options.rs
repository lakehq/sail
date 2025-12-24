use std::collections::HashMap;

use datafusion::catalog::Session;
use datafusion::parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{config_err, DataFusionError, Result};

use crate::options::{load_default_options, load_options, ParquetReadOptions, ParquetWriteOptions};
use crate::utils::split_parquet_compression_string;

fn check_parquet_level_is_none(codec: &str, level: &Option<u32>) -> Result<()> {
    if level.is_some() {
        return config_err!("Compression {codec} does not support specifying a level");
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
        max_predicate_cache_size,
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
    if let Some(v) = max_predicate_cache_size {
        to.global.max_predicate_cache_size = Some(v);
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
        let (codec, level) = split_parquet_compression_string(&v.to_lowercase())?;
        let compression = match codec.as_str() {
            "uncompressed" | "none" | "" => {
                check_parquet_level_is_none(codec.as_str(), &level)?;
                Ok("uncompressed".to_string())
            }
            "snappy" => {
                check_parquet_level_is_none(codec.as_str(), &level)?;
                Ok(v)
            }
            "gzip" => {
                if level.is_some() {
                    Ok(v)
                } else {
                    Ok(format!(
                        "gzip({})",
                        GzipLevel::default().compression_level()
                    ))
                }
            }
            "lzo" => {
                check_parquet_level_is_none(codec.as_str(), &level)?;
                Ok(v)
            }
            "brotli" => {
                if level.is_some() {
                    Ok(v)
                } else {
                    Ok(format!(
                        "brotli({})",
                        BrotliLevel::default().compression_level()
                    ))
                }
            }
            "lz4" => {
                check_parquet_level_is_none(codec.as_str(), &level)?;
                Ok(v)
            }
            "zstd" => {
                if level.is_some() {
                    Ok(v)
                } else {
                    Ok(format!(
                        "zstd({})",
                        ZstdLevel::default().compression_level()
                    ))
                }
            }
            "lz4_raw" => {
                check_parquet_level_is_none(codec.as_str(), &level)?;
                Ok(v)
            }
            _ => Err(DataFusionError::Configuration(format!(
                "Unknown or unsupported parquet compression: \
        {v}. Valid values are: uncompressed, snappy, gzip(level), \
        lzo, brotli(level), lz4, zstd(level), and lz4_raw."
            ))),
        }?;
        to.global.compression = Some(compression);
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

pub fn resolve_parquet_read_options(
    ctx: &dyn Session,
    options: Vec<HashMap<String, String>>,
) -> Result<TableParquetOptions> {
    let mut parquet_options = ctx.default_table_options().parquet;
    apply_parquet_read_options(load_default_options()?, &mut parquet_options)?;
    for opt in options {
        apply_parquet_read_options(load_options(opt)?, &mut parquet_options)?;
    }
    Ok(parquet_options)
}

pub fn resolve_parquet_write_options(
    ctx: &dyn Session,
    options: Vec<HashMap<String, String>>,
) -> Result<TableParquetOptions> {
    let mut parquet_options = ctx.default_table_options().parquet;
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

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use crate::formats::parquet::options::{
        resolve_parquet_read_options, resolve_parquet_write_options,
    };
    use crate::options::build_options;

    #[test]
    fn test_resolve_parquet_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let default_hint = state
            .default_table_options()
            .parquet
            .global
            .metadata_size_hint;

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
            ("max_predicate_cache_size", "0"),
        ]);
        let options = resolve_parquet_read_options(&state, vec![kv.clone()])?;
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
        assert_eq!(options.global.max_predicate_cache_size, Some(0));

        kv.insert("metadata_size_hint".to_string(), "0".to_string());
        let options = resolve_parquet_read_options(&state, vec![kv.clone()])?;
        assert_eq!(options.global.metadata_size_hint, default_hint);

        kv.insert("metadata_size_hint".to_string(), "".to_string());
        let options = resolve_parquet_read_options(&state, vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, default_hint);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_read_options_with_global_default() -> datafusion_common::Result<()> {
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

        let kv = build_options(&[]);
        let options = resolve_parquet_read_options(&state, vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        let kv = build_options(&[("metadata_size_hint", "0")]);
        let options = resolve_parquet_read_options(&state, vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        let kv = build_options(&[("metadata_size_hint", "")]);
        let options = resolve_parquet_read_options(&state, vec![kv])?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

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
        let options = resolve_parquet_write_options(&state, vec![kv.clone()])?;
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
        let options = resolve_parquet_write_options(&state, vec![kv.clone()])?;
        assert_eq!(options.global.column_index_truncate_length, Some(64));
        assert_eq!(options.global.statistics_truncate_length, Some(64));
        assert_eq!(options.global.encoding, None,);

        kv.insert("column_index_truncate_length".to_string(), "".to_string());
        kv.insert("statistics_truncate_length".to_string(), "".to_string());
        let options = resolve_parquet_write_options(&state, vec![kv])?;
        assert_eq!(options.global.column_index_truncate_length, Some(64));
        assert_eq!(options.global.statistics_truncate_length, Some(64));

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options_with_global_default() -> datafusion_common::Result<()> {
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

        let kv = build_options(&[]);
        let options = resolve_parquet_write_options(&state, vec![kv])?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some("bit_packed".to_string()));

        let kv = build_options(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
            ("encoding", ""),
        ]);
        let options = resolve_parquet_write_options(&state, vec![kv])?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some("bit_packed".to_string()));

        let kv = build_options(&[
            ("column_index_truncate_length", ""),
            ("statistics_truncate_length", ""),
        ]);
        let options = resolve_parquet_write_options(&state, vec![kv])?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some("bit_packed".to_string()));

        Ok(())
    }
}
