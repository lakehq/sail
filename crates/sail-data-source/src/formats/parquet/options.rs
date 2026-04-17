use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion::parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use datafusion_common::config::{ParquetOptions, TableParquetOptions};
use datafusion_common::parquet_config::DFParquetWriterVersion;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::{DataSourceError, DataSourceResult};
use crate::options::gen::{
    ParquetReadOptions, ParquetReadPartialOptions, ParquetWriteOptions, ParquetWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};
use crate::utils::split_parquet_compression_string;

fn check_parquet_level_is_none(codec: &str, level: &Option<u32>) -> DataSourceResult<()> {
    if level.is_some() {
        return Err(DataSourceError::InvalidOption {
            key: "compression".to_string(),
            value: codec.to_string(),
            cause: Some("does not support specifying a level".to_string()),
        });
    }
    Ok(())
}

impl BuildPartialOptions<ParquetReadPartialOptions> for TableParquetOptions {
    fn build_partial_options(self) -> DataSourceResult<ParquetReadPartialOptions> {
        Ok(ParquetReadPartialOptions {
            enable_page_index: Some(self.global.enable_page_index),
            pruning: Some(self.global.pruning),
            skip_metadata: Some(self.global.skip_metadata),
            // Always provide the session value so the user can override it to None via "0" or ""
            metadata_size_hint: Some(self.global.metadata_size_hint),
            pushdown_filters: Some(self.global.pushdown_filters),
            reorder_filters: Some(self.global.reorder_filters),
            schema_force_view_types: Some(self.global.schema_force_view_types),
            binary_as_string: Some(self.global.binary_as_string),
            coerce_int96: self.global.coerce_int96.map(Some),
            bloom_filter_on_read: Some(self.global.bloom_filter_on_read),
            max_predicate_cache_size: Some(self.global.max_predicate_cache_size),
        })
    }
}

impl ParquetReadOptions {
    pub fn into_table_options(self) -> TableParquetOptions {
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
        } = self;
        let global = ParquetOptions {
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
            ..ParquetOptions::default()
        };
        TableParquetOptions {
            global,
            ..TableParquetOptions::default()
        }
    }
}

impl BuildPartialOptions<ParquetWritePartialOptions> for TableParquetOptions {
    fn build_partial_options(self) -> DataSourceResult<ParquetWritePartialOptions> {
        Ok(ParquetWritePartialOptions {
            data_page_size_limit: Some(self.global.data_pagesize_limit),
            write_batch_size: Some(self.global.write_batch_size),
            writer_version: Some(self.global.writer_version.to_string()),
            skip_arrow_metadata: Some(self.global.skip_arrow_metadata),
            compression: Some(self.global.compression.clone()),
            dictionary_enabled: Some(self.global.dictionary_enabled),
            dictionary_page_size_limit: Some(self.global.dictionary_page_size_limit),
            statistics_enabled: Some(self.global.statistics_enabled.clone()),
            max_row_group_size: Some(self.global.max_row_group_size),
            column_index_truncate_length: Some(self.global.column_index_truncate_length),
            statistics_truncate_length: Some(self.global.statistics_truncate_length),
            data_page_row_count_limit: Some(self.global.data_page_row_count_limit),
            encoding: Some(self.global.encoding.clone()),
            bloom_filter_on_write: Some(self.global.bloom_filter_on_write),
            bloom_filter_fpp: Some(self.global.bloom_filter_fpp),
            bloom_filter_ndv: Some(self.global.bloom_filter_ndv),
            allow_single_file_parallelism: Some(self.global.allow_single_file_parallelism),
            maximum_parallel_row_group_writers: Some(
                self.global.maximum_parallel_row_group_writers,
            ),
            maximum_buffered_record_batches_per_stream: Some(
                self.global.maximum_buffered_record_batches_per_stream,
            ),
        })
    }
}

impl ParquetWriteOptions {
    pub fn into_table_options(self) -> DataSourceResult<TableParquetOptions> {
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
        } = self;
        let writer_version =
            DFParquetWriterVersion::from_str(writer_version.as_str()).map_err(|e| {
                DataSourceError::InvalidOption {
                    key: "writer_version".to_string(),
                    value: writer_version.to_string(),
                    cause: Some(e.to_string()),
                }
            })?;
        let compression = if let Some(v) = compression {
            let (codec, level) =
                split_parquet_compression_string(&v.to_lowercase()).map_err(|e| {
                    DataSourceError::InvalidOption {
                        key: "compression".to_string(),
                        value: v.to_string(),
                        cause: Some(e.to_string()),
                    }
                })?;
            let resolved = match codec.as_str() {
                "uncompressed" | "none" | "" => {
                    check_parquet_level_is_none(codec.as_str(), &level)?;
                    Ok::<String, DataSourceError>("uncompressed".to_string())
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
                _ => Err(DataSourceError::InvalidOption {
                    key: "compression".to_string(),
                    value: v.to_string(),
                    cause: Some(
                        "unknown or unsupported parquet compression. Valid values are: \
                        uncompressed, snappy, gzip(level), lzo, brotli(level), lz4, zstd(level), \
                        and lz4_raw."
                            .to_string(),
                    ),
                }),
            }?;
            Some(resolved)
        } else {
            None
        };
        // When the FPP or NDV is set, the parquet writer will enable bloom filter implicitly.
        // This is not desired since we want those values to take effect only when the bloom filter
        // is explicitly enabled on write.
        // So here we set FPP and NDV to `None` if the bloom filter is not enabled on write.
        let (bloom_filter_fpp, bloom_filter_ndv) = if bloom_filter_on_write {
            (bloom_filter_fpp, bloom_filter_ndv)
        } else {
            (None, None)
        };
        let global = ParquetOptions {
            data_pagesize_limit: data_page_size_limit,
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
            ..ParquetOptions::default()
        };
        Ok(TableParquetOptions {
            global,
            ..TableParquetOptions::default()
        })
    }
}

pub fn resolve_parquet_read_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> DataSourceResult<ParquetReadOptions> {
    let mut partial = ParquetReadPartialOptions::initialize();
    partial.merge(
        ctx.default_table_options()
            .parquet
            .build_partial_options()?,
    );
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

pub fn resolve_parquet_write_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> DataSourceResult<ParquetWriteOptions> {
    let mut partial = ParquetWritePartialOptions::initialize();
    partial.merge(
        ctx.default_table_options()
            .parquet
            .build_partial_options()?,
    );
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use datafusion::prelude::SessionContext;
    use datafusion_common::parquet_config::DFParquetWriterVersion;

    use crate::formats::parquet::options::{
        resolve_parquet_read_options, resolve_parquet_write_options,
    };
    use crate::options::option_list;

    #[test]
    fn test_resolve_parquet_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = option_list(&[
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
        let options = resolve_parquet_read_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
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
        // max_predicate_cache_size uses parse_optional_usize: "0" = Some(0)
        assert_eq!(options.global.max_predicate_cache_size, Some(0));

        // metadata_size_hint = "0": parse_optional_non_zero_usize("0") returns None
        // which explicitly clears the value (overrides session default)
        let kv = option_list(&[("metadata_size_hint", "0")]);
        let options = resolve_parquet_read_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        assert_eq!(options.global.metadata_size_hint, None);

        // metadata_size_hint = "": parse_optional_non_zero_usize("") returns None
        let kv = option_list(&[("metadata_size_hint", "")]);
        let options = resolve_parquet_read_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        assert_eq!(options.global.metadata_size_hint, None);

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

        // When metadata_size_hint is not provided, the session value is used
        let kv = option_list(&[]);
        let options = resolve_parquet_read_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        assert_eq!(options.global.metadata_size_hint, Some(123));

        // When metadata_size_hint = "0" is provided, the value is explicitly cleared
        let kv = option_list(&[("metadata_size_hint", "0")]);
        let options = resolve_parquet_read_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        assert_eq!(options.global.metadata_size_hint, None);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = option_list(&[
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
        let options = resolve_parquet_write_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.data_pagesize_limit, 1024);
        assert_eq!(options.global.write_batch_size, 1000);
        assert_eq!(
            options.global.writer_version,
            DFParquetWriterVersion::from_str("2.0")?
        );
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

        let kv = option_list(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
            ("encoding", ""),
        ]);
        let options = resolve_parquet_write_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.column_index_truncate_length, None);
        assert_eq!(options.global.statistics_truncate_length, None);
        assert_eq!(options.global.encoding, None);

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

        let kv = option_list(&[]);
        let options = resolve_parquet_write_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some("bit_packed".to_string()));

        let kv = option_list(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
            ("encoding", ""),
        ]);
        let options = resolve_parquet_write_options(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.column_index_truncate_length, None);
        assert_eq!(options.global.statistics_truncate_length, None);
        assert_eq!(options.global.encoding, None);

        Ok(())
    }
}
