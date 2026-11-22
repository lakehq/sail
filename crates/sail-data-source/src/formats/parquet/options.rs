// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion::parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use datafusion_common::config::{
    MaxRowGroupBytes, ParquetCdcOptions, ParquetOptions, TableParquetOptions,
};
use datafusion_common::parquet_config::DFParquetWriterVersion;
use sail_common::utils::parquet::SAIL_PARQUET_CREATED_BY;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::parquet::ParquetWriteConfig;

use crate::error::{DataSourceError, DataSourceResult};
use crate::options::r#gen::{
    ParquetReadOptions, ParquetReadPartialOptions, ParquetWriteOptions, ParquetWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions, ResolveOptions};
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
            force_filter_selections: Some(self.global.force_filter_selections),
            schema_force_view_types: Some(self.global.schema_force_view_types),
            binary_as_string: Some(self.global.binary_as_string),
            coerce_int96: self.global.coerce_int96.map(Some),
            bloom_filter_on_read: Some(self.global.bloom_filter_on_read),
            max_predicate_cache_size: Some(self.global.max_predicate_cache_size),
            max_in_list_size: Some(self.global.max_in_list_size),
            path_glob_filter: None,
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
            force_filter_selections,
            schema_force_view_types,
            binary_as_string,
            coerce_int96,
            bloom_filter_on_read,
            max_predicate_cache_size,
            max_in_list_size,
            path_glob_filter: _,
        } = self;
        let global = ParquetOptions {
            enable_page_index,
            pruning,
            skip_metadata,
            metadata_size_hint,
            pushdown_filters,
            reorder_filters,
            force_filter_selections,
            schema_force_view_types,
            binary_as_string,
            // Spark reads INT96 as `TimestampType`, which stores UTC instants.
            coerce_int96_tz: coerce_int96.as_ref().map(|_| "UTC".to_string()),
            coerce_int96,
            bloom_filter_on_read,
            max_predicate_cache_size,
            max_in_list_size,
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
            max_row_group_bytes: Some(self.global.max_row_group_bytes.map(|v| v.get())),
            column_index_truncate_length: Some(self.global.column_index_truncate_length),
            statistics_truncate_length: Some(self.global.statistics_truncate_length),
            data_page_row_count_limit: Some(self.global.data_page_row_count_limit),
            bloom_filter_on_write: Some(self.global.bloom_filter_on_write),
            bloom_filter_fpp: Some(self.global.bloom_filter_fpp),
            bloom_filter_ndv: Some(self.global.bloom_filter_ndv),
            encoding_parallelism: None,
            max_open_files_per_task: None,
            content_defined_chunking_enabled: Some(self.global.content_defined_chunking.enabled),
            content_defined_chunking_min_chunk_size: Some(
                self.global.content_defined_chunking.min_chunk_size,
            ),
            content_defined_chunking_max_chunk_size: Some(
                self.global.content_defined_chunking.max_chunk_size,
            ),
            content_defined_chunking_norm_level: Some(
                self.global.content_defined_chunking.norm_level,
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
            max_row_group_bytes,
            column_index_truncate_length,
            statistics_truncate_length,
            data_page_row_count_limit,
            bloom_filter_on_write,
            bloom_filter_fpp,
            bloom_filter_ndv,
            encoding_parallelism: _,
            max_open_files_per_task: _,
            content_defined_chunking_enabled,
            content_defined_chunking_min_chunk_size,
            content_defined_chunking_max_chunk_size,
            content_defined_chunking_norm_level,
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
        // `parse_optional_non_zero_usize` maps `0` to `None`, so the value is never zero here.
        let max_row_group_bytes = max_row_group_bytes
            .map(MaxRowGroupBytes::try_new)
            .transpose()
            .map_err(|e| DataSourceError::InvalidOption {
                key: "max_row_group_bytes".to_string(),
                value: "0".to_string(),
                cause: Some(e.to_string()),
            })?;
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
            created_by: SAIL_PARQUET_CREATED_BY.to_string(),
            compression,
            dictionary_enabled,
            dictionary_page_size_limit,
            statistics_enabled,
            max_row_group_size,
            max_row_group_bytes,
            column_index_truncate_length,
            statistics_truncate_length,
            data_page_row_count_limit,
            bloom_filter_on_write,
            bloom_filter_fpp,
            bloom_filter_ndv,
            content_defined_chunking: ParquetCdcOptions {
                enabled: content_defined_chunking_enabled,
                min_chunk_size: content_defined_chunking_min_chunk_size,
                max_chunk_size: content_defined_chunking_max_chunk_size,
                norm_level: content_defined_chunking_norm_level,
            },
            ..ParquetOptions::default()
        };
        Ok(TableParquetOptions {
            global,
            ..TableParquetOptions::default()
        })
    }
}

impl ResolveOptions for ParquetReadOptions {
    fn resolve(ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
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
}

impl ResolveOptions for ParquetWriteOptions {
    fn resolve(ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = ParquetWritePartialOptions::initialize();
        partial.merge(
            ctx.default_table_options()
                .parquet
                .build_partial_options()?,
        );
        let config = ctx.extension::<ParquetWriteConfig>()?;
        partial.merge(ParquetWritePartialOptions {
            encoding_parallelism: Some(config.encoding_parallelism()),
            max_open_files_per_task: Some(config.max_open_files_per_task()),
            ..Default::default()
        });
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;
    use std::sync::Arc;

    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::config::MaxRowGroupBytes;
    use datafusion_common::parquet_config::DFParquetWriterVersion;
    use sail_common::utils::parquet::SAIL_PARQUET_CREATED_BY;
    use sail_common_datafusion::datasource::OptionLayer;
    use sail_common_datafusion::session::parquet::ParquetWriteConfig;

    use crate::options::r#gen::{ParquetReadOptions, ParquetWriteOptions};
    use crate::options::{ResolveOptions, option_list};

    /// A session that has the values of the application configuration
    /// for the Parquet write options.
    fn write_session(
        encoding_parallelism: usize,
        max_open_files_per_task: usize,
    ) -> datafusion_common::Result<SessionContext> {
        let max_open_files_per_task = NonZeroUsize::new(max_open_files_per_task)
            .ok_or_else(|| datafusion_common::internal_datafusion_err!("invalid test value"))?;
        let config = SessionConfig::new().with_extension(Arc::new(ParquetWriteConfig::new(
            encoding_parallelism,
            max_open_files_per_task,
        )));
        Ok(SessionContext::new_with_config(config))
    }

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
            ("force_filter_selections", "true"),
            ("max_in_list_size", "100"),
        ]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
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
        assert!(options.global.force_filter_selections);
        assert_eq!(options.global.max_in_list_size, 100);

        // metadata_size_hint = "0": parse_optional_non_zero_usize("0") returns None
        // which explicitly clears the value (overrides session default)
        let kv = option_list(&[("metadata_size_hint", "0")]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        assert_eq!(options.global.metadata_size_hint, None);

        // metadata_size_hint = "": parse_optional_non_zero_usize("") returns None
        let kv = option_list(&[("metadata_size_hint", "")]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
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
        let options = ParquetReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        assert_eq!(options.global.metadata_size_hint, Some(123));

        // When metadata_size_hint = "0" is provided, the value is explicitly cleared
        let kv = option_list(&[("metadata_size_hint", "0")]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        assert_eq!(options.global.metadata_size_hint, None);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options() -> datafusion_common::Result<()> {
        let ctx = write_session(0, 100)?;
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
            ("bloom_filter_on_write", "true"),
            ("bloom_filter_fpp", "0.01"),
            ("bloom_filter_ndv", "1000"),
            ("encoding_parallelism", "4"),
            ("max_open_files_per_task", "10"),
            ("content_defined_chunking_enabled", "true"),
            ("content_defined_chunking_min_chunk_size", "4096"),
            ("content_defined_chunking_max_chunk_size", "8192"),
            ("content_defined_chunking_norm_level", "-1"),
            ("max_row_group_bytes", "65536"),
        ]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.encoding_parallelism, 4);
        assert_eq!(options.max_open_files_per_task.get(), 10);
        let options = options
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
        assert!(options.global.bloom_filter_on_write);
        assert_eq!(options.global.bloom_filter_fpp, Some(0.01));
        assert_eq!(options.global.bloom_filter_ndv, Some(1000));
        assert!(options.global.content_defined_chunking.enabled);
        assert_eq!(options.global.content_defined_chunking.min_chunk_size, 4096);
        assert_eq!(options.global.content_defined_chunking.max_chunk_size, 8192);
        assert_eq!(options.global.content_defined_chunking.norm_level, -1);
        assert_eq!(options.global.created_by, SAIL_PARQUET_CREATED_BY);
        assert_eq!(
            options.global.max_row_group_bytes.map(|v| v.get()),
            Some(65536)
        );

        let kv = option_list(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
            ("max_row_group_bytes", "0"),
        ]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.column_index_truncate_length, None);
        assert_eq!(options.global.statistics_truncate_length, None);
        assert_eq!(options.global.max_row_group_bytes, None);

        // bloom_filter_ndv = "0" leaves the NDV unset so the Parquet writer derives it
        // from the maximum row group size
        let kv = option_list(&[("bloom_filter_on_write", "true"), ("bloom_filter_ndv", "0")]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert!(options.global.bloom_filter_on_write);
        assert_eq!(options.global.bloom_filter_ndv, None);

        Ok(())
    }

    #[test]
    fn test_parquet_writer_bloom_filter_ndv() -> datafusion_common::Result<()> {
        use datafusion::parquet::file::properties::WriterPropertiesBuilder;
        use datafusion::parquet::schema::types::ColumnPath;

        let ctx = write_session(0, 100)?;
        let state = ctx.state();
        let writer_ndv = |kv: &[(&str, &str)]| -> datafusion_common::Result<Option<u64>> {
            // The Arrow schema is only embedded by the file sink, which this test bypasses.
            let base = option_list(&[("skip_arrow_metadata", "true")]);
            let options = ParquetWriteOptions::resolve(&state, vec![base, option_list(kv)])
                .map_err(datafusion_common::DataFusionError::from)?
                .into_table_options()
                .map_err(datafusion_common::DataFusionError::from)?;
            let props = WriterPropertiesBuilder::try_from(&options)?.build();
            Ok(props
                .bloom_filter_properties(&ColumnPath::from("c"))
                .map(|p| p.ndv()))
        };

        // Without an explicit NDV, the writer sizes the bloom filter for the row group size.
        assert_eq!(
            writer_ndv(&[
                ("bloom_filter_on_write", "true"),
                ("bloom_filter_ndv", "0"),
                ("max_row_group_size", "10000"),
            ])?,
            Some(10000)
        );
        // An explicit NDV takes precedence over the row group size.
        assert_eq!(
            writer_ndv(&[
                ("bloom_filter_on_write", "true"),
                ("bloom_filter_ndv", "500"),
                ("max_row_group_size", "10000"),
            ])?,
            Some(500)
        );

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options_with_global_default() -> datafusion_common::Result<()> {
        let ctx = write_session(0, 100)?;
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
            .content_defined_chunking
            .enabled = true;
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .content_defined_chunking
            .min_chunk_size = 4096;
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .content_defined_chunking
            .max_chunk_size = 8192;
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .content_defined_chunking
            .norm_level = -2;
        state
            .write()
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .max_row_group_bytes = Some(MaxRowGroupBytes::try_new(4096)?);
        let state = ctx.state();

        let kv = option_list(&[]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert!(options.global.content_defined_chunking.enabled);
        assert_eq!(options.global.content_defined_chunking.min_chunk_size, 4096);
        assert_eq!(options.global.content_defined_chunking.max_chunk_size, 8192);
        assert_eq!(options.global.content_defined_chunking.norm_level, -2);
        assert_eq!(
            options.global.max_row_group_bytes.map(|v| v.get()),
            Some(4096)
        );

        let kv = option_list(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
        ]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.column_index_truncate_length, None);
        assert_eq!(options.global.statistics_truncate_length, None);

        Ok(())
    }

    #[test]
    fn test_resolve_parquet_write_options_of_writer_tasks() -> datafusion_common::Result<()> {
        let ctx = write_session(2, 50)?;
        let state = ctx.state();

        // The values of the session are used when the options are not set.
        let options = ParquetWriteOptions::resolve(&state, vec![option_list(&[])])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.encoding_parallelism, 2);
        assert_eq!(options.max_open_files_per_task.get(), 50);

        let kv = option_list(&[("encodingParallelism", "0"), ("maxOpenFilesPerTask", "1")]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.encoding_parallelism, 0);
        assert_eq!(options.max_open_files_per_task.get(), 1);

        // A task must be able to keep one file open.
        let kv = option_list(&[("max_open_files_per_task", "0")]);
        let result = ParquetWriteOptions::resolve(&state, vec![kv]);
        assert!(
            matches!(&result, Err(e) if e.to_string() == "invalid option: max_open_files_per_task: 0"),
            "{result:?}"
        );

        // The options are not table properties, so that a table does not decide
        // how the tasks that write to the table use their worker.
        let kv = OptionLayer::TablePropertyList {
            items: vec![
                ("option.encoding_parallelism".to_string(), "7".to_string()),
                (
                    "option.max_open_files_per_task".to_string(),
                    "7".to_string(),
                ),
            ],
        };
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.encoding_parallelism, 2);
        assert_eq!(options.max_open_files_per_task.get(), 50);

        // A removed option is ignored in the same way as an unknown option.
        let kv = option_list(&[
            ("allow_single_file_parallelism", "false"),
            ("maximum_parallel_row_group_writers", "4"),
            ("maximum_buffered_record_batches_per_stream", "10"),
            ("encoding", "plain"),
            ("no_such_option", "1"),
        ]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.encoding_parallelism, 2);
        let options = options
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.encoding, None);

        Ok(())
    }
}
