use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion::parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use datafusion_common::config::{
    ConfigTimeZone, ParquetCdcOptions, ParquetOptions, TableParquetOptions,
};
use datafusion_common::parquet_config::{
    DFParquetCompression, DFParquetEncoding, DFParquetStatistics, DFParquetWriterVersion,
    DFTimeUnit,
};
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::{DataSourceError, DataSourceResult};
use crate::options::r#gen::{
    ParquetReadOptions, ParquetReadPartialOptions, ParquetWriteOptions, ParquetWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions, ResolveOptions};
use crate::utils::split_parquet_compression_string;

fn invalid_option(key: &str, value: &str, cause: impl ToString) -> DataSourceError {
    DataSourceError::InvalidOption {
        key: key.to_string(),
        value: value.to_string(),
        cause: Some(cause.to_string()),
    }
}

fn parse_option<T: FromStr>(key: &str, value: &str) -> DataSourceResult<T>
where
    T::Err: ToString,
{
    T::from_str(value).map_err(|error| invalid_option(key, value, error))
}

fn parse_compression(value: &str) -> DataSourceResult<DFParquetCompression> {
    let (codec, level) = split_parquet_compression_string(&value.to_lowercase())
        .map_err(|error| invalid_option("compression", value, error))?;
    let codec = match codec.as_str() {
        "none" | "" => "uncompressed",
        codec => codec,
    };
    // Bare codecs retain Parquet's defaults, notably zstd(1), not DataFusion's zstd(3).
    let resolved = match (codec, level) {
        ("gzip", None) => format!("gzip({})", GzipLevel::default().compression_level()),
        ("brotli", None) => format!("brotli({})", BrotliLevel::default().compression_level()),
        ("zstd", None) => format!("zstd({})", ZstdLevel::default().compression_level()),
        (codec, Some(level)) => format!("{codec}({level})"),
        (codec, None) => codec.to_string(),
    };
    DFParquetCompression::from_str(&resolved)
        .map_err(|error| invalid_option("compression", value, error))
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
            coerce_int96: self.global.coerce_int96.map(|unit| Some(unit.to_string())),
            bloom_filter_on_read: Some(self.global.bloom_filter_on_read),
            max_predicate_cache_size: Some(self.global.max_predicate_cache_size),
            path_glob_filter: None,
        })
    }
}

impl ParquetReadOptions {
    pub fn into_table_options(self) -> DataSourceResult<TableParquetOptions> {
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
            path_glob_filter: _,
        } = self;
        let coerce_int96 = coerce_int96
            .as_deref()
            .map(|value| parse_option::<DFTimeUnit>("coerce_int96", value))
            .transpose()?;
        let coerce_int96_tz = coerce_int96
            .map(|_| ConfigTimeZone::from_str("UTC"))
            .transpose()?;
        let global = ParquetOptions {
            enable_page_index,
            pruning,
            skip_metadata,
            metadata_size_hint,
            pushdown_filters,
            reorder_filters,
            schema_force_view_types,
            binary_as_string,
            // Spark reads INT96 as `TimestampType`, which stores UTC instants.
            coerce_int96_tz,
            coerce_int96,
            bloom_filter_on_read,
            max_predicate_cache_size,
            ..ParquetOptions::default()
        };
        Ok(TableParquetOptions {
            global,
            ..TableParquetOptions::default()
        })
    }
}

impl BuildPartialOptions<ParquetWritePartialOptions> for TableParquetOptions {
    fn build_partial_options(self) -> DataSourceResult<ParquetWritePartialOptions> {
        Ok(ParquetWritePartialOptions {
            data_page_size_limit: Some(self.global.data_pagesize_limit),
            write_batch_size: Some(self.global.write_batch_size),
            writer_version: Some(self.global.writer_version.to_string()),
            skip_arrow_metadata: Some(self.global.skip_arrow_metadata),
            compression: Some(self.global.compression.map(|value| value.to_string())),
            dictionary_enabled: Some(self.global.dictionary_enabled),
            dictionary_page_size_limit: Some(self.global.dictionary_page_size_limit),
            statistics_enabled: Some(
                self.global
                    .statistics_enabled
                    .map(|value| value.to_string()),
            ),
            max_row_group_size: Some(self.global.max_row_group_size),
            column_index_truncate_length: Some(self.global.column_index_truncate_length),
            statistics_truncate_length: Some(self.global.statistics_truncate_length),
            data_page_row_count_limit: Some(self.global.data_page_row_count_limit),
            encoding: Some(self.global.encoding.map(|value| value.to_string())),
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
            content_defined_chunking_enabled,
            content_defined_chunking_min_chunk_size,
            content_defined_chunking_max_chunk_size,
            content_defined_chunking_norm_level,
        } = self;
        let writer_version =
            parse_option::<DFParquetWriterVersion>("writer_version", &writer_version)?;
        let compression = compression.as_deref().map(parse_compression).transpose()?;
        let statistics_enabled = statistics_enabled
            .as_deref()
            .map(|value| parse_option::<DFParquetStatistics>("statistics_enabled", value))
            .transpose()?;
        let encoding = encoding
            .as_deref()
            .map(|value| parse_option::<DFParquetEncoding>("encoding", value))
            .transpose()?;
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
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::options::option_list;

    #[test]
    fn typed_read_options_preserve_defaults_overrides_and_utc() -> DataSourceResult<()> {
        for unit in [
            None,
            Some(DFTimeUnit::Second),
            Some(DFTimeUnit::Millisecond),
            Some(DFTimeUnit::Microsecond),
            Some(DFTimeUnit::Nanosecond),
        ] {
            let ctx = SessionContext::default();
            {
                let state = ctx.state_ref();
                let mut state = state.write();
                let execution = &mut state.config_mut().options_mut().execution;
                execution.time_zone = Some("America/Los_Angeles".parse()?);
                execution.parquet.coerce_int96 = unit;
                execution.parquet.coerce_int96_tz = Some("Asia/Tokyo".parse()?);
            }
            let state = ctx.state();
            let partial: ParquetReadPartialOptions = state
                .default_table_options()
                .parquet
                .build_partial_options()?;
            assert_eq!(
                partial.coerce_int96,
                unit.map(|unit| Some(unit.to_string()))
            );
            let options = ParquetReadOptions::resolve(&state, vec![])?.into_table_options()?;
            assert_eq!(
                options.global.coerce_int96,
                unit.or(Some(DFTimeUnit::Microsecond))
            );
            assert_eq!(
                options
                    .global
                    .coerce_int96_tz
                    .as_ref()
                    .map(|tz| tz.as_str()),
                Some("UTC")
            );

            let options = ParquetReadOptions::resolve(
                &state,
                vec![
                    option_list(&[("coerce_int96", "NS")]),
                    option_list(&[("coerce_int96", "MS")]),
                ],
            )?
            .into_table_options()?;
            assert_eq!(options.global.coerce_int96, Some(DFTimeUnit::Millisecond));
            assert_eq!(
                options
                    .global
                    .coerce_int96_tz
                    .as_ref()
                    .map(|tz| tz.as_str()),
                Some("UTC")
            );

            let options =
                ParquetReadOptions::resolve(&state, vec![option_list(&[("coerce_int96", "")])])?
                    .into_table_options()?;
            assert_eq!(options.global.coerce_int96, None);
            assert_eq!(options.global.coerce_int96_tz, None);
        }
        Ok(())
    }

    #[test]
    fn typed_write_options_roundtrip_including_explicit_none() -> DataSourceResult<()> {
        for (compression, statistics, encoding) in [
            (None, None, None),
            (
                Some(DFParquetCompression::Snappy),
                Some(DFParquetStatistics::None),
                Some(DFParquetEncoding::Plain),
            ),
            (
                Some(DFParquetCompression::Zstd(3)),
                Some(DFParquetStatistics::Page),
                Some(DFParquetEncoding::BitPacked),
            ),
        ] {
            let table = TableParquetOptions {
                global: ParquetOptions {
                    compression,
                    statistics_enabled: statistics,
                    encoding,
                    ..ParquetOptions::default()
                },
                ..TableParquetOptions::default()
            };
            let partial: ParquetWritePartialOptions = table.build_partial_options()?;
            assert_eq!(
                partial.compression,
                Some(compression.map(|value| value.to_string()))
            );
            assert_eq!(
                partial.statistics_enabled,
                Some(statistics.map(|value| value.to_string()))
            );
            assert_eq!(
                partial.encoding,
                Some(encoding.map(|value| value.to_string()))
            );
            let mut merged = ParquetWritePartialOptions::initialize();
            merged.merge(
                option_list(&[
                    ("compression", "gzip"),
                    ("statistics_enabled", "chunk"),
                    ("encoding", "rle"),
                ])
                .build_partial_options()?,
            );
            merged.merge(partial);
            let options = merged.finalize()?.into_table_options()?;
            assert_eq!(options.global.compression, compression);
            assert_eq!(options.global.statistics_enabled, statistics);
            assert_eq!(options.global.encoding, encoding);
        }
        Ok(())
    }

    #[test]
    fn write_defaults_and_empty_options_remain_distinct_from_none_statistics()
    -> DataSourceResult<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        let defaults = ParquetWriteOptions::resolve(&state, vec![])?.into_table_options()?;
        assert_eq!(
            defaults.global.compression,
            Some(DFParquetCompression::Zstd(3))
        );
        assert_eq!(
            defaults.global.statistics_enabled,
            Some(DFParquetStatistics::Page)
        );
        assert_eq!(defaults.global.encoding, None);

        let overrides = option_list(&[
            ("compression", "snappy"),
            ("statistics_enabled", "chunk"),
            ("encoding", "plain"),
        ]);
        let clear = option_list(&[
            ("compression", ""),
            ("statistics_enabled", ""),
            ("encoding", ""),
        ]);
        let options =
            ParquetWriteOptions::resolve(&state, vec![overrides, clear])?.into_table_options()?;
        assert_eq!(options.global.compression, None);
        assert_eq!(options.global.statistics_enabled, None);
        assert_eq!(options.global.encoding, None);

        let options = ParquetWriteOptions::resolve(
            &state,
            vec![option_list(&[("statistics_enabled", "NONE")])],
        )?
        .into_table_options()?;
        assert_eq!(
            options.global.statistics_enabled,
            Some(DFParquetStatistics::None)
        );
        assert_eq!(
            datafusion::parquet::file::properties::EnabledStatistics::from(
                DFParquetStatistics::None
            ),
            datafusion::parquet::file::properties::EnabledStatistics::None,
        );
        Ok(())
    }

    #[test]
    fn compression_preserves_aliases_defaults_and_level_boundaries() -> DataSourceResult<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        // Boundaries mirror DataFusion's parquet_compression_level_ranges_match_parquet;
        // bare-codec defaults intentionally follow Parquet rather than DataFusion.
        for (value, expected) in [
            ("NoNe", DFParquetCompression::Uncompressed),
            ("UNCOMPRESSED", DFParquetCompression::Uncompressed),
            ("SNAPPY", DFParquetCompression::Snappy),
            ("'GZIP'", DFParquetCompression::Gzip(6)),
            ("gzip(0)", DFParquetCompression::Gzip(0)),
            ("GZIP(9)", DFParquetCompression::Gzip(9)),
            ("brotli", DFParquetCompression::Brotli(1)),
            ("brotli(0)", DFParquetCompression::Brotli(0)),
            ("brotli(11)", DFParquetCompression::Brotli(11)),
            ("ZSTD", DFParquetCompression::Zstd(1)),
            ("zstd(0)", DFParquetCompression::Zstd(0)),
            ("\"zstd(22)\"", DFParquetCompression::Zstd(22)),
            ("LZ4", DFParquetCompression::Lz4),
            ("lz4_raw", DFParquetCompression::Lz4Raw),
        ] {
            let options =
                ParquetWriteOptions::resolve(&state, vec![option_list(&[("compression", value)])])?
                    .into_table_options()?;
            assert_eq!(options.global.compression, Some(expected), "{value}");
            // The typed conversion must also produce a valid Parquet codec, without panicking.
            let _: datafusion::parquet::basic::Compression = expected.into();
        }
        Ok(())
    }

    fn assert_invalid_option(
        result: DataSourceResult<TableParquetOptions>,
        key: &str,
        value: &str,
    ) {
        assert!(
            matches!(result, Err(DataSourceError::InvalidOption {
            key: actual_key, value: actual_value, cause: Some(cause),
        }) if actual_key == key && actual_value == value && !cause.is_empty()),
            "{key}={value:?}"
        );
    }

    #[test]
    fn invalid_typed_options_keep_the_original_key_and_value() -> DataSourceResult<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();
        for value in ["nanos", "ns ", "invalid"] {
            let options =
                ParquetReadOptions::resolve(&state, vec![option_list(&[("coerce_int96", value)])])?;
            assert_invalid_option(options.into_table_options(), "coerce_int96", value);
        }
        for (key, value) in [
            ("writer_version", "3.0"),
            ("encoding", "unknown"),
            ("statistics_enabled", "false"),
            ("compression", "unknown"),
            ("compression", "lzo"),
            ("compression", "none(1)"),
            ("compression", "uncompressed(1)"),
            ("compression", "SNAPPY(1)"),
            ("compression", "lz4(1)"),
            ("compression", "lz4_raw(1)"),
            ("compression", "gzip(10)"),
            ("compression", "brotli(12)"),
            ("compression", "zstd(23)"),
            ("compression", "zstd(-1)"),
            ("compression", "gzip("),
            ("compression", "gzip(é"),
            ("compression", "gzip(1"),
            ("compression", "gzip(1x"),
            ("compression", "gzip()"),
            ("compression", "gzip(1))"),
            ("compression", "gzip(1)junk"),
            ("compression", "gzip((1))"),
            ("compression", "gzip(4294967296)"),
        ] {
            let options = ParquetWriteOptions::resolve(&state, vec![option_list(&[(key, value)])])?;
            assert_invalid_option(options.into_table_options(), key, value);
        }
        Ok(())
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
        ]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()?;
        assert!(options.global.enable_page_index);
        assert!(options.global.pruning);
        assert!(!options.global.skip_metadata);
        assert_eq!(options.global.metadata_size_hint, Some(1024));
        assert!(options.global.pushdown_filters);
        assert!(!options.global.reorder_filters);
        assert!(options.global.schema_force_view_types);
        assert!(options.global.binary_as_string);
        assert_eq!(options.global.coerce_int96, Some(DFTimeUnit::Millisecond));
        assert!(options.global.bloom_filter_on_read);
        // max_predicate_cache_size uses parse_optional_usize: "0" = Some(0)
        assert_eq!(options.global.max_predicate_cache_size, Some(0));

        // metadata_size_hint = "0": parse_optional_non_zero_usize("0") returns None
        // which explicitly clears the value (overrides session default)
        let kv = option_list(&[("metadata_size_hint", "0")]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()?;
        assert_eq!(options.global.metadata_size_hint, None);

        // metadata_size_hint = "": parse_optional_non_zero_usize("") returns None
        let kv = option_list(&[("metadata_size_hint", "")]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()?;
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
            .into_table_options()?;
        assert_eq!(options.global.metadata_size_hint, Some(123));

        // When metadata_size_hint = "0" is provided, the value is explicitly cleared
        let kv = option_list(&[("metadata_size_hint", "0")]);
        let options = ParquetReadOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()?;
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
            ("content_defined_chunking_enabled", "true"),
            ("content_defined_chunking_min_chunk_size", "4096"),
            ("content_defined_chunking_max_chunk_size", "8192"),
            ("content_defined_chunking_norm_level", "-1"),
        ]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
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
        assert_eq!(
            options.global.compression,
            Some(DFParquetCompression::Snappy)
        );
        assert_eq!(options.global.dictionary_enabled, Some(true));
        assert_eq!(options.global.dictionary_page_size_limit, 2048);
        assert_eq!(
            options.global.statistics_enabled,
            Some(DFParquetStatistics::Chunk)
        );
        assert_eq!(options.global.max_row_group_size, 5000);
        assert_eq!(options.global.column_index_truncate_length, Some(100));
        assert_eq!(options.global.statistics_truncate_length, Some(200));
        assert_eq!(options.global.data_page_row_count_limit, 10000);
        assert_eq!(
            options.global.encoding,
            Some(DFParquetEncoding::DeltaBinaryPacked)
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
        assert!(options.global.content_defined_chunking.enabled);
        assert_eq!(options.global.content_defined_chunking.min_chunk_size, 4096);
        assert_eq!(options.global.content_defined_chunking.max_chunk_size, 8192);
        assert_eq!(options.global.content_defined_chunking.norm_level, -1);

        let kv = option_list(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
            ("encoding", ""),
        ]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
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
            .encoding = Some(DFParquetEncoding::BitPacked);
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
        let state = ctx.state();

        let kv = option_list(&[]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.max_row_group_size, 1234);
        assert_eq!(options.global.column_index_truncate_length, Some(32));
        assert_eq!(options.global.statistics_truncate_length, Some(99));
        assert_eq!(options.global.encoding, Some(DFParquetEncoding::BitPacked));
        assert!(options.global.content_defined_chunking.enabled);
        assert_eq!(options.global.content_defined_chunking.min_chunk_size, 4096);
        assert_eq!(options.global.content_defined_chunking.max_chunk_size, 8192);
        assert_eq!(options.global.content_defined_chunking.norm_level, -2);

        let kv = option_list(&[
            ("column_index_truncate_length", "0"),
            ("statistics_truncate_length", "0"),
            ("encoding", ""),
        ]);
        let options = ParquetWriteOptions::resolve(&state, vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.global.column_index_truncate_length, None);
        assert_eq!(options.global.statistics_truncate_length, None);
        assert_eq!(options.global.encoding, None);

        Ok(())
    }
}
