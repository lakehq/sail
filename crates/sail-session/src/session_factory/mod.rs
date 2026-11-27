// SPDX-License-Identifier: Apache-2.0

mod job_runner;
mod server;
mod worker;

use std::str::FromStr;

use datafusion::common::Result;
use datafusion::common::config::{ConfigOptions, MaxRowGroupBytes, ParquetOptions};
use datafusion::common::parquet_config::{DFParquetWriterVersion, DFTimeUnit};
use datafusion::prelude::SessionContext;
pub use job_runner::{
    ServerSessionJobRunnerFactory, SessionJobRunner, SessionJobRunnerFactory, SessionJobRunnerInfo,
};
use sail_common::config::{ParquetConfig, TemporaryFileCompression};
use sail_common::utils::parquet::SAIL_PARQUET_CREATED_BY;
use sail_data_source::formats::parquet::parse_parquet_compression;
pub use server::{ServerSessionFactory, ServerSessionInfo, ServerSessionMutator};
pub use worker::WorkerSessionFactory;

pub trait SessionFactory<I>: Send {
    /// Create a DataFusion [`SessionContext`].
    /// This method takes `&mut self` so that the factory can maintain internal state if needed.
    /// This method takes an opaque parameter of type `I` for session-specific information.
    fn create(&mut self, info: I) -> Result<SessionContext>;
}

/// Applies the settings that cluster execution requires, on the driver that plans for the
/// cluster and on workers.
///
/// Each task of a stage decodes its own copy of the plan, so the partitions of a stage cannot
/// share state within a process. DataFusion's coordinated nested loop join fallback and its
/// hash join dynamic filter both assume that every partition of the plan reports to
/// process-local state, so both are disabled. Task-local TopK and aggregate dynamic filters
/// remain enabled.
fn apply_cluster_constraints(options: &mut ConfigOptions) {
    options.execution.enable_nlj_coordinated_fallback = false;
    options.optimizer.enable_join_dynamic_filter_pushdown = false;
}

fn temporary_file_compression(
    compression: TemporaryFileCompression,
) -> datafusion::common::config::SpillCompression {
    use datafusion::common::config::SpillCompression;

    match compression {
        TemporaryFileCompression::Uncompressed => SpillCompression::Uncompressed,
        TemporaryFileCompression::Lz4Frame => SpillCompression::Lz4Frame,
        TemporaryFileCompression::Zstd => SpillCompression::Zstd,
    }
}

// Apply the same Parquet settings when planning on the server or building scans on workers.
fn apply_parquet_config(parquet: &mut ParquetOptions, config: &ParquetConfig) -> Result<()> {
    parquet.created_by = SAIL_PARQUET_CREATED_BY.to_string();
    parquet.enable_page_index = config.enable_page_index;
    parquet.pruning = config.pruning;
    parquet.skip_metadata = config.skip_metadata;
    parquet.metadata_size_hint = config.metadata_size_hint;
    parquet.pushdown_filters = config.pushdown_filters;
    parquet.reorder_filters = config.reorder_filters;
    parquet.force_filter_selections = config.force_filter_selections;
    parquet.schema_force_view_types = config.schema_force_view_types;
    parquet.binary_as_string = config.binary_as_string;
    parquet.max_predicate_cache_size = Some(config.max_predicate_cache_size);
    parquet.max_in_list_size = config.max_in_list_size;
    parquet.coerce_int96 = Some(DFTimeUnit::Microsecond);
    parquet.data_pagesize_limit = config.data_page_size_limit;
    parquet.write_batch_size = config.write_batch_size;
    parquet.writer_version =
        DFParquetWriterVersion::from_str(config.writer_version.as_str()).unwrap_or_default();
    parquet.skip_arrow_metadata = config.skip_arrow_metadata;
    parquet.compression = Some(parse_parquet_compression(&config.compression)?);
    parquet.dictionary_enabled = Some(config.dictionary_enabled);
    parquet.dictionary_page_size_limit = config.dictionary_page_size_limit;
    parquet.statistics_enabled = Some(config.statistics_enabled.parse()?);
    parquet.max_row_group_size = config.max_row_group_size;
    parquet.max_row_group_bytes = config
        .max_row_group_bytes
        .and_then(|v| MaxRowGroupBytes::try_new(v).ok());
    parquet.column_index_truncate_length = config.column_index_truncate_length;
    parquet.statistics_truncate_length = config.statistics_truncate_length;
    parquet.data_page_row_count_limit = config.data_page_row_count_limit;
    parquet.encoding = config.encoding.as_deref().map(str::parse).transpose()?;
    parquet.bloom_filter_on_read = config.bloom_filter_on_read;
    parquet.bloom_filter_on_write = config.bloom_filter_on_write;
    parquet.bloom_filter_fpp = Some(config.bloom_filter_fpp);
    parquet.bloom_filter_ndv = config.bloom_filter_ndv;
    parquet.allow_single_file_parallelism = config.allow_single_file_parallelism;
    parquet.maximum_parallel_row_group_writers = config.maximum_parallel_row_group_writers;
    parquet.maximum_buffered_record_batches_per_stream =
        config.maximum_buffered_record_batches_per_stream;
    parquet.content_defined_chunking.enabled = config.content_defined_chunking.enabled;
    parquet.content_defined_chunking.min_chunk_size =
        config.content_defined_chunking.min_chunk_size;
    parquet.content_defined_chunking.max_chunk_size =
        config.content_defined_chunking.max_chunk_size;
    parquet.content_defined_chunking.norm_level = config.content_defined_chunking.norm_level;
    Ok(())
}
