// SPDX-License-Identifier: Apache-2.0

//! The driver session settings shipped with each job to workers.
//!
//! A job runs with the settings of the driver session at the time the job was submitted. Tasks
//! that run on the driver use that session directly. Tasks that run on a worker get a task context
//! that the worker builds from these settings once per job (see [`create_job_context`]).

use std::collections::HashMap;
use std::sync::LazyLock;

use datafusion::common::config::{ConfigField, ConfigOptions, ParquetOptions};
use datafusion::common::{Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use prost::Message;

use crate::job_runner::apply_cluster_constraints;
use crate::worker::r#gen::JobSession;

const DATAFUSION_PREFIX: &str = "datafusion.";
const PARQUET_PREFIX: &str = "datafusion.execution.parquet.";

/// The value of each DataFusion option in its default configuration.
static DEFAULT_ENTRIES: LazyLock<HashMap<String, Option<String>>> =
    LazyLock::new(|| entry_values(&ConfigOptions::default()).collect());

/// Returns the key and the printed value of each option.
fn entry_values(options: &ConfigOptions) -> impl Iterator<Item = (String, Option<String>)> {
    options
        .entries()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
}

impl JobSession {
    /// Captures the settings of a driver session.
    ///
    /// Only options that differ from the DataFusion defaults are captured by key, since the
    /// worker applies them on top of the defaults. The Parquet options are captured as a whole,
    /// since some of them cannot be unset by key. Configuration extensions are not shipped, so
    /// operators on workers read the session time zone from `datafusion.execution.time_zone`.
    pub(crate) fn capture(options: &ConfigOptions) -> Result<Self> {
        let mut values = HashMap::new();
        for (key, value) in entry_values(options) {
            if !key.starts_with(DATAFUSION_PREFIX) || key.starts_with(PARQUET_PREFIX) {
                continue;
            }
            if DEFAULT_ENTRIES.get(&key) == Some(&value) {
                continue;
            }
            let Some(value) = value else {
                return internal_err!("cannot ship the unset session option {key} to workers");
            };
            values.insert(key, value);
        }
        let parquet = gen_datafusion_common::ParquetOptions::try_from(&options.execution.parquet)?
            .encode_to_vec();
        Ok(Self {
            options: values,
            parquet,
        })
    }

    /// Returns the options of the driver session.
    pub(crate) fn config_options(&self) -> Result<ConfigOptions> {
        let mut options = ConfigOptions::default();
        for (key, value) in &self.options {
            let Some(field) = key.strip_prefix(DATAFUSION_PREFIX) else {
                return internal_err!("invalid session option from the driver: {key}");
            };
            // Set the field itself rather than calling `ConfigOptions::set`, which expands
            // some keys to several fields.
            ConfigField::set(&mut options, field, value)
                .map_err(|e| e.context(format!("cannot apply session option {key}")))?;
        }
        let parquet = gen_datafusion_common::ParquetOptions::decode(self.parquet.as_slice())
            .map_err(|e| {
                internal_datafusion_err!("invalid Parquet options from the driver: {e}")
            })?;
        options.execution.parquet = ParquetOptions::try_from(&parquet)?;
        Ok(options)
    }
}

/// Creates the task context of a job on a worker from the options of the driver session that
/// submitted the job, so that tasks on workers run with the same settings as tasks on the driver.
///
/// A setting stays worker-local when it describes the worker process rather than the query:
///
/// * The runtime environment (memory pool, spill and temporary files, object stores and file
///   caches), the session extensions (shuffle and repartition buffers, Delta table cache), the
///   functions, and the session ID, which scopes cache entries to the object stores of the
///   worker. These are not options, so they come from the worker session as a whole.
///   Extensions that only the driver session has (such as the Spark configuration and the
///   catalog manager) do not exist on workers, so operators that run on workers read query
///   settings from the options only. Built-in functions are not reconfigured with the options
///   of the job, so operators must not rely on `ScalarUDFImpl::with_updated_config` on workers.
/// * `datafusion.execution.target_partitions` and `datafusion.execution.planning_concurrency`,
///   which size concurrency to the worker. The partitioning of a task is fixed in its plan.
///   An option whose DataFusion default depends on the host (today those that default to
///   `get_available_parallelism()`) must be kept here too, since the driver ships only the
///   options that differ from the defaults on its own host.
/// * `datafusion.execution.spill_compression`, which encodes the worker's temporary files.
/// * The settings that cluster execution requires (see [`apply_cluster_constraints`]), which are
///   enforced even if the driver session changes them.
///
/// Every other option follows the driver session, including options that DataFusion adds later.
pub(crate) fn create_job_context(
    worker: &TaskContext,
    mut options: ConfigOptions,
) -> Result<TaskContext> {
    let local = worker.session_config().options();
    options.execution.target_partitions = local.execution.target_partitions;
    options.execution.planning_concurrency = local.execution.planning_concurrency;
    options.execution.spill_compression = local.execution.spill_compression;
    apply_cluster_constraints(&mut options);
    let mut config = worker.session_config().clone();
    *config.options_mut() = options;
    Ok(TaskContext::new(
        None,
        worker.session_id(),
        config,
        worker.scalar_functions().clone(),
        worker.higher_order_functions().clone(),
        worker.aggregate_functions().clone(),
        worker.window_functions().clone(),
        worker.runtime_env(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::config::{
        ConfigNonZeroUsize, MaxRowGroupBytes, ParquetCdcOptions, SpillCompression,
    };
    use datafusion::common::parquet_config::{
        DFParquetCompression, DFParquetEncoding, DFParquetStatistics, DFParquetWriterVersion,
        DFTimeUnit, RowGroupRangeAssignment,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};
    use sail_common_datafusion::extension::SessionExtensionAccessor;
    use sail_common_datafusion::session::shuffle::ShuffleWriteConfig;
    use sail_common_datafusion::session::time_zone::{
        session_time_zone, set_spark_session_time_zone,
    };
    use sail_delta_lake::DeltaSnapshotConfig;

    use super::*;

    fn entries(options: &ConfigOptions) -> Vec<(String, Option<String>)> {
        entry_values(options).collect()
    }

    /// The options are shipped by their printed values, so each printed value must set the
    /// option to the same value. Options that are unset by default are never shipped unset.
    #[test]
    fn printed_option_values_set_the_same_values() -> Result<()> {
        let defaults = entries(&ConfigOptions::default());
        for (key, value) in &defaults {
            if key.starts_with(PARQUET_PREFIX) {
                continue;
            }
            let (Some(field), Some(value)) = (key.strip_prefix(DATAFUSION_PREFIX), value) else {
                continue;
            };
            let mut options = ConfigOptions::default();
            ConfigField::set(&mut options, field, value)?;
            assert_eq!(entries(&options), defaults, "option {key} = {value}");
        }
        Ok(())
    }

    #[test]
    fn job_session_round_trips_driver_options() -> Result<()> {
        let mut driver = ConfigOptions::default();
        driver.execution.batch_size = ConfigNonZeroUsize::try_new(7)?;
        driver.execution.target_partitions = 3;
        driver.execution.collect_statistics = false;
        driver.execution.spill_compression = SpillCompression::Zstd;
        driver.execution.enable_nlj_coordinated_fallback = false;
        driver.catalog.create_default_catalog_and_schema = false;
        driver.catalog.information_schema = false;
        driver.optimizer.prefer_hash_join = false;
        driver.optimizer.enable_join_dynamic_filter_pushdown = false;
        driver.execution.parquet.metadata_size_hint = None;
        driver.execution.parquet.coerce_int96 = Some(DFTimeUnit::Nanosecond);
        driver.execution.parquet.binary_as_string = true;
        driver.execution.parquet.compression = None;
        set_spark_session_time_zone(&mut driver, "+05:30")?;

        let session = JobSession::decode(JobSession::capture(&driver)?.encode_to_vec().as_slice())
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        assert!(
            session
                .options
                .keys()
                .all(|key| !key.starts_with(PARQUET_PREFIX))
        );
        let options = session.config_options()?;

        assert_eq!(entries(&options), entries(&driver));
        assert_eq!(options.execution.parquet, driver.execution.parquet);
        assert_eq!(session_time_zone(&options), Some("+05:30"));
        assert!(!options.optimizer.enable_join_dynamic_filter_pushdown);
        assert!(options.optimizer.enable_topk_dynamic_filter_pushdown);
        Ok(())
    }

    /// Every Parquet option must reach workers. The struct literal fails to compile when
    /// DataFusion adds an option, so that its transport is checked here.
    #[test]
    fn job_session_round_trips_every_parquet_option() -> Result<()> {
        let parquet = ParquetOptions {
            enable_page_index: false,
            pruning: false,
            skip_metadata: false,
            metadata_size_hint: Some(128),
            pushdown_filters: true,
            reorder_filters: true,
            force_filter_selections: true,
            schema_force_view_types: false,
            binary_as_string: true,
            coerce_int96: Some(DFTimeUnit::Millisecond),
            coerce_int96_tz: Some("America/Los_Angeles".parse()?),
            bloom_filter_on_read: false,
            max_predicate_cache_size: Some(4096),
            max_in_list_size: 7,
            enable_rle_to_dictionary: true,
            row_group_range_assignment: RowGroupRangeAssignment::Midpoint,
            data_pagesize_limit: 4096,
            write_batch_size: 64,
            writer_version: DFParquetWriterVersion::V2_0,
            skip_arrow_metadata: true,
            compression: Some(DFParquetCompression::Gzip(3)),
            dictionary_enabled: Some(false),
            dictionary_page_size_limit: 4096,
            statistics_enabled: Some(DFParquetStatistics::Chunk),
            max_row_group_size: 2048,
            max_row_group_bytes: Some(MaxRowGroupBytes::try_new(8192)?),
            created_by: "test".to_string(),
            column_index_truncate_length: Some(16),
            statistics_truncate_length: None,
            data_page_row_count_limit: 100,
            encoding: Some(DFParquetEncoding::Plain),
            bloom_filter_on_write: true,
            bloom_filter_fpp: Some(0.01),
            bloom_filter_ndv: Some(1000),
            write_row_group_number_distinct_values: true,
            allow_single_file_parallelism: false,
            maximum_parallel_row_group_writers: 3,
            maximum_buffered_record_batches_per_stream: 5,
            content_defined_chunking: ParquetCdcOptions {
                enabled: true,
                min_chunk_size: 1024,
                max_chunk_size: 8192,
                norm_level: 1,
            },
        };
        assert_ne!(parquet, ParquetOptions::default());
        let mut driver = ConfigOptions::default();
        driver.execution.parquet = parquet;
        let options = JobSession::capture(&driver)?.config_options()?;
        assert_eq!(options.execution.parquet, driver.execution.parquet);
        Ok(())
    }

    #[test]
    fn job_session_without_changes_has_no_options() -> Result<()> {
        let session = JobSession::capture(&ConfigOptions::default())?;
        assert!(session.options.is_empty());
        assert_eq!(
            entries(&session.config_options()?),
            entries(&ConfigOptions::default())
        );
        Ok(())
    }

    /// Workers read the session time zone from `datafusion.execution.time_zone`, which holds the
    /// Arrow label of the Spark session time zone, so they read the same time zone as the driver.
    #[test]
    fn job_session_ships_the_session_time_zone_as_an_option() -> Result<()> {
        for time_zone in [
            "America/Los_Angeles",
            "PST",
            "Z",
            "GMT+8",
            "+05:30",
            "+05:30:15",
        ] {
            let mut driver = ConfigOptions::default();
            set_spark_session_time_zone(&mut driver, time_zone)?;
            let options = JobSession::capture(&driver)?.config_options()?;
            assert_eq!(
                options.execution.time_zone, driver.execution.time_zone,
                "{time_zone}"
            );
            assert_eq!(
                DeltaSnapshotConfig::default()
                    .with_partition_time_zone(&options)
                    .partition_time_zone,
                DeltaSnapshotConfig::default()
                    .with_partition_time_zone(&driver)
                    .partition_time_zone,
                "{time_zone}"
            );
        }
        Ok(())
    }

    #[test]
    fn job_session_rejects_unknown_options() {
        let session = JobSession {
            options: HashMap::from([("datafusion.execution.unknown".to_string(), "1".to_string())]),
            ..Default::default()
        };
        assert!(session.config_options().is_err());
    }

    #[test]
    fn job_context_follows_the_driver_except_worker_local_settings() -> Result<()> {
        let mut config =
            SessionConfig::new().with_extension(Arc::new(ShuffleWriteConfig::new(1024)));
        config.options_mut().execution.spill_compression = SpillCompression::Lz4Frame;
        let worker = SessionContext::new_with_config(config).task_ctx();

        let mut driver = ConfigOptions::default();
        driver.execution.batch_size = ConfigNonZeroUsize::try_new(7)?;
        driver.execution.target_partitions = worker.session_config().target_partitions() + 1;
        driver.execution.planning_concurrency = 1;
        driver.execution.spill_compression = SpillCompression::Zstd;
        driver.execution.enable_nlj_coordinated_fallback = true;
        driver.optimizer.enable_join_dynamic_filter_pushdown = true;
        driver.execution.parquet.metadata_size_hint = Some(128);
        set_spark_session_time_zone(&mut driver, "America/Los_Angeles")?;

        let job = create_job_context(&worker, driver)?;
        let options = job.session_config().options();
        let local = worker.session_config().options();
        assert_eq!(options.execution.batch_size.get(), 7);
        assert_eq!(options.execution.parquet.metadata_size_hint, Some(128));
        assert_eq!(session_time_zone(options), Some("America/Los_Angeles"));
        assert_eq!(
            options.execution.target_partitions,
            local.execution.target_partitions
        );
        assert_eq!(
            options.execution.planning_concurrency,
            local.execution.planning_concurrency
        );
        assert_eq!(
            options.execution.spill_compression,
            SpillCompression::Lz4Frame
        );
        assert!(!options.execution.enable_nlj_coordinated_fallback);
        assert!(!options.optimizer.enable_join_dynamic_filter_pushdown);
        assert_eq!(job.session_id(), worker.session_id());
        assert!(Arc::ptr_eq(&job.runtime_env(), &worker.runtime_env()));
        assert_eq!(job.extension::<ShuffleWriteConfig>()?.buffer_size(), 1024);
        assert_eq!(
            job.scalar_functions().len(),
            worker.scalar_functions().len()
        );
        Ok(())
    }
}
