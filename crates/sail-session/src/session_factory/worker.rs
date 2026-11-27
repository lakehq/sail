// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::job_context::JobContextFactory;
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_common_datafusion::session::shuffle::ShuffleWriteConfig;
use sail_delta_lake::session_extension::DeltaTableCache;

use crate::runtime::RuntimeEnvFactory;
use crate::session_factory::SessionFactory;

pub struct WorkerSessionFactory {
    runtime_env: RuntimeEnvFactory,
    config: Arc<AppConfig>,
}

impl WorkerSessionFactory {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        let runtime_env = RuntimeEnvFactory::new(Arc::clone(&config), runtime);
        Self {
            runtime_env,
            config,
        }
    }
}

impl SessionFactory<()> for WorkerSessionFactory {
    fn create(&mut self, _info: ()) -> Result<SessionContext> {
        let runtime = self.runtime_env.create(Ok)?;
        // We still add default features for the worker session
        // since we need built-in functions to be available for the codec
        // when decoding the execution plan.
        let mut config = SessionConfig::default()
            .with_extension(Arc::new(DeltaTableCache::default()))
            .with_extension(Arc::new(RepartitionBufferConfig::new(
                self.config.cluster.task_stream_buffer,
            )))
            .with_extension(Arc::new(ShuffleWriteConfig::new(
                self.config.cluster.shuffle_write_buffer_size,
            )))
            .with_extension(Arc::new(JobContextFactory::new(create_job_context)));
        // Tasks run with the options of the driver session, so only the options that stay
        // worker-local are set here (see `create_job_context`).
        config.options_mut().execution.spill_compression =
            super::temporary_file_compression(self.config.runtime.temporary_files.compression);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        let session = SessionContext::new_with_state(state);
        Ok(session)
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
///   settings from the options only.
/// * `datafusion.execution.target_partitions` and `datafusion.execution.planning_concurrency`,
///   which size concurrency to the worker. The partitioning of a task is fixed in its plan.
/// * `datafusion.execution.spill_compression`, which encodes the worker's temporary files.
/// * The settings that cluster execution requires (see `apply_cluster_constraints`), which are
///   enforced even if the driver session changes them.
///
/// Every other option follows the driver session, including options that DataFusion adds later.
fn create_job_context(worker: &TaskContext, mut options: ConfigOptions) -> Result<TaskContext> {
    let local = worker.session_config().options();
    options.execution.target_partitions = local.execution.target_partitions;
    options.execution.planning_concurrency = local.execution.planning_concurrency;
    options.execution.spill_compression = local.execution.spill_compression;
    super::apply_cluster_constraints(&mut options);
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
    use datafusion::common::config::{ConfigNonZeroUsize, SpillCompression};
    use sail_common::config::TemporaryFileCompression;
    use sail_common_datafusion::extension::SessionExtensionAccessor;
    use sail_common_datafusion::session::time_zone::{
        session_time_zone, set_spark_session_time_zone,
    };

    use super::*;

    fn worker_session(config: AppConfig) -> Result<SessionContext> {
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        WorkerSessionFactory::new(Arc::new(config), runtime).create(())
    }

    #[tokio::test]
    async fn worker_applies_shuffle_write_buffer_size() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = AppConfig::load()?;
        config.cluster.shuffle_write_buffer_size = 1024;
        let session = worker_session(config)?;
        let context = session.task_ctx();
        let shuffle = context.extension::<ShuffleWriteConfig>()?;
        assert_eq!(shuffle.buffer_size(), 1024);
        Ok(())
    }

    #[tokio::test]
    async fn job_context_follows_the_driver_except_worker_local_settings()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = AppConfig::load()?;
        config.runtime.temporary_files.compression = TemporaryFileCompression::Lz4Frame;
        let session = worker_session(config)?;
        let worker = session.task_ctx();

        let mut driver = ConfigOptions::default();
        driver.execution.batch_size = ConfigNonZeroUsize::try_new(7)?;
        driver.execution.target_partitions = worker.session_config().target_partitions() + 1;
        driver.execution.planning_concurrency = 1;
        driver.execution.spill_compression = SpillCompression::Zstd;
        driver.execution.enable_nlj_coordinated_fallback = true;
        driver.optimizer.enable_join_dynamic_filter_pushdown = true;
        driver.execution.parquet.metadata_size_hint = Some(128);
        set_spark_session_time_zone(&mut driver, "America/Los_Angeles")?;

        let factory = worker.extension::<JobContextFactory>()?;
        let job = factory.create(&worker, driver)?;
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
        assert_eq!(
            job.extension::<ShuffleWriteConfig>()?.buffer_size(),
            worker.extension::<ShuffleWriteConfig>()?.buffer_size()
        );
        assert!(job.extension::<DeltaTableCache>().is_ok());
        assert_eq!(
            job.scalar_functions().len(),
            worker.scalar_functions().len()
        );
        Ok(())
    }
}
