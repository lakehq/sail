// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigNonZeroUsize;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_common_datafusion::session::shuffle::ShuffleWriteConfig;
use sail_delta_lake::session_extension::DeltaTableCache;
use sail_object_store::VendedObjectStores;

use crate::runtime::RuntimeEnvFactory;
use crate::session_factory::SessionFactory;

pub struct WorkerSessionFactory {
    runtime_env: RuntimeEnvFactory,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
}

impl WorkerSessionFactory {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        let runtime_env = RuntimeEnvFactory::new(Arc::clone(&config), runtime.clone());
        Self {
            runtime_env,
            config,
            runtime,
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
            // The storage locations that the session of the driver accesses with storage
            // credentials issued by catalogs are accessed in the same way, with the
            // credentials that the worker requests from the driver.
            .with_extension(Arc::new(VendedObjectStores::new(self.runtime.clone())));
        let execution = &mut config.options_mut().execution;
        execution.batch_size = ConfigNonZeroUsize::try_new(self.config.execution.batch_size)?;
        execution.spill_compression =
            super::temporary_file_compression(self.config.runtime.temporary_files.compression);
        super::apply_parquet_config(&mut execution.parquet, &self.config.parquet);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        let session = SessionContext::new_with_state(state);
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use sail_common_datafusion::extension::SessionExtensionAccessor;

    use super::*;

    #[tokio::test]
    async fn worker_applies_shuffle_write_buffer_size() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = AppConfig::load()?;
        config.cluster.shuffle_write_buffer_size = 1024;
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let session = WorkerSessionFactory::new(Arc::new(config), runtime).create(())?;
        let context = session.task_ctx();
        let shuffle = context.extension::<ShuffleWriteConfig>()?;
        assert_eq!(shuffle.buffer_size(), 1024);
        Ok(())
    }

    #[tokio::test]
    async fn worker_applies_parquet_config() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = AppConfig::load()?;
        config.parquet.enable_page_index = false;
        config.parquet.pruning = false;
        config.parquet.metadata_size_hint = Some(32768);
        config.parquet.pushdown_filters = true;
        config.parquet.schema_force_view_types = false;
        config.parquet.binary_as_string = true;
        config.parquet.compression = "gzip(3)".to_string();
        config.parquet.max_row_group_size = 2048;
        config.parquet.force_filter_selections = true;
        config.parquet.max_in_list_size = 50;
        config.parquet.max_row_group_bytes = Some(8192);
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let session = WorkerSessionFactory::new(Arc::new(config), runtime).create(())?;
        let state = session.state_ref();
        let state = state.read();
        let parquet = &state.config_options().execution.parquet;
        assert!(!parquet.enable_page_index);
        assert!(!parquet.pruning);
        assert_eq!(parquet.metadata_size_hint, Some(32768));
        assert!(parquet.pushdown_filters);
        assert!(!parquet.schema_force_view_types);
        assert!(parquet.binary_as_string);
        assert_eq!(parquet.compression.as_deref(), Some("gzip(3)"));
        assert_eq!(parquet.max_row_group_size, 2048);
        assert!(parquet.force_filter_selections);
        assert_eq!(parquet.max_in_list_size, 50);
        assert_eq!(parquet.max_row_group_bytes.map(|v| v.get()), Some(8192));
        assert_eq!(parquet.coerce_int96.as_deref(), Some("us"));
        assert_eq!(parquet.bloom_filter_ndv, None);
        assert_eq!(
            parquet.created_by,
            sail_common::utils::parquet::SAIL_PARQUET_CREATED_BY
        );
        Ok(())
    }
}
