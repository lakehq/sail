// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
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
            )));
        // Tasks run with the options of the driver session, so only the options that stay
        // worker-local are set here (see `create_job_context` in `sail-execution`).
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

#[cfg(test)]
mod tests {
    use datafusion::common::config::SpillCompression;
    use sail_common::config::TemporaryFileCompression;
    use sail_common_datafusion::extension::SessionExtensionAccessor;

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
    async fn worker_applies_temporary_file_compression() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = AppConfig::load()?;
        config.runtime.temporary_files.compression = TemporaryFileCompression::Lz4Frame;
        let session = worker_session(config)?;
        let context = session.task_ctx();
        let execution = &context.session_config().options().execution;
        assert_eq!(execution.spill_compression, SpillCompression::Lz4Frame);
        Ok(())
    }
}
