use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_delta_lake::session_extension::DeltaTableCache;
use sail_execution::artifact::{ArtifactRuntime, ArtifactRuntimeOptions};

use crate::runtime::RuntimeEnvFactory;
use crate::session_factory::SessionFactory;

pub struct WorkerSessionFactory {
    runtime_env: RuntimeEnvFactory,
    repartition_buffer_size: usize,
    artifact_runtime_options: ArtifactRuntimeOptions,
}

impl WorkerSessionFactory {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        let repartition_buffer_size = config.cluster.task_stream_buffer;
        let artifact_runtime_options = ArtifactRuntimeOptions {
            max_artifact_bytes: config.spark.artifact.max_artifact_bytes,
            max_session_bytes: config.spark.artifact.max_session_bytes,
            max_artifacts: config.spark.artifact.max_artifacts,
            max_archive_entries: config.spark.artifact.max_archive_entries,
            max_archive_expanded_bytes: config.spark.artifact.max_archive_expanded_bytes,
        };
        let runtime_env = RuntimeEnvFactory::new(config, runtime.clone());
        Self {
            runtime_env,
            repartition_buffer_size,
            artifact_runtime_options,
        }
    }
}

impl SessionFactory<()> for WorkerSessionFactory {
    fn create(&mut self, _info: ()) -> Result<SessionContext> {
        let runtime = self.runtime_env.create(Ok)?;
        // We still add default features for the worker session
        // since we need built-in functions to be available for the codec
        // when decoding the execution plan.
        let config = SessionConfig::default()
            .with_extension(Arc::new(DeltaTableCache::default()))
            .with_extension(Arc::new(ArtifactRuntime::try_new(
                self.artifact_runtime_options,
            )?))
            .with_extension(Arc::new(RepartitionBufferConfig::new(
                self.repartition_buffer_size,
            )));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        let session = SessionContext::new_with_state(state);
        Ok(session)
    }
}
