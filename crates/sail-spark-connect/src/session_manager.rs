use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::artifact::CachedLocalRelationService;
use sail_common_datafusion::session::plan::PlanService;
use sail_execution::artifact::{ArtifactRuntime, ArtifactRuntimeOptions};
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_server::actor::ActorSystem;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
};
use sail_session::session_manager::{SessionManager, SessionManagerOptions};

use crate::artifact::{ArtifactLimits, SparkCachedLocalRelationLoader};
use crate::error::{SparkError, SparkResult};
use crate::session::{SparkSession, SparkSessionOptions};

pub struct SparkSessionMutator {
    config: Arc<AppConfig>,
}

impl ServerSessionMutator for SparkSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        let spark = Arc::new(
            SparkSession::try_new(
                info.session_id.clone(),
                info.user_id.clone(),
                SparkSessionOptions {
                    execution_heartbeat_interval: Duration::from_secs(
                        self.config.spark.execution_heartbeat_interval_secs,
                    ),
                    artifact_limits: ArtifactLimits {
                        max_artifact_bytes: self.config.spark.artifact.max_artifact_bytes,
                        max_session_bytes: self.config.spark.artifact.max_session_bytes,
                        max_artifacts: self.config.spark.artifact.max_artifacts,
                        max_chunks: self.config.spark.artifact.max_chunks,
                    },
                },
            )
            .map_err(|e| internal_datafusion_err!("{e}"))?,
        );
        let cached_local_relations = CachedLocalRelationService::new(Arc::new(
            SparkCachedLocalRelationLoader::new(spark.clone()),
        ));
        let artifact_runtime = ArtifactRuntime::try_new(ArtifactRuntimeOptions {
            max_archive_entries: self.config.spark.artifact.max_archive_entries,
            max_archive_expanded_bytes: self.config.spark.artifact.max_archive_expanded_bytes,
        })?;
        Ok(config
            .with_extension(Arc::new(plan_service))
            .with_extension(Arc::new(cached_local_relations))
            .with_extension(Arc::new(artifact_runtime))
            .with_extension(spark))
    }

    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder> {
        Ok(builder)
    }

    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder> {
        Ok(builder)
    }
}

fn create_spark_session_factory(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: Arc<Mutex<ActorSystem>>,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let mutator = Box::new(SparkSessionMutator {
        config: config.clone(),
    });
    Box::new(ServerSessionFactory::new(config, runtime, system, mutator))
}

pub fn create_spark_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> SparkResult<SessionManager> {
    let system = Arc::new(Mutex::new(ActorSystem::new()));
    let factory = {
        let config = config.clone();
        let runtime = runtime.clone();
        let system = system.clone();
        Box::new(move || {
            create_spark_session_factory(config.clone(), runtime.clone(), system.clone())
        })
    };
    let options = SessionManagerOptions::new(runtime.clone(), system, factory)
        .with_session_timeout(Duration::from_secs(config.spark.session_timeout_secs))
        .with_options(config.raw().map_err(SparkError::from)?);
    Ok(SessionManager::try_new(options)?)
}
