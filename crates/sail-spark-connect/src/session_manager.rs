use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::common::{internal_datafusion_err, Result};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_server::actor::ActorSystem;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
};
use sail_session::session_manager::{SessionManager, SessionManagerOptions};

use crate::error::SparkResult;
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
        let spark = SparkSession::try_new(
            info.session_id.clone(),
            info.user_id.clone(),
            SparkSessionOptions {
                execution_heartbeat_interval: Duration::from_secs(
                    self.config.spark.execution_heartbeat_interval_secs,
                ),
            },
        )
        .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(config
            .with_extension(Arc::new(plan_service))
            .with_extension(Arc::new(spark)))
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
    remote_plugins: Vec<Arc<sail_session::remote_plugin::RemotePluginManager>>,
    pending_plugin_endpoints: Vec<String>,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let mutator = Box::new(SparkSessionMutator {
        config: config.clone(),
    });
    Box::new(ServerSessionFactory::new(
        config,
        runtime,
        system,
        mutator,
        remote_plugins,
        pending_plugin_endpoints,
    ))
}

pub async fn create_spark_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    plugin_endpoints: Vec<String>,
) -> SparkResult<SessionManager> {
    // Connect to remote Flight plugin servers (best-effort: warn and skip on failure)
    let mut remote_plugins = Vec::new();
    let mut pending_endpoints = Vec::new();
    for endpoint in &plugin_endpoints {
        match sail_session::remote_plugin::RemotePluginManager::connect(
            endpoint,
            runtime.primary().clone(),
        )
        .await
        {
            Ok(manager) => remote_plugins.push(Arc::new(manager)),
            Err(e) => {
                log::warn!(
                    "Failed to connect to plugin {endpoint}, will retry on session creation: {e}"
                );
                pending_endpoints.push(endpoint.clone());
            }
        }
    }

    let system = Arc::new(Mutex::new(ActorSystem::new()));
    let factory = {
        let config = config.clone();
        let runtime = runtime.clone();
        let system = system.clone();
        let remote_plugins = remote_plugins.clone();
        let pending_endpoints = pending_endpoints.clone();
        Box::new(move || {
            create_spark_session_factory(
                config.clone(),
                runtime.clone(),
                system.clone(),
                remote_plugins.clone(),
                pending_endpoints.clone(),
            )
        })
    };
    let options = SessionManagerOptions::new(runtime.clone(), system, factory)
        .with_session_timeout(Duration::from_secs(config.spark.session_timeout_secs));
    Ok(SessionManager::try_new(options)?)
}
