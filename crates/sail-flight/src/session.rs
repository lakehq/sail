use std::sync::{Arc, Mutex};

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

use crate::error::FlightError;

/// Session mutator for Flight SQL sessions.
///
/// This mutator configures the session with the necessary extensions for
/// Flight SQL query processing, reusing Sail's shared components.
pub struct FlightSessionMutator {
    #[allow(dead_code)]
    config: Arc<AppConfig>,
}

impl ServerSessionMutator for FlightSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        _info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        // Create the PlanService extension required by the resolver
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        Ok(config.with_extension(Arc::new(plan_service)))
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

fn create_flight_session_factory(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: Arc<Mutex<ActorSystem>>,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let mutator = Box::new(FlightSessionMutator {
        config: config.clone(),
    });
    Box::new(ServerSessionFactory::new(config, runtime, system, mutator))
}

/// Creates a SessionManager configured for Flight SQL.
///
/// This provides multi-session support for Flight SQL, allowing per-connection
/// session isolation similar to Spark Connect.
///
/// # Architecture
///
/// Uses the same SessionManager infrastructure as `sail-spark-connect`:
/// - Actor-based session management for concurrent requests
/// - Session factory for creating new sessions on demand
/// - Automatic session cleanup and lifecycle management
///
/// # Shared components with sail-spark-connect
/// - `sail_session::SessionManager`: Multi-session orchestration
/// - `sail_session::ServerSessionFactory`: Session creation and configuration
/// - `sail_plan`: PlanResolver, PlanConfig, execute_logical_plan
/// - `sail_session`: Optimizer rules, analyzer rules, table format registry
pub fn create_flight_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<SessionManager, FlightError> {
    let system = Arc::new(Mutex::new(ActorSystem::new()));
    let factory = {
        let config = config.clone();
        let runtime = runtime.clone();
        let system = system.clone();
        Box::new(move || {
            create_flight_session_factory(config.clone(), runtime.clone(), system.clone())
        })
    };
    let options = SessionManagerOptions::new(config.clone(), runtime.clone(), system, factory);
    SessionManager::try_new(options).map_err(|e| {
        FlightError::Internal(internal_datafusion_err!(
            "Failed to create session manager: {}",
            e
        ))
    })
}
