use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
};
use sail_session::session_manager::{SessionManager, create_session_manager};

use crate::error::FlightError;

pub struct FlightSessionMutator {
    #[expect(dead_code)]
    config: Arc<AppConfig>,
}

impl ServerSessionMutator for FlightSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        _info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
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
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let mutator = Box::new(FlightSessionMutator {
        config: config.clone(),
    });
    Box::new(ServerSessionFactory::new(config, runtime, mutator))
}

pub async fn create_flight_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<SessionManager, FlightError> {
    create_session_manager(
        config.clone(),
        runtime,
        create_flight_session_factory,
        Duration::from_secs(config.flight.session_timeout_secs),
    )
    .await
    .map_err(|e| {
        FlightError::DataFusion(internal_datafusion_err!(
            "Failed to create session manager: {}",
            e
        ))
    })
}
