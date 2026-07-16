use std::sync::{Arc, Mutex};

use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;
use sail_execution::driver::{DriverGateway, DriverGatewayOptions};
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_server::actor::ActorSystem;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionJobRunnerFactory, ServerSessionMutator,
    SessionFactory, SessionJobRunnerFactory,
};
use sail_session::session_manager::{SessionManager, SessionManagerOptions};

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
    let system = Arc::new(Mutex::new(ActorSystem::new()));
    let factory = {
        let config = config.clone();
        let runtime = runtime.clone();
        Box::new(move || create_flight_session_factory(config.clone(), runtime.clone()))
    };
    let job_runner_factory = {
        let config = config.clone();
        let runtime = runtime.clone();
        let system = system.clone();
        Box::new(move || {
            Box::new(ServerSessionJobRunnerFactory::new(
                config.clone(),
                runtime.clone(),
                system.clone(),
            )) as Box<dyn SessionJobRunnerFactory>
        })
    };
    let gateway = if matches!(&config.mode, ExecutionMode::Local) {
        None
    } else {
        Some(
            DriverGateway::try_new(DriverGatewayOptions::new(&config))
                .await
                .map_err(|e| {
                    FlightError::Session(format!("failed to create driver gateway: {e}"))
                })?,
        )
    };
    let mut options =
        SessionManagerOptions::new(runtime.clone(), system, factory, job_runner_factory)
            .with_session_timeout(std::time::Duration::from_secs(
                config.flight.session_timeout_secs,
            ));
    if let Some(gateway) = gateway {
        options = options.with_driver_gateway(gateway);
    }
    SessionManager::try_new(options).map_err(|e| {
        FlightError::DataFusion(internal_datafusion_err!(
            "Failed to create session manager: {}",
            e
        ))
    })
}
