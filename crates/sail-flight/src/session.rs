use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_session::optimizer::{default_analyzer_rules, default_optimizer_rules};

/// Creates a SessionContext configured with Sail's optimizers and analyzers.
/// This provides a lightweight Sail-compatible context without the full server infrastructure.
pub fn create_sail_session_context() -> SessionContext {
    // Create the PlanService extension required by the resolver
    let plan_service = PlanService::new(
        Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
        Box::new(SparkPlanFormatter),
    );

    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("sail", "public")
        .with_extension(Arc::new(plan_service));

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(default_analyzer_rules())
        .with_optimizer_rules(default_optimizer_rules())
        .build();

    SessionContext::new_with_state(state)
}
