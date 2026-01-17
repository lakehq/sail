use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_session::optimizer::{default_analyzer_rules, default_optimizer_rules};

/// Creates a SessionContext configured with Sail's optimizers and analyzers.
///
/// This function reuses the **same Sail crates** that `sail-spark-connect` uses
/// (sail-plan, sail-session, sail-common-datafusion), but with a simpler session model
/// suitable for Flight SQL (no multi-session manager, no job runner).
///
/// Key features:
/// - Uses Sail's custom analyzers and optimizers (shared with Spark Connect)
/// - Registers PlanService extension for plan formatting (same as Spark Connect)
/// - Configures default catalog and schema
/// - Enables information_schema for metadata queries
///
/// # Differences with sail-spark-connect
/// - **NO SessionManager**: Flight SQL uses simple per-request sessions
/// - **NO ActorSystem**: No need for multi-session orchestration
/// - **NO SparkSession extension**: No Spark-specific job tracking/streaming
///
/// # Shared components with sail-spark-connect
/// - `sail_plan`: PlanResolver, PlanConfig, execute_logical_plan
/// - `sail_session`: Optimizer rules, analyzer rules
/// - `sail_common_datafusion`: PlanService, catalog display
pub fn create_sail_session_context() -> SessionContext {
    // Create the PlanService extension required by the resolver
    let plan_service = PlanService::new(
        Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
        Box::new(SparkPlanFormatter),
    );

    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("sail", "default") // Match Spark default
        .with_extension(Arc::new(plan_service));

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(default_analyzer_rules())
        .with_optimizer_rules(default_optimizer_rules())
        .build();

    SessionContext::new_with_state(state)
}
