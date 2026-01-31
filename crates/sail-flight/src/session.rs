use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::CatalogProvider;
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_session::formats::create_table_format_registry;
use sail_session::optimizer::{default_analyzer_rules, default_optimizer_rules};
use sail_session::planner::new_query_planner;

/// Creates a SessionContext configured with Sail's optimizers and analyzers.
///
/// This function reuses the **same Sail crates** that `sail-spark-connect` uses
/// (sail-plan, sail-session, sail-common-datafusion), but with a simpler session model
/// suitable for Flight SQL (no multi-session manager, no job runner).
///
/// Key features:
/// - Uses Sail's custom analyzers and optimizers (shared with Spark Connect)
/// - Registers PlanService extension for plan formatting (same as Spark Connect)
/// - Registers CatalogManager for table creation and querying
/// - Registers TableFormatRegistry for file format support (parquet, csv, etc.)
/// - Uses custom query planner for extension nodes
///
/// # Differences with sail-spark-connect
/// - **NO SessionManager**: Flight SQL uses simple per-request sessions
/// - **NO ActorSystem**: No need for multi-session orchestration
/// - **NO SparkSession extension**: No Spark-specific job tracking/streaming
/// - **NO JobRunner**: No distributed execution support (yet)
///
/// # Shared components with sail-spark-connect
/// - `sail_plan`: PlanResolver, PlanConfig, execute_logical_plan, query_planner
/// - `sail_session`: Optimizer rules, analyzer rules, table format registry
/// - `sail_common_datafusion`: PlanService, catalog display
///
/// # Future improvements
/// TODO: Integrate with SessionManager from sail-session for multi-session support.
/// This would allow per-connection session isolation, similar to Spark Connect.
/// See: sail_session::session_manager::SessionManager
pub fn create_sail_session_context() -> SessionContext {
    // Create the PlanService extension required by the resolver
    let plan_service = PlanService::new(
        Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
        Box::new(SparkPlanFormatter),
    );

    // Create a memory catalog manager for table storage
    let catalog_manager =
        create_memory_catalog_manager().expect("Failed to create catalog manager");

    // Create table format registry for file format support
    let table_format_registry =
        create_table_format_registry().expect("Failed to create table format registry");

    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(false)
        .with_extension(Arc::new(plan_service))
        .with_extension(Arc::new(catalog_manager))
        .with_extension(table_format_registry);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(default_analyzer_rules())
        .with_optimizer_rules(default_optimizer_rules())
        .with_query_planner(new_query_planner())
        .build();

    SessionContext::new_with_state(state)
}

fn create_memory_catalog_manager() -> Result<CatalogManager, Box<dyn std::error::Error>> {
    let catalog_name = "sail";
    let default_database = "default";

    // Create a memory catalog provider
    let provider = MemoryCatalogProvider::new(
        catalog_name.to_string(),
        vec![default_database.to_string()].try_into()?,
        None,
    );

    let mut catalogs: HashMap<String, Arc<dyn CatalogProvider>> = HashMap::new();
    catalogs.insert(catalog_name.to_string(), Arc::new(provider));

    let options = CatalogManagerOptions {
        catalogs,
        default_catalog: catalog_name.to_string(),
        default_database: vec![default_database.to_string()],
        global_temporary_database: vec!["global_temp".to_string()],
    };

    Ok(CatalogManager::new(options)?)
}
