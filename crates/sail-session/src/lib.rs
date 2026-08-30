pub mod catalog;
pub mod error;
pub mod formats;
pub mod optimizer;
pub mod planner;
pub mod runtime;
pub mod session_factory;
pub mod session_manager;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
    use sail_catalog::provider::CatalogProvider;
    use sail_catalog_memory::MemoryCatalogProvider;
    use sail_catalog_system::physical_plan::SystemTableExec;
    use sail_catalog_system::predicate::PredicateExtractor;
    use sail_catalog_system::{SYSTEM_CATALOG_NAME, SystemCatalogProvider};
    use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
    use sail_common_datafusion::datasource::DataSourceRegistry;
    use sail_common_datafusion::lakeformat::LakeFormatRegistryBuilder;
    use sail_common_datafusion::session::plan::PlanService;
    use sail_common_datafusion::system::catalog::SystemCatalog;
    use sail_plan::catalog::SparkCatalogObjectDisplay;
    use sail_plan::config::PlanConfig;
    use sail_plan::formatter::SparkPlanFormatter;
    use sail_plan::resolver::PlanResolver;
    use sail_sql_analyzer::data_type::from_ast_data_type;
    use sail_sql_analyzer::parser::{parse_data_type, parse_one_statement};
    use sail_sql_analyzer::statement::from_ast_statement;

    fn create_session() -> Result<SessionContext, Box<dyn std::error::Error>> {
        let mut state = SessionStateBuilder::new()
            .with_query_planner(crate::planner::new_query_planner())
            .build();
        let catalog_manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([
                (
                    "sail".to_string(),
                    Arc::new(MemoryCatalogProvider::new(
                        "sail".to_string(),
                        vec![Arc::from("default")].try_into()?,
                        None,
                    )) as Arc<dyn CatalogProvider>,
                ),
                (
                    SYSTEM_CATALOG_NAME.to_string(),
                    Arc::new(SystemCatalogProvider) as Arc<dyn CatalogProvider>,
                ),
            ]),
            default_catalog: "sail".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })?;
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        state.config_mut().set_extension(Arc::new(catalog_manager));
        state.config_mut().set_extension(Arc::new(plan_service));
        state
            .config_mut()
            .set_extension(Arc::new(DataSourceRegistry::new()));
        state
            .config_mut()
            .set_extension(Arc::new(LakeFormatRegistryBuilder::new().build()));
        Ok(SessionContext::new_with_state(state))
    }

    // This test is defined in this crate so that `sail-catalog-system` or `sail-common-datafusion`
    // does not need to depend on `sail-plan` or `sail-sql-analyzer`.
    #[test]
    fn test_system_table_schema_validity() -> Result<(), Box<dyn std::error::Error>> {
        let session = create_session()?;
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        for db in SystemCatalog::databases() {
            for t in db.tables() {
                let columns = t.columns();
                for col in columns.iter() {
                    // In the table definition YAML file, we have a SQL string for the data type
                    // of each column (for documentation purposes). Here we ensure that the SQL
                    // data type matches the Arrow data type used for table row struct serde.
                    let data_type = parse_data_type(col.sql_type).and_then(from_ast_data_type)?;
                    let data_type = resolver.resolve_data_type_for_plan(&data_type)?;
                    assert_eq!(data_type, col.arrow_type);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_system_metric_attribute_predicate_extraction_from_sql()
    -> Result<(), Box<dyn std::error::Error>> {
        fn find_system_table_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&SystemTableExec> {
            plan.downcast_ref::<SystemTableExec>()
                .or_else(|| plan.children().into_iter().find_map(find_system_table_exec))
        }

        let session = create_session()?;
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));

        for (predicate, values) in [
            (
                "attributes['execution.job.id'] = 'job-1'",
                vec![("job-1", true), ("job-2", false)],
            ),
            (
                "attributes['execution.job.id'] IN ('job-1', 'job-2')",
                vec![("job-1", true), ("job-2", true), ("job-3", false)],
            ),
        ] {
            let plan = from_ast_statement(parse_one_statement(&format!(
                "SELECT * FROM system.telemetry.metrics WHERE {predicate}"
            ))?)?;
            let plan = resolver.resolve_named_plan(plan).await?.plan;
            let LogicalPlan::Projection(ref projection) = plan else {
                return Err("projection plan expected".into());
            };
            let LogicalPlan::Filter(_) = projection.input.as_ref() else {
                return Err("filter plan expected".into());
            };
            let dataframe = session.execute_logical_plan(plan).await?;
            let (state, plan) = dataframe.into_parts();
            let plan = state.optimize(&plan)?;
            let physical_plan = state
                .query_planner()
                .create_physical_plan(&plan, &state)
                .await?;
            let filters = find_system_table_exec(&physical_plan)
                .ok_or("system table execution plan expected")?
                .filters()
                .to_vec();
            let mut extractor = PredicateExtractor::new(filters);
            let filters = extractor.extract_map_values::<String>("attributes")?;
            assert_eq!(filters.len(), 1);
            let filter = &filters[0];
            assert_eq!(filter.key, "execution.job.id");
            for (value, expected) in values {
                assert_eq!((filter.predicate)(&value.to_string())?, expected);
            }
            extractor.finalize()?;
        }

        Ok(())
    }
}
