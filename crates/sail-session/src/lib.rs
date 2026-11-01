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
    use sail_common_datafusion::session::plan::PlanService;
    use sail_plan::catalog::SparkCatalogObjectDisplay;
    use sail_plan::config::PlanConfig;
    use sail_plan::formatter::SparkPlanFormatter;
    use sail_plan::resolver::PlanResolver;
    use sail_sql_analyzer::data_type::from_ast_data_type;
    use sail_sql_analyzer::parser::{parse_data_type, parse_one_statement};
    use sail_sql_analyzer::statement::from_ast_statement;
    use sail_system_store::catalog::SystemCatalog;

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

    // This test is defined in this crate because it needs the catalog manager and the plan resolver.
    #[tokio::test]
    async fn test_repeated_catalog_table_references_share_one_source()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use async_trait::async_trait;
        use datafusion::catalog::Session;
        use datafusion::datasource::empty::EmptyTable;
        use datafusion::datasource::provider_as_source;
        use datafusion::logical_expr::TableSource;
        use datafusion_common::{plan_datafusion_err, plan_err};
        use sail_common::spec;
        use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo};
        use sail_common_datafusion::extension::SessionExtensionAccessor;

        struct CountingDataSource {
            name: &'static str,
            reuse: bool,
            calls: AtomicUsize,
        }

        #[async_trait]
        impl DataSource for CountingDataSource {
            fn name(&self) -> &str {
                self.name
            }

            fn supports_source_reuse(&self) -> bool {
                self.reuse
            }

            async fn create_source(
                &self,
                _ctx: &dyn Session,
                info: SourceInfo,
            ) -> datafusion_common::Result<Arc<dyn TableSource>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let schema = info
                    .schema
                    .ok_or_else(|| plan_datafusion_err!("missing table schema"))?;
                Ok(provider_as_source(Arc::new(EmptyTable::new(Arc::new(
                    schema,
                )))))
            }

            async fn create_writer(
                &self,
                _ctx: &dyn Session,
                _info: SinkInfo,
            ) -> datafusion_common::Result<LogicalPlan> {
                plan_err!("the counting data source does not write")
            }
        }

        let session = create_session()?;
        let shared = Arc::new(CountingDataSource {
            name: "shared",
            reuse: true,
            calls: AtomicUsize::new(0),
        });
        let unshared = Arc::new(CountingDataSource {
            name: "unshared",
            reuse: false,
            calls: AtomicUsize::new(0),
        });
        let registry = session.extension::<DataSourceRegistry>()?;
        registry.register_data_source(Arc::clone(&shared) as Arc<dyn DataSource>)?;
        registry.register_data_source(Arc::clone(&unshared) as Arc<dyn DataSource>)?;
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        for sql in [
            "CREATE TABLE s (k BIGINT, v BIGINT) USING shared",
            "CREATE TABLE u (k BIGINT, v BIGINT) USING unshared",
            "CREATE VIEW sv AS SELECT * FROM s",
        ] {
            let plan = from_ast_statement(parse_one_statement(sql)?)?;
            let plan = resolver.resolve_named_plan(plan).await?.plan;
            session.execute_logical_plan(plan).await?.collect().await?;
        }
        let read = |name: &str, options: Vec<(String, String)>| {
            spec::QueryPlan::new(spec::QueryNode::Read {
                is_streaming: false,
                read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                    name: spec::ObjectName::bare(name),
                    temporal: None,
                    sample: None,
                    options,
                })),
            })
        };
        let union = |left: spec::QueryPlan, right: spec::QueryPlan| {
            spec::Plan::Query(spec::QueryPlan::new(spec::QueryNode::SetOperation(
                spec::SetOperation {
                    left: Box::new(left),
                    right: Box::new(right),
                    set_op_type: spec::SetOpType::Union,
                    is_all: true,
                    by_name: false,
                    allow_missing_columns: false,
                },
            )))
        };
        let count_calls = async |source: &CountingDataSource, plan: spec::Plan| {
            let before = source.calls.load(Ordering::SeqCst);
            resolver.resolve_named_plan(plan).await?;
            Ok::<_, Box<dyn std::error::Error>>(source.calls.load(Ordering::SeqCst) - before)
        };

        // References in subqueries and in views share the source.
        let sql = "SELECT (SELECT max(v) FROM s), (SELECT max(v) FROM s), (SELECT max(v) FROM sv)";
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        assert_eq!(count_calls(&shared, plan.clone()).await?, 1);
        // Each query creates its own source.
        assert_eq!(count_calls(&shared, plan).await?, 1);
        // References with different read options do not share the source.
        let plan = union(read("s", vec![]), read("s", vec![("k".into(), "v".into())]));
        assert_eq!(count_calls(&shared, plan).await?, 2);
        // Data sources that do not support reuse create a source for each reference.
        let plan = union(read("u", vec![]), read("u", vec![]));
        assert_eq!(count_calls(&unshared, plan).await?, 2);
        Ok(())
    }
}
