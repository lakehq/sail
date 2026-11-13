pub mod catalog;
pub mod error;
pub mod formats;
pub mod optimizer;
pub mod planner;
pub mod runtime;
mod scalar_subquery;
pub mod session_factory;
pub mod session_manager;
mod shared_cte;

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

    fn create_memory_catalog() -> Result<MemoryCatalogProvider, Box<dyn std::error::Error>> {
        Ok(MemoryCatalogProvider::new(
            "sail".to_string(),
            vec![Arc::from("default")].try_into()?,
            None,
        ))
    }

    fn create_session() -> Result<SessionContext, Box<dyn std::error::Error>> {
        create_session_with_catalog(Arc::new(create_memory_catalog()?))
    }

    fn create_session_with_catalog(
        catalog: Arc<dyn CatalogProvider>,
    ) -> Result<SessionContext, Box<dyn std::error::Error>> {
        let mut state = SessionStateBuilder::new()
            .with_query_planner(crate::planner::new_query_planner())
            .build();
        let catalog_manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([
                ("sail".to_string(), catalog),
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
        use datafusion::datasource::{TableProvider, provider_as_source};
        use datafusion::logical_expr::TableSource;
        use datafusion_common::{plan_datafusion_err, plan_err};
        use sail_common::spec;
        use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo};
        use sail_common_datafusion::extension::SessionExtensionAccessor;
        use sail_data_source::formats::python::{InProcessExecutor, PythonTableProvider};

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

            async fn create_source(
                &self,
                _ctx: &dyn Session,
                info: SourceInfo,
            ) -> datafusion_common::Result<Arc<dyn TableSource>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let schema = info
                    .schema
                    .ok_or_else(|| plan_datafusion_err!("missing table schema"))?;
                let schema = Arc::new(schema);
                // Python data sources are read once for each reference.
                let provider: Arc<dyn TableProvider> = if self.reuse {
                    Arc::new(EmptyTable::new(schema))
                } else {
                    Arc::new(PythonTableProvider::new(
                        Arc::new(InProcessExecutor::new()),
                        vec![],
                        schema,
                    ))
                };
                Ok(provider_as_source(provider))
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

    // This test is defined in this crate because it needs the catalog manager and the plan resolver.
    #[tokio::test]
    async fn test_catalog_table_references_share_one_source_regardless_of_property_order()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use async_trait::async_trait;
        use datafusion::catalog::Session;
        use datafusion::datasource::empty::EmptyTable;
        use datafusion::datasource::provider_as_source;
        use datafusion::logical_expr::TableSource;
        use datafusion_common::{plan_datafusion_err, plan_err};
        use sail_catalog::error::CatalogResult;
        use sail_catalog::provider::{
            AlterTableOptions, CreateDatabaseOptions, CreateTableMetadataRequirement,
            CreateTableOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions,
            DropViewOptions, Namespace,
        };
        use sail_common_datafusion::catalog::{DatabaseStatus, TableKind, TableStatus};
        use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo};
        use sail_common_datafusion::extension::SessionExtensionAccessor;

        /// A memory catalog that returns the properties of a table in a different order
        /// on each lookup, as catalogs that build them from unordered maps can do.
        struct PermutingCatalogProvider {
            inner: MemoryCatalogProvider,
            lookups: AtomicUsize,
        }

        #[async_trait]
        impl CatalogProvider for PermutingCatalogProvider {
            fn get_name(&self) -> &str {
                self.inner.get_name()
            }

            fn default_namespace(&self) -> Namespace {
                self.inner.default_namespace()
            }

            async fn create_database(
                &self,
                database: &Namespace,
                options: CreateDatabaseOptions,
            ) -> CatalogResult<DatabaseStatus> {
                self.inner.create_database(database, options).await
            }

            async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
                self.inner.get_database(database).await
            }

            async fn list_databases(
                &self,
                prefix: Option<&Namespace>,
            ) -> CatalogResult<Vec<DatabaseStatus>> {
                self.inner.list_databases(prefix).await
            }

            async fn drop_database(
                &self,
                database: &Namespace,
                options: DropDatabaseOptions,
            ) -> CatalogResult<()> {
                self.inner.drop_database(database, options).await
            }

            async fn create_table(
                &self,
                database: &Namespace,
                table: &str,
                options: CreateTableOptions,
            ) -> CatalogResult<TableStatus> {
                self.inner.create_table(database, table, options).await
            }

            fn create_table_metadata_requirement(
                &self,
                options: &CreateTableOptions,
            ) -> CatalogResult<CreateTableMetadataRequirement> {
                self.inner.create_table_metadata_requirement(options)
            }

            async fn get_table(
                &self,
                database: &Namespace,
                table: &str,
            ) -> CatalogResult<TableStatus> {
                let mut status = self.inner.get_table(database, table).await?;
                if let TableKind::Table { properties, .. } = &mut status.kind
                    && !properties.is_empty()
                {
                    let lookup = self.lookups.fetch_add(1, Ordering::SeqCst);
                    let n = properties.len();
                    properties.rotate_left(lookup % n);
                }
                Ok(status)
            }

            async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
                self.inner.list_tables(database).await
            }

            async fn drop_table(
                &self,
                database: &Namespace,
                table: &str,
                options: DropTableOptions,
            ) -> CatalogResult<()> {
                self.inner.drop_table(database, table, options).await
            }

            async fn alter_table(
                &self,
                database: &Namespace,
                table: &str,
                options: AlterTableOptions,
            ) -> CatalogResult<()> {
                self.inner.alter_table(database, table, options).await
            }

            async fn create_view(
                &self,
                database: &Namespace,
                view: &str,
                options: CreateViewOptions,
            ) -> CatalogResult<TableStatus> {
                self.inner.create_view(database, view, options).await
            }

            async fn get_view(
                &self,
                database: &Namespace,
                view: &str,
            ) -> CatalogResult<TableStatus> {
                self.inner.get_view(database, view).await
            }

            async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
                self.inner.list_views(database).await
            }

            async fn drop_view(
                &self,
                database: &Namespace,
                view: &str,
                options: DropViewOptions,
            ) -> CatalogResult<()> {
                self.inner.drop_view(database, view, options).await
            }
        }

        struct CountingDataSource {
            calls: AtomicUsize,
        }

        #[async_trait]
        impl DataSource for CountingDataSource {
            fn name(&self) -> &str {
                "counting"
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

        let catalog = Arc::new(PermutingCatalogProvider {
            inner: create_memory_catalog()?,
            lookups: AtomicUsize::new(0),
        });
        let session =
            create_session_with_catalog(Arc::clone(&catalog) as Arc<dyn CatalogProvider>)?;
        let source = Arc::new(CountingDataSource {
            calls: AtomicUsize::new(0),
        });
        let registry = session.extension::<DataSourceRegistry>()?;
        registry.register_data_source(Arc::clone(&source) as Arc<dyn DataSource>)?;
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        // Seven properties, so that the lookups of one query see different orders.
        let sql = "CREATE TABLE t (k BIGINT, v BIGINT) USING counting TBLPROPERTIES \
            ('p0' = '0', 'p1' = '1', 'p2' = '2', 'p3' = '3', 'p4' = '4', 'p5' = '5', 'p6' = '6')";
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        let plan = resolver.resolve_named_plan(plan).await?.plan;
        session.execute_logical_plan(plan).await?.collect().await?;

        let sql = "SELECT (SELECT max(v) FROM t), (SELECT max(v) FROM t)";
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        let lookups = catalog.lookups.load(Ordering::SeqCst);
        resolver.resolve_named_plan(plan).await?;
        let lookups = catalog.lookups.load(Ordering::SeqCst) - lookups;
        // Each reference looks the table up, and the lookups return different orders.
        assert!(
            (2..7).contains(&lookups),
            "unexpected lookup count {lookups}"
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_cte_references_share_without_spilling() -> Result<(), Box<dyn std::error::Error>>
    {
        use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use sail_common_datafusion::session::shared_cte::SharedCteConfig;
        use sail_plan::shared_cte::{
            CteReference, optimize_and_plan_shared_ctes, session_state_for_restart,
        };

        // A memory limit without temporary files lets only the CTEs that must be evaluated
        // once be shared: the volatile CTEs and the CTEs that refer to other shared CTEs.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(64 << 20, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let state = SessionStateBuilder::new_from_existing(create_session()?.state())
            .with_runtime_env(runtime)
            .build();
        let session = SessionContext::new_with_state(state);
        session
            .state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(SharedCteConfig::new(true)));
        let cte = |body: &str| {
            format!("c AS (SELECT a, {body} AS b FROM VALUES (1), (2) AS t(a) GROUP BY a)")
        };
        for (query, expected) in [
            (
                format!(
                    "WITH {} SELECT x.b FROM c x JOIN c y ON x.a = y.a",
                    cte("count(*)")
                ),
                0,
            ),
            (
                format!(
                    "WITH {} SELECT x.b FROM c x JOIN c y ON x.a = y.a",
                    cte("rand()")
                ),
                2,
            ),
            // The second CTE refers to the first one twice, and is referenced twice.
            (
                format!(
                    "WITH {}, d AS (SELECT x.a, x.b + y.b AS b FROM c x JOIN c y ON x.a = y.a) \
                     SELECT x.b FROM d x JOIN d y ON x.a = y.a",
                    cte("count(*)")
                ),
                2,
            ),
        ] {
            let resolver =
                PlanResolver::new(&session, Arc::new(PlanConfig::default())).with_shared_ctes();
            let plan = from_ast_statement(parse_one_statement(&query)?)?;
            let plan = resolver.resolve_named_plan(plan).await?.plan;
            let state = session.state();
            let mut restart = false;
            let plan = optimize_and_plan_shared_ctes(
                plan,
                &mut |plan, query| {
                    if query && std::mem::replace(&mut restart, true) {
                        session_state_for_restart(&state).optimize(&plan)
                    } else {
                        state.optimize(&plan)
                    }
                },
                |plan| async {
                    state
                        .query_planner()
                        .create_physical_plan(&plan, &state)
                        .await?;
                    Ok(plan)
                },
            )
            .await?;
            let mut references = 0;
            plan.apply_with_subqueries(|plan| {
                if let LogicalPlan::TableScan(scan) = plan
                    && CteReference::try_from_scan(scan).is_some_and(|x| x.body().is_some())
                {
                    references += 1;
                }
                Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
            })?;
            assert_eq!(references, expected, "{query}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cte_references_in_a_nested_loop_join_build_side_with_spilling()
    -> Result<(), Box<dyn std::error::Error>> {
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use sail_common_datafusion::session::shared_cte::SharedCteConfig;
        use sail_plan::shared_cte::{
            CteReference, optimize_and_plan_shared_ctes, session_state_for_restart,
        };

        // With a memory limit and temporary files, a nested loop join can execute its build
        // side again, so each reference evaluates a CTE that it reads there, unless the CTE
        // must be evaluated once.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(64 << 20, 1.0)
            .build_arc()?;
        let state = SessionStateBuilder::new_from_existing(create_session()?.state())
            .with_runtime_env(runtime)
            .build();
        let session = SessionContext::new_with_state(state);
        session
            .state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(SharedCteConfig::new(true)));
        let cte = |body: &str| {
            format!("c AS (SELECT a, {body} AS b FROM VALUES (1), (2) AS t(a) GROUP BY a)")
        };
        for (query, expected) in [
            (
                format!(
                    "WITH {} SELECT x.b FROM c x JOIN c y ON x.a < y.a",
                    cte("count(*)")
                ),
                0,
            ),
            (
                format!(
                    "WITH {} SELECT x.b FROM c x JOIN c y ON x.a < y.a",
                    cte("rand()")
                ),
                2,
            ),
            // The references in a hash join share the CTE.
            (
                format!(
                    "WITH {} SELECT x.b FROM c x JOIN c y ON x.a = y.a",
                    cte("count(*)")
                ),
                2,
            ),
        ] {
            let resolver =
                PlanResolver::new(&session, Arc::new(PlanConfig::default())).with_shared_ctes();
            let plan = from_ast_statement(parse_one_statement(&query)?)?;
            let plan = resolver.resolve_named_plan(plan).await?.plan;
            let state = session.state();
            let mut restart = false;
            let plan = optimize_and_plan_shared_ctes(
                plan,
                &mut |plan, query| {
                    if query && std::mem::replace(&mut restart, true) {
                        session_state_for_restart(&state).optimize(&plan)
                    } else {
                        state.optimize(&plan)
                    }
                },
                |plan| async {
                    state
                        .query_planner()
                        .create_physical_plan(&plan, &state)
                        .await?;
                    Ok(plan)
                },
            )
            .await?;
            let mut references = 0;
            plan.apply_with_subqueries(|plan| {
                if let LogicalPlan::TableScan(scan) = plan
                    && CteReference::try_from_scan(scan).is_some_and(|x| x.body().is_some())
                {
                    references += 1;
                }
                Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
            })?;
            assert_eq!(references, expected, "{query}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cte_references_share_or_inline() -> Result<(), Box<dyn std::error::Error>> {
        use sail_common_datafusion::session::shared_cte::SharedCteConfig;
        use sail_plan::shared_cte::{
            CteReference, optimize_and_plan_shared_ctes, session_state_for_restart,
        };

        // Returns the optimized plan, which is planned as well, so that the query planner
        // decides which bodies its references share.
        async fn optimize(
            query: &str,
            share: bool,
        ) -> Result<LogicalPlan, Box<dyn std::error::Error>> {
            let session = create_session()?;
            session
                .state_ref()
                .write()
                .config_mut()
                .set_extension(Arc::new(SharedCteConfig::new(share)));
            let resolver =
                PlanResolver::new(&session, Arc::new(PlanConfig::default())).with_shared_ctes();
            let plan = from_ast_statement(parse_one_statement(query)?)?;
            let plan = resolver.resolve_named_plan(plan).await?.plan;
            let state = session.state();
            let mut restart = false;
            Ok(optimize_and_plan_shared_ctes(
                plan,
                &mut |plan, query| {
                    if query && std::mem::replace(&mut restart, true) {
                        session_state_for_restart(&state).optimize(&plan)
                    } else {
                        state.optimize(&plan)
                    }
                },
                |plan| async {
                    // The shared bodies are planned before the query, which the test session
                    // cannot plan when it has an uncorrelated scalar subquery.
                    let _ = state
                        .query_planner()
                        .create_physical_plan(&plan, &state)
                        .await;
                    Ok(plan)
                },
            )
            .await?)
        }

        // Returns the number of references that share a body and the number of bodies.
        fn shared(plan: &LogicalPlan) -> datafusion_common::Result<(usize, usize)> {
            let mut references = 0;
            let mut bodies = std::collections::HashSet::new();
            plan.apply_with_subqueries(|plan| {
                if let LogicalPlan::TableScan(scan) = plan
                    && let Some(body) = CteReference::try_from_scan(scan).and_then(|x| x.body())
                {
                    references += 1;
                    bodies.insert(Arc::as_ptr(body));
                }
                Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
            })?;
            Ok((references, bodies.len()))
        }

        let cte =
            "WITH c AS (SELECT a, count(*) AS b FROM VALUES (1), (2), (3) AS t(a) GROUP BY a)";
        for (query, expected) in [
            // The references read the same rows.
            ("SELECT x.a, y.b FROM c x JOIN c y ON x.a = y.a", (2, 1)),
            (
                "SELECT a FROM c WHERE a > 1 UNION ALL SELECT b FROM c WHERE a > 1",
                (2, 1),
            ),
            // A limit above an aggregate reads all the rows of the references.
            (
                "(SELECT max(a) FROM c) UNION ALL (SELECT max(b) FROM c) LIMIT 1",
                (2, 1),
            ),
            ("SELECT a, (SELECT max(b) FROM c) FROM c", (2, 1)),
            // The references read different rows.
            (
                "SELECT a FROM c WHERE a > 1 UNION ALL SELECT b FROM c WHERE a > 2",
                (0, 0),
            ),
            (
                "SELECT a FROM c WHERE a > 1 UNION ALL SELECT b FROM c",
                (0, 0),
            ),
            // A limit can stop reading a reference early.
            (
                "(SELECT a FROM c LIMIT 2) UNION ALL (SELECT b FROM c LIMIT 2)",
                (0, 0),
            ),
            (
                "(SELECT a FROM c WHERE a > 1 LIMIT 2) UNION ALL (SELECT b FROM c WHERE a > 1 LIMIT 2)",
                (0, 0),
            ),
            ("SELECT x.a FROM c x JOIN c y ON x.a = y.a LIMIT 1", (0, 0)),
            // Two of the three references read the same rows.
            (
                "SELECT a FROM c WHERE a > 1 UNION ALL SELECT b FROM c WHERE a > 1 UNION ALL SELECT b FROM c",
                (2, 1),
            ),
            // A filter with a scalar subquery is not recorded for the reference.
            (
                "SELECT x.a FROM c x JOIN c y ON x.a = y.a WHERE x.a > (SELECT 1) AND y.a > (SELECT 1)",
                (0, 0),
            ),
            // The CTE is referenced once.
            ("SELECT a FROM c", (0, 0)),
            ("SELECT a FROM c WHERE b > 0 ORDER BY b DESC", (0, 0)),
            ("SELECT a % 2 FROM c GROUP BY 1 HAVING max(b) > 0", (0, 0)),
        ] {
            let query = format!("{cte} {query}");
            let plan = optimize(&query, true).await?;
            assert_eq!(
                shared(&plan)?,
                expected,
                "{query}\n{}",
                plan.display_indent()
            );
            if expected == (0, 0) {
                let inlined = optimize(&query, false).await?;
                assert_eq!(
                    plan.display_indent().to_string(),
                    inlined.display_indent().to_string(),
                    "{query}"
                );
            }
        }

        // A resolver that does not share CTEs resolves no reference as a scan.
        let session = create_session()?;
        session
            .state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(SharedCteConfig::new(true)));
        let resolver = PlanResolver::new(&session, Arc::new(PlanConfig::default()));
        let plan = from_ast_statement(parse_one_statement(&format!(
            "{cte} SELECT x.a, y.b FROM c x JOIN c y ON x.a = y.a"
        ))?)?;
        let plan = resolver.resolve_named_plan(plan).await?.plan;
        assert!(!plan.display_indent().to_string().contains("TableScan"));

        // Each reference evaluates a CTE that is sorted, refers to outer columns, only
        // projects and filters one source, or is repartitioned explicitly.
        for query in [
            "WITH c AS (SELECT a FROM (SELECT /*+ REPARTITION(2) */ a FROM VALUES (1), (2) AS t(a)) GROUP BY a) SELECT x.a FROM c x JOIN c y ON x.a = y.a",
            "WITH c AS (SELECT a, a + 1 AS b FROM VALUES (1), (2) AS t(a) WHERE a > 0) SELECT x.b FROM c x JOIN c y ON x.a = y.a",
            "WITH c AS (SELECT a FROM VALUES (1), (2) AS t(a) GROUP BY a ORDER BY a) SELECT x.a FROM c x JOIN c y ON x.a = y.a",
            "SELECT (WITH c AS (SELECT a + t.b AS a FROM VALUES (1), (2) AS s(a)) SELECT max(x.a) FROM c x JOIN c y ON x.a = y.a) FROM VALUES (1) AS t(b)",
        ] {
            let plan = optimize(query, true).await?;
            assert_eq!(shared(&plan)?, (0, 0), "{query}");
        }

        // The references to a volatile CTE share one evaluation, even if they read different
        // rows, read through a limit, or the CTE is sorted or only projects one source. A
        // CTE that refers to a volatile CTE in place is volatile as well.
        let volatile = "WITH v AS (SELECT a, random() AS r FROM VALUES (1), (2) AS t(a))";
        for query in [
            "SELECT x.r FROM v x JOIN v y ON x.a = y.a",
            "SELECT r FROM v WHERE a > 1 UNION ALL SELECT r FROM v WHERE r > 0.5",
            "(SELECT r FROM v LIMIT 1) UNION ALL (SELECT r FROM v)",
            "SELECT DISTINCT a FROM v UNION ALL SELECT DISTINCT a FROM v",
            "SELECT spark_partition_id(), x.r FROM v x JOIN v y ON x.a = y.a",
            ", d AS (SELECT a, r FROM v WHERE a > 0) SELECT x.r FROM d x JOIN d y ON x.a = y.a WHERE x.a = 1",
            ", s AS (SELECT a, r FROM v ORDER BY r) SELECT x.r FROM s x JOIN s y ON x.a = y.a",
            "SELECT a, r FROM v EXCEPT ALL SELECT a, r FROM v",
            "SELECT a, r FROM v INTERSECT SELECT a, r FROM v",
        ] {
            let query = format!("{volatile} {query}");
            let plan = optimize(&query, true).await?;
            assert_eq!(shared(&plan)?, (2, 1), "{query}\n{}", plan.display_indent());
        }
        // A volatile CTE that is referenced once is evaluated in place.
        for query in [
            "SELECT a FROM v ORDER BY r",
            "SELECT a % 2 FROM v GROUP BY 1 HAVING max(r) >= 0",
            ", d AS (SELECT a, r FROM v WHERE a > 0) SELECT a FROM d ORDER BY r",
        ] {
            let query = format!("{volatile} {query}");
            let plan = optimize(&query, true).await?;
            assert_eq!(shared(&plan)?, (0, 0), "{query}");
            assert_eq!(
                plan.display_indent().to_string(),
                optimize(&query, false).await?.display_indent().to_string(),
                "{query}"
            );
        }
        // The shared body of a volatile CTE skips the rows that no reference reads, and
        // each reference keeps its filter.
        let query =
            format!("{volatile} SELECT r FROM v WHERE a > 1 UNION ALL SELECT r FROM v WHERE a < 0");
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (2, 1), "{}", plan.display_indent());
        let mut bodies = vec![];
        let mut filtered = 0;
        plan.apply_with_subqueries(|plan| {
            match plan {
                LogicalPlan::Filter(filter)
                    if matches!(filter.input.as_ref(), LogicalPlan::TableScan(scan)
                        if CteReference::try_from_scan(scan).is_some()) =>
                {
                    filtered += 1;
                }
                LogicalPlan::TableScan(scan) => {
                    if let Some(body) = CteReference::try_from_scan(scan).and_then(|x| x.body()) {
                        bodies.push(body.plan().display_indent().to_string());
                    }
                }
                _ => {}
            }
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        })?;
        assert_eq!(filtered, 2, "{}", plan.display_indent());
        assert!(
            bodies[0].contains("> Int32(1) OR ") && bodies[0].contains(" < Int32(0)"),
            "{}",
            bodies[0]
        );
        // A reference that reads all the rows reads them from the shared body as well.
        let query = format!("{volatile} SELECT r FROM v WHERE a > 1 UNION ALL SELECT r FROM v");
        let plan = optimize(&query, true).await?;
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(body) = CteReference::try_from_scan(scan).and_then(|x| x.body())
            {
                assert!(
                    !body.plan().display_indent().to_string().contains("Filter"),
                    "{}",
                    body.plan().display_indent()
                );
            }
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        })?;
        // A call of an external service does not make a CTE volatile, but it does not keep
        // a volatile CTE from being shared.
        for (expr, expected) in [
            ("jev_models()", (0, 0)),
            ("struct(jev_models(), rand())", (2, 1)),
        ] {
            let query = format!(
                "WITH v AS (SELECT a, {expr} AS r FROM VALUES (1), (2) AS t(a) GROUP BY a) \
                 SELECT x.r FROM v x JOIN v y ON x.a = y.a"
            );
            let plan = optimize(&query, true).await?;
            assert_eq!(
                shared(&plan)?,
                expected,
                "{query}\n{}",
                plan.display_indent()
            );
        }
        // A CTE is volatile if its output depends on the partitions of its input.
        for expr in [
            "rand(7)",
            "monotonically_increasing_id()",
            "spark_partition_id()",
        ] {
            let query = format!(
                "WITH v AS (SELECT a, {expr} AS r FROM (SELECT /*+ REPARTITION(2) */ a FROM VALUES (1), (2) AS t(a))) \
                 SELECT x.r FROM v x JOIN v y ON x.a = y.a"
            );
            let plan = optimize(&query, true).await?;
            assert_eq!(shared(&plan)?, (2, 1), "{query}\n{}", plan.display_indent());
        }
        let query = format!(
            "{volatile}, c AS (SELECT a FROM VALUES (1), (2) AS t(a) GROUP BY a) \
             SELECT x.r FROM v x JOIN v y ON x.a = y.a JOIN c p ON x.a = p.a JOIN c q ON x.a = q.a"
        );
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (4, 2), "{}", plan.display_indent());
        // A query that reads the partitions of its input inlines the other CTEs.
        let query = format!("{query} WHERE spark_partition_id() = 0");
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (2, 1), "{}", plan.display_indent());

        // Each reference evaluates a CTE whose body returns rows before its input ends, as a
        // join, since the body would keep its rows for a reference that reads it later.
        let join = "WITH j AS (SELECT x.a, y.b FROM VALUES (1), (2) AS x(a) \
                    JOIN VALUES (1, 5), (1, 6) AS y(a, b) ON x.a = y.a)";
        {
            let query = format!("{join} SELECT p.a, q.b FROM j p JOIN j q ON p.a = q.a");
            let plan = optimize(&query, true).await?;
            assert_eq!(shared(&plan)?, (0, 0), "{query}\n{}", plan.display_indent());
            assert_eq!(
                plan.display_indent().to_string(),
                optimize(&query, false).await?.display_indent().to_string(),
                "{query}"
            );
        }
        // Each reference computes the deterministic expressions of the top projection of the
        // definition, after the filters above it, while the shared body computes the rest.
        let query = "WITH c AS (SELECT a, 10 / (count(*) - 2) AS b FROM VALUES (1), (2), (2) AS t(a) GROUP BY a) \
                     SELECT x.b FROM c x JOIN c y ON x.a = y.a WHERE x.a < 2";
        let plan = optimize(query, true).await?;
        assert_eq!(shared(&plan)?, (2, 1), "{}", plan.display_indent());
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(body) = CteReference::try_from_scan(scan).and_then(|x| x.body())
            {
                let body = body.plan().display_indent().to_string();
                assert!(!body.contains("Division by zero"), "{body}");
            }
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        })?;
        assert!(
            plan.display_indent()
                .to_string()
                .contains("Division by zero")
        );
        // A reference that is sorted with a limit evaluates the definition itself, so that
        // the optimizer can limit the groups of an aggregate in it.
        let query = format!(
            "{cte} (SELECT a FROM c ORDER BY a LIMIT 1) UNION ALL (SELECT b FROM c ORDER BY a LIMIT 1)"
        );
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (0, 0), "{}", plan.display_indent());
        // A sort with a limit above a join reads the references in full.
        let query = format!("{cte} SELECT x.a FROM c x JOIN c y ON x.a = y.a ORDER BY 1 LIMIT 1");
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (2, 1), "{}", plan.display_indent());

        // A sorted, distinct, windowed or intersected body keeps no more than it holds.
        for body in [
            "SELECT DISTINCT a FROM VALUES (1), (2) AS t(a)",
            "SELECT a, rank() OVER (PARTITION BY a % 2 ORDER BY a) AS b FROM VALUES (1), (2) AS t(a)",
            "SELECT a FROM VALUES (1), (2) AS t(a) INTERSECT SELECT a FROM VALUES (2), (3) AS t(a)",
            "SELECT a FROM VALUES (1), (2) AS t(a) EXCEPT ALL SELECT a FROM VALUES (2), (3) AS t(a)",
        ] {
            let query = format!("WITH s AS ({body}) SELECT x.* FROM s x JOIN s y ON x.a = y.a");
            let plan = optimize(&query, true).await?;
            assert_eq!(shared(&plan)?, (2, 1), "{query}\n{}", plan.display_indent());
        }
        let query = format!(
            "{join} SELECT t.a FROM VALUES (1), (2) AS t(a) \
             WHERE t.a IN (SELECT a FROM j) AND t.a + 1 IN (SELECT a + 1 FROM j)"
        );
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (0, 0), "{}", plan.display_indent());
        assert_eq!(
            plan.display_indent().to_string(),
            optimize(&query, false).await?.display_indent().to_string()
        );

        // A consumer that removes duplicate rows turns the join into a semi join as well.
        let query = format!("{join} SELECT DISTINCT a FROM j UNION ALL SELECT DISTINCT a FROM j");
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (0, 0), "{}", plan.display_indent());
        assert_eq!(
            plan.display_indent().to_string(),
            optimize(&query, false).await?.display_indent().to_string()
        );

        // The references to a join read it themselves, those to another definition read
        // different rows, and a third definition is shared.
        let query = "WITH d AS (SELECT * FROM VALUES (1, 10L, 'a'), (1, 11L, 'b'), (3, 30L, 'c') AS x(k, v, s)), \
             a AS (SELECT x.k, x.v, y.s FROM d x JOIN d y ON x.v = y.v), \
             b AS (SELECT k, count(*) c FROM d GROUP BY k), \
             c AS (SELECT s, sum(v) t FROM d GROUP BY s) \
             SELECT k, v FROM a WHERE k = 1 UNION ALL SELECT k, v FROM a WHERE k = 1 \
             UNION ALL SELECT k, v FROM a WHERE k = 3 \
             UNION ALL SELECT k, c FROM b UNION ALL SELECT k, c FROM b WHERE c > 1 \
             UNION ALL SELECT x.t, y.t FROM c x, c y WHERE x.t = y.t";
        let plan = optimize(query, true).await?;
        assert_eq!(shared(&plan)?, (2, 1), "{}", plan.display_indent());

        // A CTE that refers twice to a shared CTE is shared as well.
        let query = format!(
            "{cte}, d AS (SELECT x.a, y.b FROM c x JOIN c y ON x.a = y.a) \
             SELECT p.a, q.b FROM d p JOIN d q ON p.a = q.a"
        );
        let plan = optimize(&query, true).await?;
        assert_eq!(shared(&plan)?, (2, 1));
        let mut nested = (0, 0);
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(body) = CteReference::try_from_scan(scan).and_then(|x| x.body())
            {
                nested = shared(body.plan())?;
            }
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        })?;
        assert_eq!(nested, (2, 1));
        Ok(())
    }
}
