// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, TableSource};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_common::plan_datafusion_err;
use futures::future::BoxFuture;
use sail_catalog::error::CatalogResult;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions,
    CreateTableMetadataRequirement, CreateTableOptions, CreateViewOptions, DropDatabaseOptions,
    DropTableOptions, DropViewOptions, Namespace,
};
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common::spec;
use sail_common::spec::CreateTableMode;
use sail_common_datafusion::catalog::{
    DatabaseStatus, LakehouseExecutionContext, LakehouseOperation, TableKind, TableStatus,
};
use sail_common_datafusion::datasource::{
    DataSource, DataSourceRegistry, OptionLayer, SinkInfo, SourceInfo,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::{LakeSource, RowLevelOperation};
use sail_common_datafusion::session::shared_cte::SharedCteConfig;
use sail_plan::config::PlanConfig;
use sail_plan::resolver::PlanResolver;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;

use super::{create_memory_catalog, create_session_with_catalogs, plan_shared_ctes};

mod delta_snapshot;
mod temporary_view;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const SNAPSHOT: &str = "snapshot";

/// A memory catalog that counts the table loads and returns a different `snapshot`
/// property from each load, as a catalog does when the table changes between loads.
struct SnapshotCatalogProvider {
    inner: MemoryCatalogProvider,
    names: TableNames,
    loads: AtomicUsize,
    /// The table and view requests, whether they found something or not.
    requests: Mutex<Vec<String>>,
    /// Whether each session has its own catalog of this name, as the memory catalog.
    session_local: bool,
    /// Table names and what to do once when each table is requested, such as a commit to
    /// another table in the middle of a statement.
    on_request: Mutex<Vec<(String, BoxFuture<'static, ()>)>>,
}

/// How a catalog finds a table and which name it returns for the table.
#[derive(Clone, Copy)]
enum TableNames {
    /// Table names match exactly.
    Exact,
    /// The catalog ignores the letter case of table names and returns the name that
    /// the table was created with, as the Hive Metastore does.
    IgnoreCase,
    /// The catalog ignores the letter case of table names and returns the name that
    /// it was asked for.
    IgnoreCaseAndEcho,
    /// Table and view names match exactly, and the catalog returns them in lowercase.
    ReturnLowercase,
    /// A database name of one part is in the database `main`, and the catalog returns
    /// the database with `main`, as Unity Catalog does with its default catalog.
    DefaultMain,
}

impl SnapshotCatalogProvider {
    fn new(inner: MemoryCatalogProvider, names: TableNames) -> Self {
        Self {
            inner,
            names,
            loads: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            session_local: false,
            on_request: Mutex::new(vec![]),
        }
    }

    fn record(&self, kind: &str, name: &str) {
        if let Ok(mut requests) = self.requests.lock() {
            requests.push(format!("{kind} {name}"));
        }
    }
}

#[async_trait]
impl CatalogProvider for SnapshotCatalogProvider {
    fn get_name(&self) -> &str {
        self.inner.get_name()
    }

    fn default_namespace(&self) -> Namespace {
        self.inner.default_namespace()
    }

    fn is_session_local(&self) -> bool {
        self.session_local
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

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        self.record("table", table);
        let action = self.on_request.lock().ok().and_then(|mut x| {
            let i = x.iter().position(|(name, _)| name == table)?;
            Some(x.remove(i))
        });
        if let Some((_, action)) = action {
            action.await;
        }
        let mut status = match self.names {
            TableNames::Exact => self.inner.get_table(database, table).await?,
            TableNames::IgnoreCase => {
                self.inner
                    .get_table(database, &table.to_lowercase())
                    .await?
            }
            TableNames::IgnoreCaseAndEcho => {
                let mut status = self
                    .inner
                    .get_table(database, &table.to_lowercase())
                    .await?;
                status.name = table.to_string();
                status
            }
            TableNames::ReturnLowercase => {
                let mut status = self.inner.get_table(database, table).await?;
                status.name = table.to_lowercase();
                status
            }
            TableNames::DefaultMain => match Vec::<String>::from(database.clone()).as_slice() {
                [schema] => {
                    let database = vec![Arc::from("main"), Arc::from(schema.as_str())];
                    self.inner.get_table(&database.try_into()?, table).await?
                }
                _ => self.inner.get_table(database, table).await?,
            },
        };
        let load = self.loads.fetch_add(1, Ordering::SeqCst);
        if let TableKind::Table { properties, .. } = &mut status.kind {
            properties.push((SNAPSHOT.to_string(), load.to_string()));
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

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        self.record("view", view);
        let mut status = self.inner.get_view(database, view).await?;
        if matches!(self.names, TableNames::ReturnLowercase) {
            status.name = view.to_lowercase();
        }
        Ok(status)
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

/// What a table source was created with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CreatedSource {
    /// The `snapshot` property of the catalog load that the source reads.
    snapshot: String,
    /// The read options and the time travel options.
    options: Vec<(String, String)>,
    /// The table name that the source carries to execution.
    catalog_table: Option<Vec<String>>,
}

/// The columns `k` and `v`, which all the tables that the tests write to have.
fn columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]))
}

/// What the target of a write was planned with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedWrite {
    /// The `snapshot` property of the catalog load that the write is planned on.
    snapshot: Option<String>,
    /// The operation that the catalog table of the target is accessed for.
    operation: Option<LakehouseOperation>,
    /// The table name that the write carries to execution, where the write commits
    /// to the table of that name.
    catalog_table: Option<Vec<String>>,
}

/// A data source that records the sources it creates and the writes it plans. It is
/// named after a format with time travel, identifier reads, and row-level operations,
/// so that the tests can cover them.
struct RecordingDataSource {
    name: &'static str,
    created: Mutex<Vec<CreatedSource>>,
    written: Mutex<Vec<PlannedWrite>>,
    /// The number of sources created for reads by path.
    path_sources: AtomicUsize,
}

impl RecordingDataSource {
    fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            created: Mutex::new(vec![]),
            written: Mutex::new(vec![]),
            path_sources: AtomicUsize::new(0),
        })
    }

    fn record_write(
        &self,
        options: Vec<OptionLayer>,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> datafusion_common::Result<LogicalPlan> {
        let snapshot = options.into_iter().find_map(|layer| match layer {
            OptionLayer::TablePropertyList { items } => items
                .into_iter()
                .find(|(key, _)| key == SNAPSHOT)
                .map(|(_, value)| value),
            _ => None,
        });
        self.written
            .lock()
            .map_err(|e| plan_datafusion_err!("{e}"))?
            .push(PlannedWrite {
                snapshot,
                operation: lakehouse_table.as_ref().map(|x| x.operation),
                catalog_table: lakehouse_table.map(|x| x.catalog_table().to_vec()),
            });
        LogicalPlanBuilder::empty(false).build()
    }
}

#[async_trait]
impl DataSource for RecordingDataSource {
    fn name(&self) -> &str {
        self.name
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let mut snapshot = None;
        let mut options = vec![];
        for layer in info.options {
            match layer {
                OptionLayer::TablePropertyList { items } => {
                    snapshot = items
                        .into_iter()
                        .find(|(key, _)| key == SNAPSHOT)
                        .map(|(_, value)| value);
                }
                OptionLayer::OptionList { items } => options.extend(items),
                _ => {}
            }
        }
        let schema = match info.schema {
            Some(schema) => Arc::new(schema),
            // A read by path, which the tests use for temporary views only.
            None => {
                self.path_sources.fetch_add(1, Ordering::SeqCst);
                return Ok(provider_as_source(Arc::new(EmptyTable::new(columns()))));
            }
        };
        let snapshot = snapshot.ok_or_else(|| plan_datafusion_err!("missing snapshot"))?;
        self.created
            .lock()
            .map_err(|e| plan_datafusion_err!("{e}"))?
            .push(CreatedSource {
                snapshot,
                options,
                catalog_table: info.lakehouse_table.map(|x| x.catalog_table().to_vec()),
            });
        Ok(provider_as_source(Arc::new(EmptyTable::new(schema))))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> datafusion_common::Result<LogicalPlan> {
        self.record_write(info.options, info.lakehouse_table)
    }

    async fn infer_schema(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> datafusion_common::Result<SchemaRef> {
        Ok(columns())
    }

    fn as_lake_source(self: Arc<Self>) -> Option<Arc<dyn LakeSource>> {
        Some(self)
    }
}

#[async_trait]
impl LakeSource for RecordingDataSource {
    async fn plan_row_level_operation(
        &self,
        _ctx: &dyn Session,
        operation: RowLevelOperation,
    ) -> datafusion_common::Result<LogicalPlan> {
        let target = match operation {
            RowLevelOperation::Truncate { target, .. } => *target,
            RowLevelOperation::Delete(info) => info.target,
            RowLevelOperation::Update(info) => info.target,
            RowLevelOperation::Merge(info) => info.options.target,
        };
        self.record_write(target.options, target.lakehouse_table)
    }
}

/// The catalog loads and the sources of one statement.
#[derive(Debug)]
struct Resolution {
    loads: usize,
    requests: Vec<String>,
    sources: Vec<CreatedSource>,
    writes: Vec<PlannedWrite>,
    error: Option<String>,
}

impl Resolution {
    /// Asserts that the statement resolved with `loads` catalog loads and returns its sources.
    fn expect(self, loads: usize, sources: usize, context: &str) -> Vec<CreatedSource> {
        assert_eq!(self.error, None, "{context}");
        assert_eq!(self.loads, loads, "catalog loads of {context}");
        assert_eq!(self.sources.len(), sources, "sources of {context}");
        self.sources
    }
}

struct Fixture {
    session: SessionContext,
    catalog: Arc<SnapshotCatalogProvider>,
    /// The catalog `second`, which is not the default catalog.
    second: Arc<SnapshotCatalogProvider>,
    source: Arc<RecordingDataSource>,
    /// Whether the session shares the repeated references to a CTE.
    share_ctes: bool,
}

impl Fixture {
    /// Creates a session with the table `sail.default.t (k, v)` and the statements in `setup`.
    async fn try_new(setup: &[&str]) -> TestResult<Self> {
        Self::try_new_with_names(TableNames::Exact, setup).await
    }

    /// Creates a session like [`Self::try_new`], with catalogs that find and return
    /// table names as `names` says.
    async fn try_new_with_names(names: TableNames, setup: &[&str]) -> TestResult<Self> {
        Self::try_new_with(names, false, setup).await
    }

    /// Creates a session like [`Self::try_new_with_names`], which shares the repeated
    /// references to a CTE if `share_ctes` is set.
    async fn try_new_with(names: TableNames, share_ctes: bool, setup: &[&str]) -> TestResult<Self> {
        let catalog = SnapshotCatalogProvider::new(create_memory_catalog()?, names);
        let fixture = Self::try_new_with_catalog(catalog, share_ctes)?;
        fixture.create_table(&["t"], &["k", "v"]).await?;
        for sql in setup {
            fixture.execute(sql).await?;
        }
        Ok(fixture)
    }

    /// Creates a session without tables, with the catalog `sail` and the catalog `second`,
    /// which finds and returns table names as `sail` does.
    fn try_new_with_catalog(
        catalog: SnapshotCatalogProvider,
        share_ctes: bool,
    ) -> TestResult<Self> {
        let names = catalog.names;
        let catalog = Arc::new(catalog);
        let second = Arc::new(SnapshotCatalogProvider::new(
            MemoryCatalogProvider::new(
                "second".to_string(),
                vec![Arc::from("default")].try_into()?,
                None,
            ),
            names,
        ));
        let session = create_session_with_catalogs(
            Arc::clone(&catalog) as Arc<dyn CatalogProvider>,
            vec![(
                "second".to_string(),
                Arc::clone(&second) as Arc<dyn CatalogProvider>,
            )],
        )?;
        if share_ctes {
            session
                .state_ref()
                .write()
                .config_mut()
                .set_extension(Arc::new(SharedCteConfig::new(true)));
        }
        let source = RecordingDataSource::new("iceberg");
        session
            .extension::<DataSourceRegistry>()?
            .register_data_source(Arc::clone(&source) as Arc<dyn DataSource>)?;
        Ok(Self {
            session,
            catalog,
            second,
            source,
            share_ctes,
        })
    }

    async fn create_table(&self, table: &[&str], columns: &[&str]) -> TestResult {
        self.create_table_at(table, columns, "memory:///table")
            .await
    }

    async fn create_table_at(
        &self,
        table: &[&str],
        columns: &[&str],
        location: &str,
    ) -> TestResult {
        let mut options = create_table_options(columns, vec![]);
        options.location = Some(location.to_string());
        self.session
            .extension::<CatalogManager>()?
            .create_table(table, options)
            .await?;
        Ok(())
    }

    fn resolver(&self) -> PlanResolver<'_> {
        PlanResolver::new(&self.session, Arc::new(PlanConfig::default())).with_shared_ctes()
    }

    async fn execute(&self, sql: &str) -> TestResult {
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        let plan = self.resolver().resolve_named_plan(plan).await?.plan;
        self.execute_plan(plan).await
    }

    async fn execute_plan(&self, plan: LogicalPlan) -> TestResult {
        if self.share_ctes {
            // The future is boxed to keep the futures of the tests small.
            return Box::pin(self.execute_plan_with_shared_ctes(plan)).await;
        }
        self.session
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?;
        Ok(())
    }

    async fn execute_plan_with_shared_ctes(&self, plan: LogicalPlan) -> TestResult {
        let plan = plan_shared_ctes(&self.session, plan).await?;
        collect(plan, self.session.state().task_ctx()).await?;
        Ok(())
    }

    async fn resolve_plan(&self, plan: spec::Plan) -> TestResult<Resolution> {
        let loads = self.catalog.loads.load(Ordering::SeqCst);
        // Discards what the earlier statements recorded.
        take(&self.catalog.requests)?;
        take(&self.source.created)?;
        take(&self.source.written)?;
        let error = self
            .resolver()
            .resolve_named_plan(plan)
            .await
            .err()
            .map(|e| e.to_string());
        Ok(Resolution {
            loads: self.catalog.loads.load(Ordering::SeqCst) - loads,
            requests: take(&self.catalog.requests)?,
            sources: take(&self.source.created)?,
            writes: take(&self.source.written)?,
            error,
        })
    }

    async fn resolve(&self, sql: &str) -> TestResult<Resolution> {
        self.resolve_plan(from_ast_statement(parse_one_statement(sql)?)?)
            .await
    }
}

/// Takes the recorded items out of the list.
fn take<T>(items: &Mutex<Vec<T>>) -> TestResult<Vec<T>> {
    let mut items = items.lock().map_err(|e| e.to_string())?;
    Ok(std::mem::take(&mut *items))
}

/// The options to create a table with `BIGINT` columns directly in a catalog. The test
/// data sources have no lakehouse support, so a statement cannot create their tables.
fn create_table_options(columns: &[&str], properties: Vec<(String, String)>) -> CreateTableOptions {
    let columns = columns
        .iter()
        .map(|name| CreateTableColumnOptions {
            metadata: Default::default(),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            identity: None,
        })
        .collect();
    CreateTableOptions {
        columns,
        comment: None,
        constraints: vec![],
        location: Some("memory:///table".to_string()),
        format: "iceberg".to_string(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        mode: CreateTableMode::Create,
        properties,
        is_external: false,
        is_write_precondition: false,
    }
}

fn object_name(parts: &[&str]) -> spec::ObjectName {
    let (head, tail) = parts.split_first().unwrap_or((&"", &[]));
    tail.iter()
        .fold(spec::ObjectName::bare(*head), |name, part| {
            name.child(*part)
        })
}

/// A read of a table by name, as `spark.read.options(..).table(..)` sends it.
fn read_table(parts: &[&str], options: &[(&str, &str)]) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Read {
        is_streaming: false,
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: object_name(parts),
            temporal: None,
            sample: None,
            options: owned(options),
        })),
    })
}

/// A read of a path in the format, as `spark.read.format(..).options(..).load(..)` sends it.
fn read_path(format: &str, path: &str, options: &[(&str, &str)]) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Read {
        is_streaming: false,
        read_type: spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
            format: Some(format.to_string()),
            schema: None,
            options: owned(options),
            paths: vec![path.to_string()],
            predicates: vec![],
        })),
    })
}

/// A read of a table by identifier, as `spark.read.format("iceberg").load(..)` sends it.
fn read_identifier(identifier: &str) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Read {
        is_streaming: false,
        read_type: spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
            format: Some("iceberg".to_string()),
            schema: None,
            options: vec![],
            paths: vec![identifier.to_string()],
            predicates: vec![],
        })),
    })
}

fn union(left: spec::QueryPlan, right: spec::QueryPlan) -> spec::Plan {
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
}

fn read(plan: spec::QueryPlan) -> spec::Plan {
    spec::Plan::Query(plan)
}

fn owned(options: &[(&str, &str)]) -> Vec<(String, String)> {
    options
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

const SPELLINGS: [&str; 5] = [
    "t",
    "`t`",
    "default.t",
    "sail.default.t",
    "`sail`.`default`.`t`",
];

#[tokio::test]
async fn test_table_spellings_share_one_catalog_load() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    for a in SPELLINGS {
        for b in SPELLINGS {
            let sql = format!("SELECT (SELECT max(v) FROM {a}), (SELECT max(v) FROM {b})");
            fixture.resolve(&sql).await?.expect(1, 1, &sql);
        }
    }
    let sql = format!(
        "SELECT {}",
        SPELLINGS
            .map(|x| format!("(SELECT max(v) FROM {x})"))
            .join(", ")
    );
    fixture.resolve(&sql).await?.expect(1, 1, &sql);
    // The source carries the full name of the table, whichever name comes first.
    for sql in [
        "SELECT * FROM t a JOIN sail.default.t b ON a.k = b.k",
        "SELECT * FROM sail.default.t a JOIN t b ON a.k = b.k",
    ] {
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].catalog_table, Some(full_name()), "{sql}");
    }
    // Each statement loads the table again.
    let first = fixture.resolve(&sql).await?.expect(1, 1, &sql);
    let second = fixture.resolve(&sql).await?.expect(1, 1, &sql);
    assert_ne!(first[0].snapshot, second[0].snapshot);
    Ok(())
}

#[tokio::test]
async fn test_table_spellings_share_one_catalog_load_across_query_shapes() -> TestResult {
    // The same holds when the session shares CTEs.
    for share_ctes in [false, true] {
        let setup = ["CREATE VIEW tv AS SELECT * FROM default.t"];
        let fixture = Fixture::try_new_with(TableNames::Exact, share_ctes, &setup).await?;
        table_spellings_share_one_catalog_load_across_query_shapes(&fixture).await?;
    }
    Ok(())
}

async fn table_spellings_share_one_catalog_load_across_query_shapes(
    fixture: &Fixture,
) -> TestResult {
    for sql in [
        // A join. The columns are still qualified by the names as written.
        "SELECT a.k, default.t.v, c.v FROM t a \
            JOIN default.t ON a.k = default.t.k \
            JOIN sail.default.t c ON a.k = c.k",
        // A CTE and a reference outside it.
        "WITH c AS (SELECT * FROM default.t) SELECT * FROM c JOIN t ON c.k = t.k",
        // A CTE that is referenced twice.
        "WITH c AS (SELECT * FROM sail.default.t) \
            SELECT * FROM c x JOIN c y ON x.k = y.k JOIN t ON x.k = t.k",
        // Scalar subqueries.
        "SELECT (SELECT max(v) FROM t) FROM sail.default.t WHERE v > (SELECT min(v) FROM default.t)",
        // An IN subquery and an EXISTS subquery.
        "SELECT * FROM t WHERE k IN (SELECT k FROM sail.default.t) \
            AND EXISTS (SELECT 1 FROM default.t x WHERE x.k = t.k)",
        // A view that is defined over another spelling.
        "SELECT (SELECT max(v) FROM t), (SELECT max(v) FROM tv)",
        "SELECT * FROM tv JOIN sail.default.t x ON tv.k = x.k",
        // A table name given as a string.
        "SELECT * FROM t a JOIN IDENTIFIER('default.t') b ON a.k = b.k",
        // A set operation.
        "SELECT * FROM t UNION ALL SELECT * FROM default.t UNION ALL SELECT * FROM sail.default.t",
    ] {
        fixture.resolve(sql).await?.expect(1, 1, sql);
    }
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_is_not_merged_with_the_table_of_the_same_name() -> TestResult {
    // The temporary view has the column `x`, and the table has the columns `k` and `v`,
    // so each query resolves only if each name reads the right relation.
    let fixture = Fixture::try_new(&["CREATE TEMPORARY VIEW t AS SELECT 1L AS x"]).await?;
    for sql in [
        "SELECT (SELECT max(x) FROM t), (SELECT max(v) FROM default.t)",
        "SELECT (SELECT max(v) FROM default.t), (SELECT max(x) FROM t)",
        "SELECT (SELECT max(x) FROM t), (SELECT max(v) FROM sail.default.t), \
            (SELECT max(x) FROM `t`), (SELECT max(v) FROM default.t)",
    ] {
        fixture.resolve(sql).await?.expect(1, 1, sql);
    }
    // The temporary view alone loads nothing from the catalog.
    let sql = "SELECT (SELECT max(x) FROM t), (SELECT max(x) FROM t)";
    fixture.resolve(sql).await?.expect(0, 0, sql);
    // An identifier read does not see temporary views, so `t` is the table here.
    let plan = union(read_identifier("t"), read_identifier("default.t"));
    fixture
        .resolve_plan(plan)
        .await?
        .expect(1, 1, "identifier read");
    Ok(())
}

#[tokio::test]
async fn test_global_temporary_view_is_not_merged_with_the_table_of_the_same_name() -> TestResult {
    // `global_temp.g` is the global temporary view, while `sail.global_temp.g` is the
    // table in the catalog database of the same name.
    let fixture = Fixture::try_new(&[
        "CREATE DATABASE global_temp",
        "CREATE GLOBAL TEMPORARY VIEW g AS SELECT 1L AS x",
    ])
    .await?;
    fixture
        .create_table(&["sail", "global_temp", "g"], &["k", "v"])
        .await?;
    for sql in [
        "SELECT (SELECT max(x) FROM global_temp.g), (SELECT max(v) FROM sail.global_temp.g)",
        "SELECT (SELECT max(v) FROM sail.global_temp.g), (SELECT max(x) FROM global_temp.g)",
    ] {
        fixture.resolve(sql).await?.expect(1, 1, sql);
    }
    let sql = "SELECT (SELECT max(x) FROM global_temp.g), (SELECT max(x) FROM global_temp.g)";
    fixture.resolve(sql).await?.expect(0, 0, sql);
    // A missing global temporary view is not looked up in the catalog.
    let resolution = fixture.resolve("SELECT * FROM global_temp.t").await?;
    assert_eq!(resolution.loads, 0);
    assert!(resolution.error.is_some());
    Ok(())
}

#[tokio::test]
async fn test_table_spellings_keep_their_own_read_options() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    // Different options: one catalog load, and one source for each set of options.
    let plan = union(
        read_table(&["t"], &[("a", "1")]),
        read_table(&["default", "t"], &[("b", "2")]),
    );
    let mut sources = fixture.resolve_plan(plan).await?.expect(1, 2, "options");
    sources.sort_by(|a, b| a.options.cmp(&b.options));
    assert_eq!(sources[0].options, owned(&[("a", "1")]));
    assert_eq!(sources[1].options, owned(&[("b", "2")]));
    assert_eq!(sources[0].snapshot, sources[1].snapshot);
    // Options on one reference only.
    let plan = union(
        read_table(&["sail", "default", "t"], &[]),
        read_table(&["t"], &[("a", "1")]),
    );
    let mut sources = fixture.resolve_plan(plan).await?.expect(1, 2, "options");
    sources.sort_by(|a, b| a.options.cmp(&b.options));
    assert_eq!(sources[0].options, vec![]);
    assert_eq!(sources[1].options, owned(&[("a", "1")]));
    assert_eq!(sources[0].snapshot, sources[1].snapshot);
    // The same options: one source.
    let plan = union(
        read_table(&["t"], &[("a", "1")]),
        read_table(&["default", "t"], &[("a", "1")]),
    );
    fixture.resolve_plan(plan).await?.expect(1, 1, "options");
    Ok(())
}

#[tokio::test]
async fn test_table_spellings_keep_their_own_time_travel() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let sql = "SELECT (SELECT max(v) FROM t), (SELECT max(v) FROM default.t VERSION AS OF 1), \
        (SELECT max(v) FROM sail.default.t VERSION AS OF 2)";
    let mut sources = fixture.resolve(sql).await?.expect(1, 3, sql);
    sources.sort_by(|a, b| a.options.cmp(&b.options));
    assert_eq!(sources[0].options, vec![]);
    assert_eq!(sources[1].options.len(), 1);
    assert_eq!(sources[1].options[0].1, "1");
    assert_eq!(sources[2].options.len(), 1);
    assert_eq!(sources[2].options[0].1, "2");
    assert!(sources.iter().all(|x| x.snapshot == sources[0].snapshot));
    // The same version through two spellings: one source.
    let sql = "SELECT (SELECT max(v) FROM t VERSION AS OF 1), \
        (SELECT max(v) FROM default.t VERSION AS OF 1)";
    fixture.resolve(sql).await?.expect(1, 1, sql);
    Ok(())
}

#[tokio::test]
async fn test_tables_of_the_same_name_in_two_databases_are_not_merged() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE DATABASE other"]).await?;
    fixture.create_table(&["other", "t"], &["k", "w"]).await?;
    // The columns `v` and `w` tell the two tables apart.
    let sql = "SELECT (SELECT max(v) FROM t), (SELECT max(v) FROM default.t), \
        (SELECT max(w) FROM other.t), (SELECT max(w) FROM sail.other.t)";
    fixture.resolve(sql).await?.expect(2, 2, sql);
    // The bare name follows the current database.
    fixture.execute("USE DATABASE other").await?;
    let sql = "SELECT (SELECT max(w) FROM t), (SELECT max(v) FROM default.t), \
        (SELECT max(w) FROM other.t), (SELECT max(v) FROM sail.default.t)";
    fixture.resolve(sql).await?.expect(2, 2, sql);
    Ok(())
}

#[tokio::test]
async fn test_identifier_read_spellings_share_one_catalog_load() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE VIEW tv AS SELECT * FROM t"]).await?;
    for (a, b) in [("t", "default.t"), ("default.t", "sail.default.t")] {
        let plan = union(read_identifier(a), read_identifier(b));
        fixture.resolve_plan(plan).await?.expect(1, 1, a);
    }
    // An identifier read and a read by name share the load too.
    for plan in [
        union(read_identifier("default.t"), read_table(&["t"], &[])),
        union(
            read_table(&["sail", "default", "t"], &[]),
            read_identifier("t"),
        ),
    ] {
        fixture.resolve_plan(plan).await?.expect(1, 1, "mixed");
    }
    Ok(())
}

#[tokio::test]
async fn test_identifier_read_does_not_see_views() -> TestResult {
    let fixture = Fixture::try_new(&[
        "CREATE VIEW tv AS SELECT * FROM t",
        "CREATE TEMPORARY VIEW temp AS SELECT 1L AS k, 2L AS v",
    ])
    .await?;
    let not_found = |name: &str| Some(format!("analysis error: Table not found: {name}"));
    // The catalog is asked for the table, whether or not the view was read before.
    // The name `default.tv` could also be a relation of a table `default`.
    for (identifier, fallback) in [
        ("tv", vec![]),
        ("default.tv", vec!["table default"]),
        ("sail.default.tv", vec![]),
    ] {
        let requests = |before: &[&str]| -> Vec<String> {
            let after = ["table tv"].iter().chain(&fallback);
            before.iter().chain(after).map(|x| x.to_string()).collect()
        };

        let resolution = fixture
            .resolve_plan(read(read_identifier(identifier)))
            .await?;
        assert_eq!(resolution.error, not_found("tv"), "{identifier}");
        assert_eq!(resolution.requests, requests(&[]), "{identifier}");

        let plan = union(read_table(&["tv"], &[]), read_identifier(identifier));
        let resolution = fixture.resolve_plan(plan).await?;
        assert_eq!(resolution.error, not_found("tv"), "{identifier}");
        assert_eq!(
            resolution.requests,
            requests(&["table tv", "view tv", "table t"]),
            "{identifier}"
        );

        let plan = union(read_identifier(identifier), read_table(&["tv"], &[]));
        let resolution = fixture.resolve_plan(plan).await?;
        assert_eq!(resolution.error, not_found("tv"), "{identifier}");
        assert_eq!(resolution.requests, requests(&[]), "{identifier}");
    }
    // The same holds for a temporary view.
    for plan in [
        read(read_identifier("temp")),
        union(read_table(&["temp"], &[]), read_identifier("temp")),
        union(read_identifier("temp"), read_table(&["temp"], &[])),
    ] {
        let resolution = fixture.resolve_plan(plan).await?;
        assert_eq!(resolution.error, not_found("temp"));
        assert_eq!(resolution.requests, ["table temp"]);
    }
    Ok(())
}

#[tokio::test]
async fn test_missing_tables_are_not_found() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let not_found = |name: &str| format!("{NOT_FOUND}: {name}");
    for (table, error) in [
        ("missing", not_found("missing")),
        ("default.missing", not_found("missing")),
        ("sail.default.missing", not_found("missing")),
        ("nowhere.missing", not_found("missing")),
        ("sail.nowhere.missing", not_found("missing")),
        (
            "sail.missing",
            "invalid argument: empty namespace".to_string(),
        ),
        // Names are matched exactly, so `T` is not the table `t`.
        ("T", not_found("T")),
        ("default.T", not_found("T")),
        ("DEFAULT.t", not_found("t")),
    ] {
        for (sql, loads) in [
            (format!("SELECT * FROM {table}"), 0),
            (
                format!("SELECT (SELECT max(v) FROM t), (SELECT max(v) FROM {table})"),
                1,
            ),
            (
                format!("SELECT (SELECT max(v) FROM {table}), (SELECT max(v) FROM {table})"),
                0,
            ),
        ] {
            let resolution = fixture.resolve(&sql).await?;
            assert_eq!(resolution.error.as_deref(), Some(error.as_str()), "{sql}");
            assert_eq!(resolution.loads, loads, "{sql}");
        }
    }
    Ok(())
}

/// Runs a test on a thread with a larger stack. The plan resolver futures of queries
/// over several views need more than the default test thread stack in debug builds.
fn run_on_large_stack<F>(test: fn() -> F) -> Result<(), String>
where
    F: Future<Output = TestResult> + 'static,
{
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?
                .block_on(test())
                .map_err(|e| e.to_string())
        })
        .map_err(|e| e.to_string())?
        .join()
        .unwrap_or_else(|e| std::panic::resume_unwind(e))
}

#[test]
fn test_view_spellings_share_one_catalog_lookup() -> Result<(), String> {
    run_on_large_stack(view_spellings_share_one_catalog_lookup)
}

async fn view_spellings_share_one_catalog_lookup() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE VIEW tv AS SELECT * FROM default.t"]).await?;
    for sql in [
        "SELECT * FROM tv a JOIN default.tv b ON a.k = b.k JOIN sail.default.tv c ON a.k = c.k",
        "SELECT (SELECT max(v) FROM sail.default.tv), (SELECT max(v) FROM `tv`), \
            (SELECT max(v) FROM sail.default.t)",
    ] {
        let resolution = fixture.resolve(sql).await?;
        // The view is looked up as a table first, and then as a view.
        assert_eq!(
            resolution.requests,
            ["table tv", "view tv", "table t"],
            "{sql}"
        );
        resolution.expect(1, 1, sql);
    }
    Ok(())
}

#[test]
fn test_nested_views_share_the_catalog_lookups_of_the_query() -> Result<(), String> {
    run_on_large_stack(nested_views_share_the_catalog_lookups_of_the_query)
}

async fn nested_views_share_the_catalog_lookups_of_the_query() -> TestResult {
    let fixture = Fixture::try_new(&[
        "CREATE VIEW inner_view AS SELECT * FROM default.t",
        "CREATE VIEW outer_view AS SELECT * FROM sail.default.inner_view",
    ])
    .await?;
    let sql = "SELECT (SELECT max(v) FROM outer_view), (SELECT max(v) FROM default.inner_view), \
        (SELECT max(v) FROM t), (SELECT max(v) FROM default.outer_view)";
    let resolution = fixture.resolve(sql).await?;
    assert_eq!(
        resolution.requests,
        [
            "table outer_view",
            "view outer_view",
            "table inner_view",
            "view inner_view",
            "table t"
        ]
    );
    resolution.expect(1, 1, sql);
    Ok(())
}

#[tokio::test]
async fn test_cte_is_not_merged_with_the_table_of_the_same_name() -> TestResult {
    // The CTE has the column `x`, and the table has the columns `k` and `v`.
    let fixture = Fixture::try_new(&[]).await?;
    for (sql, loads) in [
        ("WITH t AS (SELECT 1L AS x) SELECT max(x) FROM t", 0),
        (
            "WITH t AS (SELECT 1L AS x) \
                SELECT (SELECT max(x) FROM t), (SELECT max(v) FROM default.t)",
            1,
        ),
        (
            "WITH t AS (SELECT 1L AS x) \
                SELECT (SELECT max(v) FROM sail.default.t), (SELECT max(x) FROM t), \
                (SELECT max(v) FROM default.t)",
            1,
        ),
        // The CTE reads the table of its own name under another spelling.
        (
            "WITH t AS (SELECT k AS x FROM default.t) \
                SELECT * FROM t a JOIN sail.default.t b ON a.x = b.k",
            1,
        ),
    ] {
        fixture.resolve(sql).await?.expect(loads, loads, sql);
    }
    Ok(())
}

#[tokio::test]
async fn test_tables_of_the_same_name_in_two_catalogs_are_not_merged() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    fixture
        .create_table(&["second", "default", "t"], &["k", "w"])
        .await?;
    let second_loads = || fixture.second.loads.load(Ordering::SeqCst);
    // The columns `v` and `w` tell the two tables apart.
    let sql = "SELECT (SELECT max(v) FROM t), (SELECT max(w) FROM second.default.t), \
        (SELECT max(v) FROM sail.default.t), (SELECT max(w) FROM `second`.`default`.t)";
    let before = second_loads();
    fixture.resolve(sql).await?.expect(1, 2, sql);
    assert_eq!(second_loads() - before, 1, "{sql}");
    // The bare name and the name with a database follow the current catalog.
    fixture.execute("USE CATALOG second").await?;
    let sql = "SELECT (SELECT max(w) FROM t), (SELECT max(w) FROM default.t), \
        (SELECT max(w) FROM second.default.t), (SELECT max(v) FROM sail.default.t)";
    let before = second_loads();
    fixture.resolve(sql).await?.expect(1, 2, sql);
    assert_eq!(second_loads() - before, 1, "{sql}");
    Ok(())
}

#[tokio::test]
async fn test_table_spellings_in_a_nested_database_share_one_catalog_load() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE DATABASE a", "CREATE DATABASE a.b"]).await?;
    fixture.create_table(&["a", "b", "t"], &["k", "w"]).await?;
    let sql = "SELECT (SELECT max(w) FROM a.b.t), (SELECT max(w) FROM `a`.`b`.`t`), \
        (SELECT max(v) FROM t)";
    fixture.resolve(sql).await?.expect(2, 2, sql);
    fixture.execute("USE DATABASE a.b").await?;
    let sql = "SELECT (SELECT max(w) FROM t), (SELECT max(w) FROM a.b.t), \
        (SELECT max(w) FROM `t`), (SELECT max(v) FROM sail.default.t)";
    fixture.resolve(sql).await?.expect(2, 2, sql);
    Ok(())
}

#[tokio::test]
async fn test_names_that_the_catalog_returns_one_table_for_share_the_first_load() -> TestResult {
    let fixture = Fixture::try_new_with_names(TableNames::IgnoreCase, &[]).await?;
    // The catalog is asked for each name, and both references use the first answer.
    for (sql, requests) in [
        (
            "SELECT * FROM t a JOIN T b ON a.k = b.k",
            ["table t", "table T"],
        ),
        (
            "SELECT * FROM T a JOIN default.t b ON a.k = b.k",
            ["table T", "table t"],
        ),
        (
            "SELECT (SELECT max(v) FROM t), (SELECT max(v) FROM sail.default.T), \
                (SELECT max(v) FROM default.T)",
            ["table t", "table T"],
        ),
    ] {
        let resolution = fixture.resolve(sql).await?;
        assert_eq!(resolution.requests, requests.map(str::to_string), "{sql}");
        resolution.expect(2, 1, sql);
    }
    // The names keep their own read options and time travel on the shared load.
    let sql = "SELECT * FROM t a JOIN T VERSION AS OF 1 b ON a.k = b.k";
    let sources = fixture.resolve(sql).await?.expect(2, 2, sql);
    assert_eq!(sources[0].snapshot, sources[1].snapshot, "{sql}");
    // A write is planned on the first load, and carries the name of that load.
    for (sql, name) in [
        ("INSERT INTO T SELECT * FROM t", "t"),
        ("INSERT INTO t SELECT * FROM T", "T"),
        ("DELETE FROM T WHERE k IN (SELECT k FROM t)", "T"),
        (
            "MERGE INTO t a USING T b ON a.k = b.k WHEN MATCHED THEN DELETE",
            "t",
        ),
    ] {
        let resolution = fixture.resolve(sql).await?;
        let writes = resolution.writes.clone();
        let sources = resolution.expect(2, 1, sql);
        assert_eq!(writes.len(), 1, "{sql}");
        assert_eq!(
            writes[0].snapshot,
            Some(sources[0].snapshot.clone()),
            "{sql}"
        );
        let name = vec!["sail".to_string(), "default".to_string(), name.to_string()];
        assert_eq!(writes[0].catalog_table, Some(name.clone()), "{sql}");
        assert_eq!(sources[0].catalog_table, Some(name), "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn test_table_with_and_without_the_default_database_part_share_the_first_load() -> TestResult
{
    let fixture = Fixture::try_new_with_names(
        TableNames::DefaultMain,
        &["CREATE DATABASE main", "CREATE DATABASE main.schema"],
    )
    .await?;
    fixture
        .create_table(&["main", "schema", "t"], &["k", "w"])
        .await?;
    for sql in [
        "SELECT * FROM schema.t a JOIN main.schema.t b ON a.k = b.k",
        "SELECT * FROM main.schema.t a JOIN sail.schema.t b ON a.k = b.k",
        "INSERT INTO main.schema.t SELECT * FROM schema.t",
    ] {
        let resolution = fixture.resolve(sql).await?;
        let writes = resolution.writes.clone();
        let sources = resolution.expect(2, 1, sql);
        for write in writes {
            assert_eq!(write.snapshot, Some(sources[0].snapshot.clone()), "{sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_names_that_the_catalog_returns_different_tables_for_are_not_merged() -> TestResult {
    // The catalog finds one table for both names and returns the name it was asked
    // for, so nothing tells that the names are one table.
    let fixture = Fixture::try_new_with_names(TableNames::IgnoreCaseAndEcho, &[]).await?;
    let sql = "SELECT * FROM t a JOIN T b ON a.k = b.k";
    let sources = fixture.resolve(sql).await?.expect(2, 2, sql);
    assert_ne!(sources[0].snapshot, sources[1].snapshot, "{sql}");

    // Two tables whose names differ in letter case.
    let fixture = Fixture::try_new(&[]).await?;
    fixture.create_table(&["T"], &["k", "w"]).await?;
    let sql = "SELECT (SELECT max(v) FROM t), (SELECT max(w) FROM T)";
    fixture.resolve(sql).await?.expect(2, 2, sql);
    let sql = "INSERT INTO T SELECT * FROM t";
    let resolution = fixture.resolve(sql).await?;
    let writes = resolution.writes.clone();
    let sources = resolution.expect(2, 1, sql);
    assert_ne!(writes[0].snapshot, Some(sources[0].snapshot.clone()));

    // Two tables that the catalog returns one name for, at different locations.
    let fixture = Fixture::try_new_with_names(TableNames::ReturnLowercase, &[]).await?;
    fixture
        .create_table_at(&["T"], &["k", "w"], "memory:///other")
        .await?;
    let sql = "SELECT (SELECT max(v) FROM t), (SELECT max(w) FROM T)";
    fixture.resolve(sql).await?.expect(2, 2, sql);

    // The same database and table name in two catalogs.
    let fixture = Fixture::try_new_with_names(TableNames::IgnoreCase, &[]).await?;
    fixture
        .create_table(&["second", "default", "t"], &["k", "w"])
        .await?;
    let sql = "SELECT (SELECT max(v) FROM T), (SELECT max(w) FROM second.default.t), \
        (SELECT max(w) FROM second.default.T), (SELECT max(v) FROM t)";
    let second = fixture.second.loads.load(Ordering::SeqCst);
    fixture.resolve(sql).await?.expect(2, 2, sql);
    assert_eq!(fixture.second.loads.load(Ordering::SeqCst) - second, 2);
    Ok(())
}

#[tokio::test]
async fn test_view_is_not_merged_with_the_table_that_the_catalog_returns_its_name_for() -> TestResult
{
    let fixture = Fixture::try_new_with_names(
        TableNames::ReturnLowercase,
        &["CREATE VIEW V AS SELECT k, v AS w FROM t"],
    )
    .await?;
    fixture.create_table(&["v"], &["k", "x"]).await?;
    // The table `v`, and the view `V` over the table `t`.
    let sql = "SELECT (SELECT max(x) FROM v), (SELECT max(w) FROM V)";
    fixture.resolve(sql).await?.expect(2, 2, sql);
    Ok(())
}

/// The full name of the table `t`.
fn full_name() -> Vec<String> {
    vec!["sail".to_string(), "default".to_string(), "t".to_string()]
}

/// Statements that write to the table `t` and may read it under other names.
const WRITES: [(&str, usize); 16] = [
    ("INSERT INTO t SELECT * FROM t", 1),
    ("INSERT INTO t SELECT * FROM default.t", 1),
    ("INSERT INTO default.t SELECT * FROM sail.default.t", 1),
    (
        "INSERT INTO sail.default.t SELECT a.k, b.v FROM t a JOIN default.t b ON a.k = b.k",
        1,
    ),
    ("INSERT INTO sail.default.t SELECT * FROM t", 1),
    ("INSERT OVERWRITE t SELECT * FROM default.t", 1),
    ("INSERT INTO t VALUES (1, 2)", 0),
    ("DELETE FROM t WHERE k = 1", 1),
    ("DELETE FROM default.t WHERE k IN (SELECT k FROM t)", 1),
    ("UPDATE t SET v = 1 WHERE k = 1", 1),
    (
        "UPDATE sail.default.t SET v = (SELECT max(v) FROM default.t)",
        1,
    ),
    (
        "MERGE INTO t a USING default.t b ON a.k = b.k WHEN MATCHED THEN DELETE",
        1,
    ),
    (
        "MERGE INTO t a USING (SELECT * FROM sail.default.t) b ON a.k = b.k \
            WHEN MATCHED THEN UPDATE SET v = b.v WHEN NOT MATCHED THEN INSERT *",
        1,
    ),
    ("TRUNCATE TABLE t", 1),
    ("TRUNCATE TABLE default.t", 1),
    ("TRUNCATE TABLE sail.default.t", 1),
];

#[tokio::test]
async fn test_write_target_shares_the_catalog_load_of_the_statement() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    for (sql, sources) in WRITES {
        let resolution = fixture.resolve(sql).await?;
        let writes = resolution.writes.clone();
        let sources = resolution.expect(1, sources, sql);
        assert_eq!(writes.len(), 1, "{sql}");
        // The write is planned on the table metadata that the statement reads.
        for source in sources {
            assert_eq!(writes[0].snapshot, Some(source.snapshot), "{sql}");
        }
        assert_eq!(
            writes[0].operation,
            Some(LakehouseOperation::Write),
            "{sql}"
        );
        // The write carries the full name of its target, however the statement
        // writes the name of the table.
        assert_eq!(writes[0].catalog_table, Some(full_name()), "{sql}");
    }
    // Each statement loads the table again.
    let sql = "INSERT INTO t SELECT * FROM default.t";
    let first = fixture.resolve(sql).await?.writes;
    let second = fixture.resolve(sql).await?.writes;
    assert_ne!(first[0].snapshot, second[0].snapshot);
    Ok(())
}

/// A write of the table `sail.default.t` to the table `t`, as `DataFrameWriter` sends it.
fn write_table(save_method: spec::TableSaveMethod, mode: spec::SaveMode) -> spec::Plan {
    spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::Write(
        spec::Write {
            input: Box::new(read_table(&["sail", "default", "t"], &[])),
            source: None,
            save_type: spec::SaveType::Table {
                table: object_name(&["t"]),
                save_method,
            },
            mode: Some(mode),
            sort_columns: vec![],
            partitioning_columns: vec![],
            clustering_columns: vec![],
            bucket_by: None,
            options: vec![],
        },
    )))
}

#[tokio::test]
async fn test_data_frame_write_target_shares_the_catalog_load_of_the_query() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    for (save_method, mode) in [
        (spec::TableSaveMethod::SaveAsTable, spec::SaveMode::Append),
        (spec::TableSaveMethod::InsertInto, spec::SaveMode::Append),
        (spec::TableSaveMethod::InsertInto, spec::SaveMode::Overwrite),
    ] {
        let context = format!("{save_method:?} {mode:?}");
        let resolution = fixture.resolve_plan(write_table(save_method, mode)).await?;
        let writes = resolution.writes.clone();
        let sources = resolution.expect(1, 1, &context);
        assert_eq!(writes.len(), 1, "{context}");
        assert_eq!(
            writes[0].snapshot,
            Some(sources[0].snapshot.clone()),
            "{context}"
        );
        assert_eq!(writes[0].catalog_table, Some(full_name()), "{context}");
    }
    // A statement that creates the table if it is missing finds the table with the
    // load of its query.
    let sql = "CREATE TABLE IF NOT EXISTS t AS SELECT * FROM default.t";
    let resolution = fixture.resolve(sql).await?;
    assert_eq!(resolution.writes, vec![], "{sql}");
    resolution.expect(1, 1, sql);
    Ok(())
}

#[tokio::test]
async fn test_write_to_one_table_and_read_of_another_load_each_once() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE DATABASE other"]).await?;
    fixture.create_table(&["other", "t"], &["k", "v"]).await?;
    for sql in [
        "INSERT INTO t SELECT * FROM other.t",
        "DELETE FROM t WHERE k IN (SELECT k FROM other.t)",
        "MERGE INTO t a USING other.t b ON a.k = b.k WHEN MATCHED THEN DELETE",
    ] {
        let resolution = fixture.resolve(sql).await?;
        assert_eq!(resolution.error, None, "{sql}");
        assert_eq!(resolution.loads, 2, "{sql}");
        assert_eq!(resolution.writes.len(), 1, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn test_insert_into_temporary_view_does_not_load_a_catalog_table() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    // A temporary view over a data source, with the name of the catalog table `t`.
    let source = RecordingDataSource::new("recording");
    fixture
        .session
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::clone(&source) as Arc<dyn DataSource>)?;
    fixture
        .execute("CREATE TEMPORARY VIEW t USING recording OPTIONS (path 'memory:///view')")
        .await?;
    let resolution = fixture.resolve("INSERT INTO t VALUES (1, 2)").await?;
    assert_eq!(resolution.error, None);
    assert_eq!(resolution.requests, Vec::<String>::new());
    assert_eq!(resolution.writes, vec![]);
    assert_eq!(source.written.lock().map_err(|e| e.to_string())?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_writes_to_missing_tables_and_views_are_rejected() -> TestResult {
    let fixture = Fixture::try_new(&[
        "CREATE VIEW tv AS SELECT * FROM t",
        "CREATE TEMPORARY VIEW temp AS SELECT 1L AS k, 2L AS v",
    ])
    .await?;
    let insert = |name: &str| {
        format!(
            "analysis error: [TABLE_OR_VIEW_NOT_FOUND] The table or view `{name}` cannot be found"
        )
    };
    let errors = vec![
        ("INSERT INTO missing VALUES (1, 2)", insert("missing")),
        ("INSERT INTO missing SELECT * FROM t", insert("missing")),
        ("INSERT INTO tv VALUES (1, 2)", insert("tv")),
        ("INSERT INTO tv SELECT * FROM tv", insert("tv")),
        ("INSERT INTO temp VALUES (1, 2)", insert("temp")),
        ("INSERT INTO temp SELECT * FROM default.t", insert("temp")),
        (
            "DELETE FROM missing WHERE k = 1",
            format!("{NOT_FOUND}: missing"),
        ),
        ("DELETE FROM tv WHERE k = 1", ROW_LEVEL.to_string()),
        ("DELETE FROM temp WHERE k = 1", ROW_LEVEL.to_string()),
        (
            "DELETE FROM tv WHERE k IN (SELECT k FROM tv)",
            ROW_LEVEL.to_string(),
        ),
        ("UPDATE tv SET v = 1", ROW_LEVEL.to_string()),
        (
            "MERGE INTO tv a USING t b ON a.k = b.k WHEN MATCHED THEN DELETE",
            ROW_LEVEL.to_string(),
        ),
        (
            "TRUNCATE TABLE tv",
            "not supported: [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'TRUNCATE TABLE' expects \
                a table but `tv` is a view."
                .to_string(),
        ),
        ("TRUNCATE TABLE missing", format!("{NOT_FOUND}: missing")),
    ];
    for (sql, error) in errors {
        let resolution = fixture.resolve(sql).await?;
        assert_eq!(resolution.error, Some(error), "{sql}");
        assert_eq!(resolution.writes, vec![], "{sql}");
    }
    Ok(())
}

const NOT_FOUND: &str =
    "analysis error: Table not found: [TABLE_OR_VIEW_NOT_FOUND] Table or view not found";
const ROW_LEVEL: &str = "not supported: row-level operations are only supported against tables";
