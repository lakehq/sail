// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::Session;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::prelude::SessionContext;
use datafusion_common::{plan_datafusion_err, plan_err};
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
use sail_common_datafusion::catalog::{DatabaseStatus, TableKind, TableStatus};
use sail_common_datafusion::datasource::{
    DataSource, DataSourceRegistry, OptionLayer, SinkInfo, SourceInfo,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::config::PlanConfig;
use sail_plan::resolver::PlanResolver;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;

use super::{create_memory_catalog, create_session_with_catalog};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const SNAPSHOT: &str = "snapshot";

/// A memory catalog that counts the table loads and returns a different `snapshot`
/// property from each load, as a catalog does when the table changes between loads.
struct SnapshotCatalogProvider {
    inner: MemoryCatalogProvider,
    loads: AtomicUsize,
    /// The table and view requests, whether they found something or not.
    requests: Mutex<Vec<String>>,
}

impl SnapshotCatalogProvider {
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
        let mut status = self.inner.get_table(database, table).await?;
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

/// What a table source was created with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CreatedSource {
    /// The `snapshot` property of the catalog load that the source reads.
    snapshot: String,
    /// The read options and the time travel options.
    options: Vec<(String, String)>,
}

/// A data source that records the sources it creates. It is named after a format
/// with time travel and identifier reads, so that the tests can cover both.
struct RecordingDataSource {
    created: Mutex<Vec<CreatedSource>>,
}

#[async_trait]
impl DataSource for RecordingDataSource {
    fn name(&self) -> &str {
        "iceberg"
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
        let snapshot = snapshot.ok_or_else(|| plan_datafusion_err!("missing snapshot"))?;
        self.created
            .lock()
            .map_err(|e| plan_datafusion_err!("{e}"))?
            .push(CreatedSource { snapshot, options });
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
        plan_err!("the recording data source does not write")
    }
}

/// The catalog loads and the sources of one statement.
#[derive(Debug)]
struct Resolution {
    loads: usize,
    requests: Vec<String>,
    sources: Vec<CreatedSource>,
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
    source: Arc<RecordingDataSource>,
}

impl Fixture {
    /// Creates a session with the table `sail.default.t (k, v)` and the statements in `setup`.
    async fn try_new(setup: &[&str]) -> TestResult<Self> {
        let catalog = Arc::new(SnapshotCatalogProvider {
            inner: create_memory_catalog()?,
            loads: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
        });
        let session =
            create_session_with_catalog(Arc::clone(&catalog) as Arc<dyn CatalogProvider>)?;
        let source = Arc::new(RecordingDataSource {
            created: Mutex::new(vec![]),
        });
        session
            .extension::<DataSourceRegistry>()?
            .register_data_source(Arc::clone(&source) as Arc<dyn DataSource>)?;
        let fixture = Self {
            session,
            catalog,
            source,
        };
        fixture.create_table(&["t"], &["k", "v"]).await?;
        for sql in setup {
            fixture.execute(sql).await?;
        }
        Ok(fixture)
    }

    async fn create_table(&self, table: &[&str], columns: &[&str]) -> TestResult {
        self.session
            .extension::<CatalogManager>()?
            .create_table(table, create_table_options(columns, vec![]))
            .await?;
        Ok(())
    }

    fn resolver(&self) -> PlanResolver<'_> {
        PlanResolver::new(&self.session, Arc::new(PlanConfig::default()))
    }

    async fn execute(&self, sql: &str) -> TestResult {
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        let plan = self.resolver().resolve_named_plan(plan).await?.plan;
        self.session
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?;
        Ok(())
    }

    async fn resolve_plan(&self, plan: spec::Plan) -> TestResult<Resolution> {
        let loads = self.catalog.loads.load(Ordering::SeqCst);
        self.catalog
            .requests
            .lock()
            .map_err(|e| e.to_string())?
            .clear();
        self.source
            .created
            .lock()
            .map_err(|e| e.to_string())?
            .clear();
        let error = self
            .resolver()
            .resolve_named_plan(plan)
            .await
            .err()
            .map(|e| e.to_string());
        Ok(Resolution {
            loads: self.catalog.loads.load(Ordering::SeqCst) - loads,
            requests: self
                .catalog
                .requests
                .lock()
                .map_err(|e| e.to_string())?
                .clone(),
            sources: self
                .source
                .created
                .lock()
                .map_err(|e| e.to_string())?
                .clone(),
            error,
        })
    }

    async fn resolve(&self, sql: &str) -> TestResult<Resolution> {
        self.resolve_plan(from_ast_statement(parse_one_statement(sql)?)?)
            .await
    }
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
        location: None,
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
    // Each statement loads the table again.
    let first = fixture.resolve(&sql).await?.expect(1, 1, &sql);
    let second = fixture.resolve(&sql).await?.expect(1, 1, &sql);
    assert_ne!(first[0].snapshot, second[0].snapshot);
    Ok(())
}

#[tokio::test]
async fn test_table_spellings_share_one_catalog_load_across_query_shapes() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE VIEW tv AS SELECT * FROM default.t"]).await?;
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
        // The same holds when the session shares CTEs.
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        let loads = fixture.catalog.loads.load(Ordering::SeqCst);
        fixture
            .resolver()
            .with_shared_ctes()
            .resolve_named_plan(plan)
            .await?;
        assert_eq!(
            fixture.catalog.loads.load(Ordering::SeqCst) - loads,
            1,
            "{sql}"
        );
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
async fn test_missing_tables_are_reported_as_before() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let not_found = |name: &str| {
        format!(
            "analysis error: Table not found: [TABLE_OR_VIEW_NOT_FOUND] Table or view not found: {name}"
        )
    };
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
