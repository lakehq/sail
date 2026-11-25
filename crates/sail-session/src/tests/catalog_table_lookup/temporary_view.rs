// SPDX-License-Identifier: Apache-2.0

use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::Expr;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use sail_catalog::provider::DropTableOptions;
use sail_common_datafusion::catalog::TableKind;
use sail_plan::shared_cte::visit_scans;

use super::*;

/// A column of a table that a test creates again with other columns.
struct TestColumn {
    name: &'static str,
    data_type: DataType,
    nullable: bool,
    comment: Option<&'static str>,
}

fn column(name: &'static str) -> TestColumn {
    TestColumn {
        name,
        data_type: DataType::Int64,
        nullable: true,
        comment: None,
    }
}

impl Fixture {
    /// The `snapshot` property that the next catalog load returns.
    fn next_snapshot(&self) -> String {
        self.catalog.loads.load(Ordering::SeqCst).to_string()
    }

    async fn drop_table(&self, table: &[&str]) -> TestResult {
        self.session
            .extension::<CatalogManager>()?
            .drop_table(
                table,
                DropTableOptions {
                    if_exists: false,
                    purge: false,
                },
            )
            .await?;
        Ok(())
    }

    /// Drops the table and creates it again with the columns, as a table that changed
    /// after a view was created.
    async fn replace_table(&self, table: &[&str], columns: Vec<TestColumn>) -> TestResult {
        self.drop_table(table).await?;
        let mut options = create_table_options(&[], vec![]);
        options.columns = columns
            .into_iter()
            .map(|column| CreateTableColumnOptions {
                metadata: Default::default(),
                name: column.name.to_string(),
                data_type: column.data_type,
                nullable: column.nullable,
                comment: column.comment.map(|x| x.to_string()),
                default: None,
                generated_always_as: None,
                identity: None,
            })
            .collect();
        self.session
            .extension::<CatalogManager>()?
            .create_table(table, options)
            .await?;
        Ok(())
    }

    /// Creates a temporary view of the query, as `DataFrame.createOrReplaceTempView`
    /// and `DataFrame.createOrReplaceGlobalTempView` send it.
    async fn create_data_frame_view(
        &self,
        view: &str,
        is_global: bool,
        input: spec::QueryPlan,
    ) -> TestResult {
        let plan = spec::Plan::Command(spec::CommandPlan::new(
            spec::CommandNode::CreateTemporaryView {
                view: view.to_string().into(),
                is_global,
                definition: spec::TemporaryViewDefinition {
                    input: Box::new(input),
                    columns: None,
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: vec![],
                },
            },
        ));
        let plan = self.resolver().resolve_named_plan(plan).await?.plan;
        self.execute_plan(plan).await
    }

    /// Resolves the statement and returns its plan.
    async fn plan(&self, sql: &str) -> TestResult<LogicalPlan> {
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        Ok(self.resolver().resolve_named_plan(plan).await?.plan)
    }

    /// Returns the stored plan of a temporary view.
    async fn stored_view_plan(&self, view: &[&str]) -> TestResult<Arc<LogicalPlan>> {
        let status = self
            .session
            .extension::<CatalogManager>()?
            .find_temporary_view(view)
            .await?
            .ok_or("missing temporary view")?;
        match status.kind {
            TableKind::TemporaryView { plan, .. } | TableKind::GlobalTemporaryView { plan, .. } => {
                Ok(plan)
            }
            _ => Err("not a temporary view".into()),
        }
    }
}

/// Counts the scans of the plan, of its subqueries, and of the CTE definitions that
/// it refers to, as the scans that have a table provider and the other scans.
fn count_scans(plan: &LogicalPlan) -> datafusion_common::Result<(usize, usize)> {
    let mut scans = (0, 0);
    visit_scans(plan, &mut |scan| {
        if source_as_provider(&scan.source).is_ok() {
            scans.0 += 1;
        } else {
            scans.1 += 1;
        }
        Ok(())
    })?;
    Ok(scans)
}

/// Creates the temporary views of each kind over the table `t`, which are named after
/// `prefix`, and returns the names to read them by. The global temporary views are
/// shared by all the sessions of the process, so each test has its own prefix.
async fn create_views(fixture: &Fixture, prefix: &str) -> TestResult<Vec<String>> {
    fixture
        .execute(&format!(
            "CREATE TEMPORARY VIEW {prefix}_sv AS SELECT * FROM t"
        ))
        .await?;
    fixture
        .execute(&format!(
            "CREATE TEMPORARY VIEW {prefix}_svc (k, v) AS SELECT k, v FROM t"
        ))
        .await?;
    fixture
        .execute(&format!(
            "CREATE GLOBAL TEMPORARY VIEW {prefix}_gv AS SELECT * FROM t"
        ))
        .await?;
    fixture
        .create_data_frame_view(&format!("{prefix}_dv"), false, read_table(&["t"], &[]))
        .await?;
    fixture
        .create_data_frame_view(&format!("{prefix}_dgv"), true, read_table(&["t"], &[]))
        .await?;
    Ok(vec![
        format!("{prefix}_sv"),
        format!("{prefix}_svc"),
        format!("global_temp.{prefix}_gv"),
        format!("{prefix}_dv"),
        format!("global_temp.{prefix}_dgv"),
    ])
}

#[tokio::test]
async fn test_temporary_view_reads_the_table_as_the_catalog_returns_it_now() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let views = create_views(&fixture, "now").await?;
    for view in &views {
        for _ in 0..2 {
            let snapshot = fixture.next_snapshot();
            let sql = format!("SELECT * FROM {view}");
            let sources = fixture.resolve(&sql).await?.expect(1, 1, &sql);
            assert_eq!(sources[0].snapshot, snapshot, "{sql}");
            assert_eq!(sources[0].catalog_table, Some(full_name()), "{sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_shares_the_catalog_load_of_the_statement() -> TestResult {
    for share_ctes in [false, true] {
        temporary_view_shares_the_catalog_load_of_the_statement(share_ctes).await?;
    }
    Ok(())
}

async fn temporary_view_shares_the_catalog_load_of_the_statement(share_ctes: bool) -> TestResult {
    let setup = [
        "CREATE TEMPORARY VIEW vc AS WITH c AS (SELECT k, rand() AS r FROM t) \
            SELECT a.k, a.r, b.r AS r2 FROM c a JOIN c b ON a.k = b.k",
        "CREATE TEMPORARY VIEW vs AS SELECT k FROM t WHERE v = (SELECT max(v) FROM default.t)",
    ];
    let fixture = Fixture::try_new_with(TableNames::Exact, share_ctes, &setup).await?;
    let prefix = format!("share{share_ctes}");
    let views = create_views(&fixture, &prefix).await?;
    let mut statements = vec![];
    for view in &views {
        statements.extend([
            format!("SELECT * FROM {view} a JOIN t b ON a.k = b.k"),
            format!("SELECT * FROM default.t a JOIN {view} b ON a.k = b.k"),
            format!("SELECT * FROM {view} a JOIN {view} b ON a.k = b.k"),
            format!("SELECT * FROM {view} a JOIN {prefix}_dv b ON a.k = b.k"),
            format!("SELECT (SELECT max(k) FROM {view}), (SELECT max(k) FROM sail.default.t)"),
            format!("SELECT * FROM t WHERE k IN (SELECT k FROM {view})"),
            format!("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM {view} x WHERE x.k = t.k)"),
            format!("WITH c AS (SELECT * FROM {view}) SELECT * FROM c JOIN t ON c.k = t.k"),
            format!(
                "WITH c AS (SELECT k, rand() AS r FROM {view}) \
                    SELECT * FROM c a JOIN c b ON a.k = b.k JOIN t ON a.k = t.k"
            ),
            format!("SELECT * FROM {view} UNION ALL SELECT * FROM t"),
        ]);
    }
    statements.extend([
        "SELECT * FROM vc".to_string(),
        "SELECT * FROM vc JOIN t ON vc.k = t.k".to_string(),
        "SELECT * FROM vs".to_string(),
        "SELECT * FROM vs JOIN vc ON vs.k = vc.k JOIN t ON vs.k = t.k".to_string(),
    ]);
    for sql in &statements {
        let snapshot = fixture.next_snapshot();
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].snapshot, snapshot, "{sql}");
        fixture.execute(sql).await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_write_shares_the_catalog_load_of_the_temporary_view_that_it_reads() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let views = create_views(&fixture, "write").await?;
    for view in &views {
        for sql in [
            format!("INSERT INTO t SELECT * FROM {view}"),
            format!("INSERT OVERWRITE t SELECT * FROM {view}"),
            format!("DELETE FROM t WHERE k IN (SELECT k FROM {view})"),
            format!("UPDATE t SET v = (SELECT max(v) FROM {view})"),
            format!("MERGE INTO t a USING {view} b ON a.k = b.k WHEN MATCHED THEN DELETE"),
        ] {
            let snapshot = fixture.next_snapshot();
            let resolution = fixture.resolve(&sql).await?;
            let writes = resolution.writes.clone();
            let sources = resolution.expect(1, 1, &sql);
            assert_eq!(sources[0].snapshot, snapshot, "{sql}");
            assert_eq!(writes.len(), 1, "{sql}");
            assert_eq!(writes[0].snapshot, Some(snapshot), "{sql}");
            assert_eq!(writes[0].catalog_table, Some(full_name()), "{sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_without_a_table_takes_no_catalog_load() -> TestResult {
    let fixture = Fixture::try_new(&[
        "CREATE TEMPORARY VIEW lit AS SELECT 1L AS k",
        "CREATE TEMPORARY VIEW lit_cte AS WITH c AS (SELECT rand() AS r) \
            SELECT a.r, b.r AS r2 FROM c a JOIN c b ON a.r = b.r",
    ])
    .await?;
    for sql in [
        "SELECT * FROM lit",
        "SELECT * FROM lit_cte",
        "SELECT * FROM lit a JOIN lit b ON a.k = b.k",
    ] {
        let resolution = fixture.resolve(sql).await?;
        assert_eq!(resolution.requests, Vec::<String>::new(), "{sql}");
        resolution.expect(0, 0, sql);
    }
    // The stored plan of such a view is the plan that the query resolved to.
    let stored = fixture.stored_view_plan(&["lit"]).await?;
    assert_eq!(count_scans(&stored)?, (0, 0));
    Ok(())
}

#[test]
fn test_stored_temporary_view_plan_holds_no_table_source() -> Result<(), String> {
    run_on_large_stack(stored_temporary_view_plan_holds_no_table_source)
}

async fn stored_temporary_view_plan_holds_no_table_source() -> TestResult {
    for share_ctes in [false, true] {
        stored_view_plan_holds_no_table_source(share_ctes).await?;
    }
    Ok(())
}

async fn stored_view_plan_holds_no_table_source(share_ctes: bool) -> TestResult {
    let setup = [
        "CREATE VIEW pv AS SELECT * FROM default.t",
        "CREATE TEMPORARY VIEW a AS SELECT * FROM t",
        "CREATE TEMPORARY VIEW b AS SELECT a.k, t.v FROM a JOIN t ON a.k = t.k",
        "CREATE TEMPORARY VIEW c AS WITH c AS (SELECT k, rand() AS r FROM t) \
            SELECT x.k, x.r, y.r AS r2 FROM c x JOIN c y ON x.k = y.k",
        "CREATE TEMPORARY VIEW d AS SELECT k FROM t WHERE v = (SELECT max(v) FROM t) \
            AND k IN (SELECT k FROM a) AND EXISTS (SELECT 1 FROM pv WHERE pv.k = t.k)",
        "CREATE TEMPORARY VIEW e AS SELECT * FROM t VERSION AS OF 1",
        "CREATE TEMPORARY VIEW f AS SELECT * FROM pv UNION ALL SELECT * FROM a",
    ];
    let fixture = Fixture::try_new_with(TableNames::Exact, share_ctes, &setup).await?;
    // The volatile CTE of the view `c` is one definition when the session shares CTEs.
    let c = if share_ctes { 1 } else { 2 };
    for (view, scans) in [("a", 1), ("b", 2), ("c", c), ("d", 4), ("e", 1), ("f", 2)] {
        let stored = fixture.stored_view_plan(&[view]).await?;
        assert_eq!(count_scans(&stored)?, (0, scans), "stored plan of {view}");
        // A stored plan cannot be planned as it is.
        let planned = fixture
            .session
            .state()
            .create_physical_plan(stored.as_ref())
            .await;
        assert!(planned.is_err(), "stored plan of {view}");
        // A read of the view scans table providers only.
        let plan = fixture.plan(&format!("SELECT * FROM {view}")).await?;
        assert_eq!(count_scans(&plan)?, (scans, 0), "read of {view}");
        fixture.execute(&format!("SELECT * FROM {view}")).await?;
    }
    Ok(())
}

#[test]
fn test_temporary_view_over_views_reads_one_table_load() -> Result<(), String> {
    run_on_large_stack(temporary_view_over_views_reads_one_table_load)
}

async fn temporary_view_over_views_reads_one_table_load() -> TestResult {
    let fixture = Fixture::try_new(&[
        "CREATE VIEW pv AS SELECT * FROM default.t",
        "CREATE TEMPORARY VIEW over_pv AS SELECT * FROM pv",
        "CREATE TEMPORARY VIEW v1 AS SELECT * FROM t",
        "CREATE TEMPORARY VIEW v2 AS SELECT * FROM v1",
        "CREATE TEMPORARY VIEW v3 AS SELECT a.k, b.v FROM v2 a JOIN v1 b ON a.k = b.k",
    ])
    .await?;
    for sql in [
        "SELECT * FROM over_pv",
        "SELECT * FROM over_pv a JOIN t b ON a.k = b.k",
        "SELECT * FROM v2",
        "SELECT * FROM v3",
        "SELECT * FROM v3 JOIN v1 USING (k) JOIN t USING (k) JOIN over_pv USING (k)",
    ] {
        let snapshot = fixture.next_snapshot();
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].snapshot, snapshot, "{sql}");
    }
    // A view that is replaced by a query over itself, as
    // `spark.table("r").filter(..).createOrReplaceTempView("r")` does.
    fixture
        .create_data_frame_view("r", false, read_table(&["t"], &[]))
        .await?;
    for _ in 0..3 {
        fixture
            .create_data_frame_view("r", false, read_table(&["r"], &[]))
            .await?;
        let snapshot = fixture.next_snapshot();
        let sql = "SELECT * FROM r JOIN t USING (k)";
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].snapshot, snapshot);
    }
    Ok(())
}

#[test]
fn test_deep_chain_of_temporary_views_reads_one_table_load() -> Result<(), String> {
    run_on_large_stack(deep_chain_of_temporary_views_reads_one_table_load)
}

async fn deep_chain_of_temporary_views_reads_one_table_load() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE TEMPORARY VIEW c0 AS SELECT * FROM t"]).await?;
    for i in 1..=101 {
        fixture
            .execute(&format!(
                "CREATE TEMPORARY VIEW c{i} AS SELECT * FROM c{}",
                i - 1
            ))
            .await?;
    }
    let stored = fixture.stored_view_plan(&["c101"]).await?;
    assert_eq!(count_scans(&stored)?, (0, 1));
    for sql in [
        "SELECT * FROM c101",
        "SELECT * FROM c101 JOIN c50 USING (k) JOIN t USING (k)",
    ] {
        let snapshot = fixture.next_snapshot();
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].snapshot, snapshot, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_keeps_the_table_that_its_definition_named() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE DATABASE other"]).await?;
    fixture
        .create_table_at(&["other", "t"], &["k", "v"], "memory:///other")
        .await?;
    fixture
        .execute("CREATE TEMPORARY VIEW named AS SELECT * FROM t")
        .await?;
    fixture
        .create_data_frame_view("named_df", false, read_table(&["t"], &[]))
        .await?;
    // Another current database.
    fixture.execute("USE DATABASE other").await?;
    for sql in [
        "SELECT * FROM named",
        "SELECT * FROM named_df",
        "SELECT * FROM named JOIN sail.default.t USING (k)",
    ] {
        let resolution = fixture.resolve(sql).await?;
        assert_eq!(resolution.requests, ["table t"], "{sql}");
        let sources = resolution.expect(1, 1, sql);
        assert_eq!(sources[0].catalog_table, Some(full_name()), "{sql}");
    }
    // The table of the same name in the current database is another table.
    let sql = "SELECT * FROM named a JOIN t b ON a.k = b.k";
    fixture.resolve(sql).await?.expect(2, 2, sql);
    fixture.execute("USE DATABASE default").await?;
    // A temporary view that takes the name of the table later.
    fixture
        .execute("CREATE TEMPORARY VIEW t AS SELECT 1L AS x")
        .await?;
    for sql in ["SELECT k, v FROM named", "SELECT k, v FROM named_df"] {
        fixture.resolve(sql).await?.expect(1, 1, sql);
    }
    fixture.execute("DROP VIEW t").await?;
    // A CTE of the query that reads the view, with the name of the table.
    for sql in [
        "WITH t AS (SELECT 1L AS x) SELECT k, v FROM named",
        "WITH t AS (SELECT 1L AS x) SELECT k, v FROM named_df JOIN t ON k = x",
    ] {
        fixture.resolve(sql).await?.expect(1, 1, sql);
    }
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_keeps_its_read_options_and_time_travel() -> TestResult {
    let fixture =
        Fixture::try_new(&["CREATE TEMPORARY VIEW old AS SELECT * FROM t VERSION AS OF 1"]).await?;
    fixture
        .create_data_frame_view("opt", false, read_table(&["t"], &[("a", "1")]))
        .await?;
    for _ in 0..2 {
        let snapshot = fixture.next_snapshot();
        let sql = "SELECT * FROM old";
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].snapshot, snapshot);
        assert_eq!(sources[0].options.len(), 1);
        assert_eq!(sources[0].options[0].1, "1");
        let sql = "SELECT * FROM opt";
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].options, owned(&[("a", "1")]));
    }
    // The view and the table have their own sources over one catalog load.
    let sql = "SELECT * FROM old a JOIN t b ON a.k = b.k JOIN opt c ON a.k = c.k";
    let mut sources = fixture.resolve(sql).await?.expect(1, 3, sql);
    sources.sort_by(|a, b| a.options.cmp(&b.options));
    assert_eq!(sources[0].options, vec![]);
    assert_eq!(sources[1].options, owned(&[("a", "1")]));
    assert_eq!(sources[2].options[0].1, "1");
    assert!(sources.iter().all(|x| x.snapshot == sources[0].snapshot));
    // A direct read of the same version shares the source of the view.
    let sql = "SELECT * FROM old a JOIN t VERSION AS OF 1 b ON a.k = b.k";
    fixture.resolve(sql).await?.expect(1, 1, sql);
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_reads_the_columns_that_it_was_created_with() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let views = create_views(&fixture, "cols").await?;
    // The table has a new column, and its columns have another order.
    fixture
        .replace_table(&["t"], vec![column("w"), column("v"), column("k")])
        .await?;
    for view in &views {
        let sql = format!("SELECT * FROM {view}");
        let plan = fixture.plan(&sql).await?;
        let names = plan
            .schema()
            .fields()
            .iter()
            .map(|x| x.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(names.len(), 2, "{sql}");
        fixture.execute(&sql).await?;
        fixture
            .execute(&format!("SELECT k, max(v) FROM {view} GROUP BY k"))
            .await?;
    }
    // A view over a view and the table: the view side has the columns of the view.
    fixture
        .execute("CREATE TEMPORARY VIEW wide AS SELECT a.k, a.v, b.w FROM cols_sv a JOIN t b ON a.k = b.k")
        .await?;
    fixture
        .execute("CREATE TEMPORARY VIEW narrow AS SELECT * FROM cols_sv")
        .await?;
    let sql = "SELECT * FROM wide";
    fixture.resolve(sql).await?.expect(1, 1, sql);
    fixture.execute(sql).await?;
    // The new column is dropped again. The views that do not read it still read the table.
    fixture
        .replace_table(&["t"], vec![column("k"), column("v")])
        .await?;
    for sql in ["SELECT * FROM cols_sv", "SELECT * FROM narrow"] {
        fixture.resolve(sql).await?.expect(1, 1, sql);
        fixture.execute(sql).await?;
    }
    let resolution = fixture.resolve("SELECT * FROM wide").await?;
    let error = resolution.error.ok_or("missing error")?;
    assert!(
        error.contains(
            "[INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION] View `wide` plan \
            references table `sail`.`default`.`t` whose data columns changed since the view \
            plan was initially captured.\nColumn changes:\n- `w` BIGINT has been removed\n\
            This indicates the table has evolved and the view based on the plan must be \
            recreated."
        ),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_rejects_changes_of_the_columns_that_it_reads() -> TestResult {
    let changes: Vec<(Vec<TestColumn>, &str)> = vec![
        (vec![column("k")], "- `v` BIGINT has been removed"),
        (
            vec![
                column("k"),
                TestColumn {
                    data_type: DataType::Int32,
                    ..column("v")
                },
            ],
            "- `v` type has changed from BIGINT to INT",
        ),
    ];
    for (index, (columns, change)) in changes.into_iter().enumerate() {
        let fixture = Fixture::try_new(&[]).await?;
        let views = create_views(&fixture, &format!("change{index}")).await?;
        fixture.replace_table(&["t"], columns).await?;
        for view in &views {
            let resolution = fixture.resolve(&format!("SELECT * FROM {view}")).await?;
            assert_eq!(resolution.loads, 1, "{view}");
            let error = resolution.error.ok_or("missing error")?;
            assert!(
                error.contains("[INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION]"),
                "{error}"
            );
            assert!(
                error.contains("references table `sail`.`default`.`t` whose data columns changed"),
                "{error}"
            );
            assert!(error.contains(change), "{error}");
        }
    }
    // A column that the views read can have nulls now, or none. A view created when it had
    // none was resolved as if it had nulls.
    let not_null = || TestColumn {
        nullable: false,
        ..column("v")
    };
    for (prefix, before, after) in [
        ("nullable", not_null(), column("v")),
        ("not_null", column("v"), not_null()),
    ] {
        let fixture = Fixture::try_new(&[]).await?;
        fixture
            .replace_table(&["t"], vec![column("k"), before])
            .await?;
        let views = create_views(&fixture, prefix).await?;
        fixture
            .replace_table(&["t"], vec![column("k"), after])
            .await?;
        for view in &views {
            let sql = format!("SELECT * FROM {view}");
            fixture.resolve(&sql).await?.expect(1, 1, &sql);
            fixture.execute(&sql).await?;
        }
    }
    Ok(())
}

/// Returns the arguments of the `count` aggregates of the plan.
fn count_arguments(plan: &LogicalPlan) -> datafusion_common::Result<Vec<Expr>> {
    let mut arguments = vec![];
    plan.apply(|plan| {
        plan.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::AggregateFunction(function) = expr
                    && function.func.name() == "count"
                {
                    arguments.extend(function.params.args.iter().cloned());
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    Ok(arguments)
}

#[tokio::test]
async fn test_temporary_view_counts_a_column_without_nulls_as_a_column() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let not_null = TestColumn {
        nullable: false,
        ..column("v")
    };
    fixture
        .replace_table(&["t"], vec![column("k"), not_null])
        .await?;
    // A query counts the rows, since the column has no nulls now.
    let sql = "SELECT count(v) AS n FROM t";
    let arguments = count_arguments(&fixture.plan(sql).await?)?;
    assert!(
        matches!(arguments.as_slice(), [Expr::Literal(..)]),
        "{arguments:?}"
    );
    // A view counts the values of the column, which can have nulls when it is read.
    fixture
        .execute(&format!("CREATE TEMPORARY VIEW counted AS {sql}"))
        .await?;
    let stored = fixture.stored_view_plan(&["counted"]).await?;
    let arguments = count_arguments(&stored)?;
    assert!(
        matches!(arguments.as_slice(), [Expr::Column(_)]),
        "{arguments:?}"
    );
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_plans_when_only_the_column_metadata_changed() -> TestResult {
    for share_ctes in [false, true] {
        temporary_view_plans_when_only_the_column_metadata_changed(share_ctes).await?;
    }
    Ok(())
}

async fn temporary_view_plans_when_only_the_column_metadata_changed(
    share_ctes: bool,
) -> TestResult {
    let setup = [
        "CREATE TEMPORARY VIEW meta_cte AS WITH c AS (SELECT k, v, rand() AS r FROM t) \
            SELECT a.k, a.v, b.r FROM c a JOIN c b ON a.k = b.k",
    ];
    let fixture = Fixture::try_new_with(TableNames::Exact, share_ctes, &setup).await?;
    let views = create_views(&fixture, &format!("meta{share_ctes}")).await?;
    fixture
        .replace_table(
            &["t"],
            vec![
                TestColumn {
                    comment: Some("the key"),
                    ..column("k")
                },
                column("v"),
            ],
        )
        .await?;
    for view in views.iter().map(String::as_str).chain(["meta_cte"]) {
        for sql in [
            format!("SELECT * FROM {view}"),
            format!("SELECT k, max(v) FROM {view} GROUP BY k"),
            format!("SELECT a.k, count(*) FROM {view} a JOIN t b ON a.k = b.k GROUP BY a.k"),
            format!("SELECT DISTINCT k FROM {view} UNION SELECT k FROM t"),
        ] {
            fixture.resolve(&sql).await?.expect(1, 1, &sql);
            fixture.execute(&sql).await?;
        }
    }
    Ok(())
}

#[test]
fn test_temporary_view_of_a_missing_table_is_not_found() -> Result<(), String> {
    run_on_large_stack(temporary_view_of_a_missing_table_is_not_found)
}

async fn temporary_view_of_a_missing_table_is_not_found() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    let views = create_views(&fixture, "missing").await?;
    fixture.drop_table(&["t"]).await?;
    let direct = fixture
        .resolve("SELECT * FROM sail.default.t")
        .await?
        .error
        .ok_or("missing error")?;
    assert!(
        direct.contains("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: t"),
        "{direct}"
    );
    for view in &views {
        let resolution = fixture.resolve(&format!("SELECT * FROM {view}")).await?;
        assert!(resolution.sources.is_empty(), "{view}");
        let error = resolution.error.ok_or("missing error")?;
        assert!(
            error.contains("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: t"),
            "{error}"
        );
    }
    // The table exists again: the views read it.
    fixture.create_table(&["t"], &["k", "v", "w"]).await?;
    for view in &views {
        let sql = format!("SELECT * FROM {view}");
        fixture.resolve(&sql).await?.expect(1, 1, &sql);
    }
    // A catalog view takes the name of the table.
    fixture.drop_table(&["t"]).await?;
    fixture
        .execute("CREATE VIEW t AS SELECT 1L AS k, 2L AS v")
        .await?;
    for view in &views {
        let resolution = fixture.resolve(&format!("SELECT * FROM {view}")).await?;
        let error = resolution.error.ok_or("missing error")?;
        assert!(error.contains("[TABLE_OR_VIEW_NOT_FOUND]"), "{error}");
        assert!(
            error.contains("`sail`.`default`.`t` is now a view"),
            "{error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_replaced_and_dropped_temporary_views() -> TestResult {
    let fixture = Fixture::try_new(&["CREATE DATABASE other"]).await?;
    fixture
        .create_table_at(&["other", "u"], &["k", "v"], "memory:///other")
        .await?;
    fixture
        .execute("CREATE TEMPORARY VIEW moving AS SELECT * FROM t")
        .await?;
    // A reader holds the plan that it looked up, whatever happens to the view next.
    let stored = fixture.stored_view_plan(&["moving"]).await?;
    fixture
        .execute("CREATE OR REPLACE TEMPORARY VIEW moving AS SELECT * FROM other.u")
        .await?;
    assert_eq!(count_scans(&stored)?, (0, 1));
    let resolution = fixture.resolve("SELECT * FROM moving").await?;
    assert_eq!(resolution.requests, ["table u"]);
    resolution.expect(1, 1, "replaced view");
    fixture.execute("DROP VIEW moving").await?;
    assert_eq!(count_scans(&stored)?, (0, 1));
    assert!(
        fixture
            .resolve("SELECT * FROM moving")
            .await?
            .error
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn test_temporary_view_over_a_table_with_duplicate_column_names() -> TestResult {
    let fixture = Fixture::try_new(&[]).await?;
    fixture
        .create_table_at(&["dup"], &["k", "k"], "memory:///dup")
        .await?;
    fixture
        .execute("CREATE TEMPORARY VIEW dup_v (a, b) AS SELECT * FROM dup")
        .await?;
    for _ in 0..2 {
        let snapshot = fixture.next_snapshot();
        let sql = "SELECT * FROM dup_v";
        let sources = fixture.resolve(sql).await?.expect(1, 1, sql);
        assert_eq!(sources[0].snapshot, snapshot);
        fixture.execute(sql).await?;
    }
    let sql = "SELECT * FROM dup_v, dup";
    fixture.resolve(sql).await?.expect(1, 1, sql);
    Ok(())
}

/// Creates a session with the catalog `sail`, which each session has its own instance
/// of, and the catalog `second`, which is the same catalog in every session.
async fn create_session_fixture() -> TestResult<Fixture> {
    let catalog = SnapshotCatalogProvider {
        session_local: true,
        ..SnapshotCatalogProvider::new(create_memory_catalog()?, TableNames::Exact)
    };
    let fixture = Fixture::try_new_with_catalog(catalog, false)?;
    // Each session sees the same table in the catalog `second`.
    fixture
        .create_table_at(
            &["second", "default", "shared"],
            &["k", "v"],
            "memory:///shared",
        )
        .await?;
    Ok(fixture)
}

/// The catalog loads of the two catalogs of a session.
fn loads(fixture: &Fixture) -> (usize, usize) {
    (
        fixture.catalog.loads.load(Ordering::SeqCst),
        fixture.second.loads.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn test_global_temporary_view_in_another_session() -> TestResult {
    let creator = create_session_fixture().await?;
    let reader = create_session_fixture().await?;
    creator
        .create_table_at(&["mine"], &["k", "v"], "memory:///mine")
        .await?;
    creator
        .execute("CREATE GLOBAL TEMPORARY VIEW xs_mine AS SELECT * FROM mine")
        .await?;
    creator
        .execute("CREATE GLOBAL TEMPORARY VIEW xs_shared AS SELECT * FROM second.default.shared")
        .await?;
    creator
        .create_data_frame_view("xs_mine_df", true, read_table(&["mine"], &[]))
        .await?;
    creator
        .create_data_frame_view(
            "xs_shared_df",
            true,
            read_table(&["second", "default", "shared"], &[]),
        )
        .await?;
    // A table in the catalog of the session that created the view is read in that
    // catalog, once for each statement, whatever the session of the reader has.
    for sql in [
        "SELECT * FROM global_temp.xs_mine",
        "SELECT * FROM global_temp.xs_mine_df",
        "SELECT * FROM global_temp.xs_mine a JOIN global_temp.xs_mine_df b ON a.k = b.k",
    ] {
        let (before, snapshot) = (loads(&creator), loads(&reader));
        let sources = reader.resolve(sql).await?.expect(0, 1, sql);
        assert_eq!(sources[0].snapshot, before.0.to_string(), "{sql}");
        assert_eq!(loads(&creator), (before.0 + 1, before.1), "{sql}");
        assert_eq!(loads(&reader), snapshot, "{sql}");
        reader.execute(sql).await?;
    }
    assert!(reader.resolve("SELECT * FROM mine").await?.error.is_some());
    // A table of the same name in the session of the reader is another table.
    reader
        .create_table_at(&["mine"], &["k", "w"], "memory:///other")
        .await?;
    let sql = "SELECT a.v, b.w FROM global_temp.xs_mine a JOIN mine b ON a.k = b.k";
    let before = loads(&creator);
    reader.resolve(sql).await?.expect(1, 2, sql);
    assert_eq!(loads(&creator), (before.0 + 1, before.1));
    // A table in a catalog that is the same in every session is read in the catalog of
    // the reader, with the other references to the table in the statement.
    for sql in [
        "SELECT * FROM global_temp.xs_shared",
        "SELECT * FROM global_temp.xs_shared_df",
        "SELECT * FROM global_temp.xs_shared a JOIN second.default.shared b ON a.k = b.k \
            JOIN global_temp.xs_shared_df c ON a.k = c.k",
    ] {
        let (before, snapshot) = (loads(&creator), loads(&reader));
        let sources = reader.resolve(sql).await?.expect(0, 1, sql);
        assert_eq!(sources[0].snapshot, snapshot.1.to_string(), "{sql}");
        assert_eq!(loads(&reader), (snapshot.0, snapshot.1 + 1), "{sql}");
        assert_eq!(loads(&creator), before, "{sql}");
    }
    // The session that created the views reads them in its own catalogs.
    for (sql, expected) in [
        (
            "SELECT * FROM global_temp.xs_mine a JOIN mine b ON a.k = b.k",
            (1, 0),
        ),
        ("SELECT * FROM global_temp.xs_shared_df", (0, 1)),
    ] {
        let before = loads(&creator);
        let resolution = creator.resolve(sql).await?;
        assert_eq!(resolution.error, None, "{sql}");
        assert_eq!(resolution.sources.len(), 1, "{sql}");
        assert_eq!(
            loads(&creator),
            (before.0 + expected.0, before.1 + expected.1),
            "{sql}"
        );
    }
    // The views outlive the session that created them.
    let catalog = Arc::clone(&creator.catalog);
    drop(creator);
    let before = catalog.loads.load(Ordering::SeqCst);
    let sql = "SELECT * FROM global_temp.xs_mine";
    reader.resolve(sql).await?.expect(0, 1, sql);
    assert_eq!(catalog.loads.load(Ordering::SeqCst), before + 1);
    Ok(())
}

impl Fixture {
    /// Resolves the statement and returns the number of sources that it created for
    /// reads of paths.
    async fn path_sources(&self, sql: &str) -> TestResult<usize> {
        let before = self.source.path_sources.load(Ordering::SeqCst);
        let resolution = self.resolve(sql).await?;
        assert_eq!(resolution.error, None, "{sql}");
        assert_eq!(resolution.requests, Vec::<String>::new(), "{sql}");
        Ok(self.source.path_sources.load(Ordering::SeqCst) - before)
    }
}

#[tokio::test]
async fn test_temporary_view_reads_the_lake_table_path_again() -> TestResult {
    for share_ctes in [false, true] {
        let setup = [
            "CREATE TEMPORARY VIEW p_sql AS SELECT * FROM iceberg.`memory:///p`",
            "CREATE TEMPORARY VIEW p_old AS SELECT * FROM iceberg.`memory:///p` VERSION AS OF 1",
            "CREATE TEMPORARY VIEW p_cte AS WITH c AS (SELECT k, rand() AS r FROM iceberg.`memory:///p`) \
                SELECT a.k, a.r, b.r AS r2 FROM c a JOIN c b ON a.k = b.k",
        ];
        let fixture = Fixture::try_new_with(TableNames::Exact, share_ctes, &setup).await?;
        fixture
            .create_data_frame_view("p_df", false, read_path("iceberg", "memory:///p", &[]))
            .await?;
        fixture
            .create_data_frame_view(
                "p_opt",
                false,
                read_path("iceberg", "memory:///p", &[("b", "2"), ("a", "1")]),
            )
            .await?;
        for view in ["p_sql", "p_old", "p_df", "p_opt"] {
            let stored = fixture.stored_view_plan(&[view]).await?;
            assert_eq!(count_scans(&stored)?, (0, 1), "stored plan of {view}");
            // Each statement reads the path once, however often it reads the view.
            for sql in [
                format!("SELECT * FROM {view}"),
                format!("SELECT * FROM {view} a JOIN {view} b ON a.k = b.k"),
                format!("SELECT (SELECT max(k) FROM {view}), (SELECT max(v) FROM {view})"),
            ] {
                assert_eq!(fixture.path_sources(&sql).await?, 1, "{sql}");
                fixture.execute(&sql).await?;
            }
        }
        assert_eq!(fixture.path_sources("SELECT * FROM p_cte").await?, 1);
        fixture.execute("SELECT * FROM p_cte").await?;
        // The views and a direct read of the path share one source.
        for sql in [
            "SELECT * FROM iceberg.`memory:///p` a JOIN iceberg.`memory:///p` b ON a.k = b.k",
            "SELECT * FROM p_sql a JOIN iceberg.`memory:///p` b ON a.k = b.k \
                JOIN p_df d ON a.k = d.k JOIN p_cte e ON a.k = e.k",
        ] {
            assert_eq!(fixture.path_sources(sql).await?, 1, "{sql}");
        }
        // Reads with other options, another version, or another path have their own source.
        for sql in [
            "SELECT * FROM p_sql a JOIN p_old b ON a.k = b.k",
            "SELECT * FROM p_sql a JOIN p_opt b ON a.k = b.k",
            "SELECT * FROM p_sql a JOIN iceberg.`memory:///q` b ON a.k = b.k",
        ] {
            assert_eq!(fixture.path_sources(sql).await?, 2, "{sql}");
        }
        // The order of the options of a read does not matter.
        let before = fixture.source.path_sources.load(Ordering::SeqCst);
        let plan = union(
            read_path("iceberg", "memory:///p", &[("a", "1"), ("b", "2")]),
            read_table(&["p_opt"], &[]),
        );
        let resolution = fixture.resolve_plan(plan).await?;
        assert_eq!(resolution.error, None);
        assert_eq!(
            fixture.source.path_sources.load(Ordering::SeqCst) - before,
            1
        );
    }
    Ok(())
}
