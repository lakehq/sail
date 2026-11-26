// SPDX-License-Identifier: Apache-2.0

use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::InSubquery;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sail_delta_lake::{DeltaLakeSource, DeltaTableSource};
use sail_logical_plan::row_level::RowLevelWriteNode;

use super::*;

/// The location of the Delta table `d (k BIGINT, v BIGINT)`.
const LOCATION: &str = "memory:///d/";

/// The commit of a version of the table `d` with the table ID `id`.
fn commit(version: i64, id: &str) -> (Path, String) {
    let path = Path::from(format!("d/_delta_log/{version:020}.json"));
    let add = format!(
        r#"{{"add":{{"path":"part-{version}.parquet","partitionValues":{{}},"size":4,"modificationTime":{version},"dataChange":true}}}}"#
    );
    if version > 0 {
        return (path, format!("{add}\n"));
    }
    let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"k\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"v\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}"#;
    let contents = format!(
        r#"{{"protocol":{{"minReaderVersion":1,"minWriterVersion":4}}}}
{{"metaData":{{"id":"{id}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{schema}","partitionColumns":[],"configuration":{{"delta.enableChangeDataFeed":"true"}}}}}}
{add}
"#
    );
    (path, contents)
}

async fn put(store: &InMemory, (path, contents): (Path, String)) -> TestResult {
    store.put(&path, contents.into()).await?;
    Ok(())
}

impl Fixture {
    /// Creates a session with the table `t`, which the tests request to commit to the
    /// Delta table `d` in the middle of a statement, and the Delta table `d` at version 0.
    async fn try_new_with_delta_table() -> TestResult<(Self, Arc<InMemory>)> {
        let fixture = Self::try_new(&[]).await?;
        let store = Arc::new(InMemory::new());
        fixture.session.runtime_env().register_object_store(
            ObjectStoreUrl::parse("memory:///")?.as_ref(),
            Arc::clone(&store) as Arc<dyn object_store::ObjectStore>,
        );
        fixture
            .session
            .extension::<DataSourceRegistry>()?
            .register_data_source(Arc::new(DeltaLakeSource))?;
        put(&store, commit(0, "first")).await?;
        let mut options = create_table_options(&["k", "v"], vec![]);
        options.format = "delta".to_string();
        options.location = Some(LOCATION.to_string());
        fixture
            .session
            .extension::<CatalogManager>()?
            .create_table(&["d"], options)
            .await?;
        Ok((fixture, store))
    }

    /// Writes the commits when the statement requests the table.
    fn commit_on_request(
        &self,
        table: &str,
        store: &Arc<InMemory>,
        commits: Vec<(Path, String)>,
    ) -> TestResult {
        let store = Arc::clone(store);
        self.catalog
            .on_request
            .lock()
            .map_err(|e| e.to_string())?
            .push((
                table.to_string(),
                Box::pin(async move {
                    for commit in commits {
                        // A failed commit fails the check of the version that a later
                        // statement reads.
                        let _ = put(&store, commit).await;
                    }
                }),
            ));
        Ok(())
    }

    /// Returns the versions of the Delta table that the statement reads, in plan order.
    async fn delta_versions(&self, plan: spec::Plan) -> TestResult<Vec<i64>> {
        let plan = self.resolver().resolve_named_plan(plan).await?.plan;
        let mut versions = vec![];
        collect_delta_versions(&plan, &mut versions)?;
        Ok(versions)
    }
}

/// Adds the versions of the Delta table that the plan reads, also in the target and in the
/// condition of a row-level write, which are not inputs of the write.
fn collect_delta_versions(
    plan: &LogicalPlan,
    versions: &mut Vec<i64>,
) -> datafusion_common::Result<()> {
    plan.apply(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.downcast_ref::<DeltaTableSource>() {
                    versions.push(source.snapshot().version());
                }
            }
            LogicalPlan::Extension(extension) => {
                if let Some(write) = extension.node.as_any().downcast_ref::<RowLevelWriteNode>() {
                    collect_delta_versions(write.raw_target(), versions)?;
                    if let Some(condition) = write.condition() {
                        condition.expr.apply(|expr| {
                            if let Expr::InSubquery(InSubquery { subquery, .. }) = expr {
                                collect_delta_versions(&subquery.subquery, versions)?;
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })?;
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// The columns `k` and `v` of the read.
fn select_k_v(input: spec::QueryPlan) -> spec::QueryPlan {
    let column = |name: &str| spec::Expr::UnresolvedAttribute {
        name: spec::ObjectName::bare(name),
        plan_id: None,
        is_metadata_column: false,
    };
    spec::QueryPlan::new(spec::QueryNode::Project {
        input: Some(Box::new(input)),
        expressions: vec![column("k"), column("v")],
    })
}

/// The `UNION ALL` of the reads, which are resolved in order.
fn union_all(reads: Vec<spec::QueryPlan>) -> TestResult<spec::Plan> {
    let mut reads = reads.into_iter();
    let first = reads.next().ok_or("no reads")?;
    let plan = reads.fold(first, |left, right| match union(left, right) {
        spec::Plan::Query(plan) => plan,
        spec::Plan::Command(_) => unreachable!("a union is a query"),
    });
    Ok(read(plan))
}

#[tokio::test]
async fn test_delta_reads_by_name_and_by_path_read_one_version() -> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    // The read by name loads version 0, and the commit of version 1 comes before the
    // read by path, which reads version 0 as well.
    fixture.commit_on_request("t", &store, vec![commit(1, "first")])?;
    let plan = union_all(vec![
        read_table(&["d"], &[]),
        read_table(&["t"], &[]),
        read_path("delta", LOCATION, &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![0, 0]);
    // The next statement reads version 1.
    fixture.commit_on_request("t", &store, vec![commit(2, "first")])?;
    let plan = union_all(vec![
        read_path("delta", LOCATION, &[]),
        read_table(&["t"], &[]),
        read_table(&["d"], &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![1, 1]);
    // Reads with other options read one version as well.
    fixture.commit_on_request("t", &store, vec![commit(3, "first")])?;
    let plan = union_all(vec![
        read_path("delta", LOCATION, &[("metadataAsDataRead", "true")]),
        read_table(&["t"], &[]),
        read_path("delta", LOCATION, &[("deltaLogReplayStrategy", "hash")]),
        read_table(&["d"], &[("metadataAsDataRead", "true")]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![2, 2, 2]);
    let plan = read(read_table(&["d"], &[]));
    assert_eq!(fixture.delta_versions(plan).await?, vec![3]);
    Ok(())
}

#[tokio::test]
async fn test_delta_time_travel_and_change_reads_do_not_set_the_version() -> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    // Reads that travel in time or read changes neither use nor record the version of
    // the statement, so the later read reads the latest version.
    fixture.commit_on_request("t", &store, vec![commit(1, "first")])?;
    let plan = union_all(vec![
        read_path("delta", LOCATION, &[("versionAsOf", "0")]),
        select_k_v(read_path(
            "delta",
            LOCATION,
            &[("readChangeFeed", "true"), ("startingVersion", "0")],
        )),
        read_table(&["t"], &[]),
        read_table(&["d"], &[]),
        read_table(&["d"], &[("versionAsOf", "0")]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![0, 1, 0]);
    // A read that travels in time after the first read keeps its version.
    fixture.commit_on_request("t", &store, vec![commit(2, "first")])?;
    let plan = union_all(vec![
        read_path("delta", LOCATION, &[]),
        read_table(&["t"], &[]),
        read_path("delta", LOCATION, &[("versionAsOf", "2")]),
        read_table(&["d"], &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![1, 2, 1]);
    Ok(())
}

#[tokio::test]
async fn test_delta_table_created_again_is_read_at_the_snapshot_of_the_first_read() -> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    fixture.create_table(&["t2"], &["k", "v"]).await?;
    fixture.commit_on_request("t", &store, vec![commit(0, "second"), commit(1, "second")])?;
    fixture.commit_on_request("t2", &store, vec![commit(2, "second")])?;
    // The reads after the table was created again read the snapshot of the first read.
    let plan = union_all(vec![
        read_table(&["d"], &[]),
        read_table(&["t"], &[]),
        read_path("delta", LOCATION, &[]),
        read_table(&["t2"], &[]),
        read_path("delta", LOCATION, &[("deltaLogReplayStrategy", "hash")]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![0, 0, 0]);
    // The next statement reads the new table.
    let plan = read(read_table(&["d"], &[]));
    assert_eq!(fixture.delta_versions(plan).await?, vec![2]);
    Ok(())
}

#[tokio::test]
async fn test_delta_table_created_again_after_a_read_without_files_is_read_at_its_latest_version()
-> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    fixture.commit_on_request("t", &store, vec![commit(0, "second"), commit(1, "second")])?;
    // The first read reads the snapshot without its files, and the read of the data after
    // the table was created again reads the new table.
    let plan = union_all(vec![
        read_path("delta", LOCATION, &[("metadataAsDataRead", "true")]),
        read_table(&["t"], &[]),
        read_table(&["d"], &[]),
        read_path("delta", LOCATION, &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![0, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn test_delta_reads_do_not_load_the_log_again() -> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    put(&store, commit(1, "first")).await?;
    // The commits that the first read read are removed, as the cleanup of the log removes
    // the commits before a checkpoint.
    let log = Arc::clone(&store);
    fixture
        .catalog
        .on_request
        .lock()
        .map_err(|e| e.to_string())?
        .push((
            "t".to_string(),
            Box::pin(async move {
                for version in 0..=1 {
                    let _ = log.delete(&commit(version, "first").0).await;
                }
            }),
        ));
    let plan = union_all(vec![
        read_table(&["d"], &[]),
        read_table(&["t"], &[]),
        read_path("delta", LOCATION, &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![1, 1]);
    Ok(())
}

#[tokio::test]
async fn test_delta_row_level_target_and_source_read_one_version() -> TestResult {
    for sql in [
        "MERGE INTO d USING (SELECT k, v FROM t UNION ALL \
            SELECT k, v FROM delta.`memory:///d/`) AS s ON d.k = s.k WHEN MATCHED THEN DELETE",
        "UPDATE d SET v = 1 WHERE k IN (SELECT k FROM t UNION ALL \
            SELECT k FROM delta.`memory:///d/`)",
        "DELETE FROM d WHERE k IN (SELECT k FROM t UNION ALL \
            SELECT k FROM delta.`memory:///d/`)",
    ] {
        let (fixture, store) = Fixture::try_new_with_delta_table().await?;
        // The target is read first. The source or the condition reads the table by path
        // after a commit, and reads the version of the target, which the write is
        // validated against.
        fixture.commit_on_request("t", &store, vec![commit(1, "first")])?;
        let versions = fixture
            .delta_versions(from_ast_statement(parse_one_statement(sql)?)?)
            .await?;
        assert!(versions.len() >= 2, "{sql}: {versions:?}");
        assert!(versions.iter().all(|v| *v == 0), "{sql}: {versions:?}");
        let plan = read(read_path("delta", LOCATION, &[]));
        assert_eq!(fixture.delta_versions(plan).await?, vec![1], "{sql}");
    }
    Ok(())
}
