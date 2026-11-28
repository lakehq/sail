// SPDX-License-Identifier: Apache-2.0

use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::InSubquery;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sail_delta_lake::{DeltaLakeSource, DeltaTableSource};
use sail_iceberg::IcebergLakeSource;
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
        let mut options = create_table_options(&["k", "v"]);
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
        self.on_request(table, async move {
            for commit in commits {
                // A failed commit fails the check of the version that a later
                // statement reads.
                let _ = put(&store, commit).await;
            }
        })
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
async fn test_delta_reads_read_one_version_when_the_statement_reads_the_location_as_iceberg()
-> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    fixture
        .session
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::new(IcebergLakeSource))?;
    // The location also has the metadata of an Iceberg table, as a Delta Lake table with
    // the metadata of Iceberg (UniForm) has. The Iceberg table `t` has its metadata too.
    let fields = r#"{"id":1,"name":"k","required":false,"type":"long"},{"id":2,"name":"v","required":false,"type":"long"}"#;
    for (location, path) in [(LOCATION, "d"), ("memory:///table/", "table")] {
        let metadata = format!(
            r#"{{"format-version":2,"table-uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1","location":"{location}","last-sequence-number":0,"last-updated-ms":0,"last-column-id":2,"schemas":[{{"type":"struct","schema-id":0,"fields":[{fields}]}}],"current-schema-id":0,"partition-specs":[{{"spec-id":0,"fields":[]}}],"default-spec-id":0,"last-partition-id":999,"sort-orders":[{{"order-id":0,"fields":[]}}],"default-sort-order-id":0,"properties":{{}},"snapshots":[],"snapshot-log":[],"metadata-log":[]}}"#
        );
        let file = Path::from(format!("{path}/metadata/v1.metadata.json"));
        put(&store, (file, metadata)).await?;
    }
    // The read as Iceberg comes between the Delta Lake reads, and the commit of version 1
    // comes before the second Delta Lake read, which reads version 0 as well.
    fixture.commit_on_request("t", &store, vec![commit(1, "first")])?;
    let plan = union_all(vec![
        read_path("delta", LOCATION, &[]),
        select(read_path("iceberg", LOCATION, &[]), &["k", "v"]),
        read_table(&["t"], &[]),
        read_path("delta", LOCATION, &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![0, 0]);
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
        select(
            read_path(
                "delta",
                LOCATION,
                &[("readChangeFeed", "true"), ("startingVersion", "0")],
            ),
            &["k", "v"],
        ),
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
async fn test_delta_table_created_again_after_a_read_without_files_fails_the_statement()
-> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    fixture.commit_on_request("t", &store, vec![commit(0, "second"), commit(1, "second")])?;
    // The first read reads the snapshot without its files, and the read of the data after
    // the table was created again would read another table.
    let plan = reads_without_then_with_files()?;
    assert_replaced(fixture.delta_versions(plan).await);
    Ok(())
}

#[tokio::test]
async fn test_delta_table_created_again_with_fewer_versions_after_a_read_without_files_fails_the_statement()
-> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    put(&store, commit(1, "first")).await?;
    // The table is created again with only version 0 after the first read read version 1
    // without its files.
    let log = Arc::clone(&store);
    fixture.on_request("t", async move {
        let _ = log.delete(&commit(1, "first").0).await;
        let _ = put(&log, commit(0, "second")).await;
    })?;
    let plan = reads_without_then_with_files()?;
    assert_replaced(fixture.delta_versions(plan).await);
    Ok(())
}

/// A read of the Delta table `d` without its files, a read of the table `t`, which the tests
/// request to change `d`, and two reads of the data of `d`.
fn reads_without_then_with_files() -> TestResult<spec::Plan> {
    union_all(vec![
        read_path("delta", LOCATION, &[("metadataAsDataRead", "true")]),
        read_table(&["t"], &[]),
        read_table(&["d"], &[]),
        read_path("delta", LOCATION, &[]),
    ])
}

/// Asserts that the statement failed since the Delta table was created again while the
/// statement was planned.
fn assert_replaced(versions: TestResult<Vec<i64>>) {
    let error = versions.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(error.contains("was replaced by another table"), "{error}");
}

#[tokio::test]
async fn test_delta_reads_do_not_load_the_log_again() -> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    put(&store, commit(1, "first")).await?;
    // The commits that the first read read are removed, as the cleanup of the log removes
    // the commits before a checkpoint.
    let log = Arc::clone(&store);
    fixture.on_request("t", async move {
        for version in 0..=1 {
            let _ = log.delete(&commit(version, "first").0).await;
        }
    })?;
    let plan = union_all(vec![
        read_table(&["d"], &[]),
        read_table(&["t"], &[]),
        read_path("delta", LOCATION, &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![1, 1]);
    Ok(())
}

#[tokio::test]
async fn test_catalog_managed_delta_reads_request_the_ratified_commits_once() -> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    fixture
        .catalog
        .ratifies_delta_commits
        .store(true, Ordering::SeqCst);
    // The log is removed between the reads, as a cleanup of the log after a checkpoint
    // removes the commits that the first read read.
    let log = Arc::clone(&store);
    fixture.on_request("t", async move {
        let _ = log.delete(&commit(0, "first").0).await;
    })?;
    let plan = union_all(vec![
        read_table(&["d"], &[]),
        read_table(&["t"], &[]),
        read_table(&["d"], &[("deltaLogReplayStrategy", "hash")]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![0, 0]);
    let requests = fixture
        .catalog
        .ratified_commit_requests
        .load(Ordering::SeqCst);
    assert_eq!(requests, 1);
    Ok(())
}

#[tokio::test]
async fn test_catalog_managed_delta_read_of_files_after_a_read_without_them_requests_no_commits()
-> TestResult {
    let (fixture, store) = Fixture::try_new_with_delta_table().await?;
    fixture
        .catalog
        .ratifies_delta_commits
        .store(true, Ordering::SeqCst);
    // The read of the data loads the files of the snapshot that the first read read
    // without them, with the commits that the catalog ratified for that snapshot. The
    // catalog has the commits after version 0 only.
    for version in 1..=2 {
        put(&store, commit(version, "first")).await?;
    }
    fixture.catalog.ratified_version.store(2, Ordering::SeqCst);
    let plan = union_all(vec![
        read_table(&["d"], &[("metadataAsDataRead", "true")]),
        read_table(&["d"], &[]),
    ])?;
    assert_eq!(fixture.delta_versions(plan).await?, vec![2, 2]);
    let requests = fixture
        .catalog
        .ratified_commit_requests
        .load(Ordering::SeqCst);
    assert_eq!(requests, 1);
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
