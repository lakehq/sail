// SPDX-License-Identifier: Apache-2.0

use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sail_iceberg::{IcebergLakeSource, IcebergTableSource};

use super::*;

/// The location of the Iceberg table `i`.
const LOCATION: &str = "memory:///i/";

/// The metadata file of the version of the Iceberg table `i` that has the first `version`
/// of the columns `k` and `v`, and no snapshot.
fn metadata(version: usize) -> (Path, String) {
    let fields = ["k", "v"]
        .iter()
        .take(version)
        .enumerate()
        .map(|(i, name)| {
            format!(
                r#"{{"id":{},"name":"{name}","required":false,"type":"long"}}"#,
                i + 1
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let contents = format!(
        r#"{{"format-version":2,"table-uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1","location":"{LOCATION}","last-sequence-number":0,"last-updated-ms":0,"last-column-id":{version},"schemas":[{{"type":"struct","schema-id":0,"fields":[{fields}]}}],"current-schema-id":0,"partition-specs":[{{"spec-id":0,"fields":[]}}],"default-spec-id":0,"last-partition-id":999,"sort-orders":[{{"order-id":0,"fields":[]}}],"default-sort-order-id":0,"properties":{{}},"snapshots":[],"snapshot-log":[],"metadata-log":[]}}"#
    );
    (
        Path::from(format!("i/metadata/v{version}.metadata.json")),
        contents,
    )
}

/// Returns the number of columns of the version of the Iceberg table that each read of
/// the statement reads, in plan order.
async fn iceberg_columns(fixture: &Fixture, plan: spec::Plan) -> TestResult<Vec<usize>> {
    let plan = fixture.resolver().resolve_named_plan(plan).await?.plan;
    let mut columns = vec![];
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && scan.source.downcast_ref::<IcebergTableSource>().is_some()
        {
            columns.push(scan.source.schema().fields().len());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(columns)
}

/// Creates a session with the Iceberg table `i` at its first version, which is also the
/// table `i` of the catalog.
async fn try_new_with_iceberg_table() -> TestResult<(Fixture, Arc<InMemory>)> {
    let fixture = Fixture::try_new(&[]).await?;
    let store = Arc::new(InMemory::new());
    fixture.session.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory:///")?.as_ref(),
        Arc::clone(&store) as Arc<dyn object_store::ObjectStore>,
    );
    fixture
        .session
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::new(IcebergLakeSource))?;
    let (path, contents) = metadata(1);
    store.put(&path, contents.into()).await?;
    fixture.create_table_at(&["i"], &["k"], LOCATION).await?;
    Ok((fixture, store))
}

#[tokio::test]
async fn test_iceberg_reads_with_other_options_read_one_version_of_the_metadata() -> TestResult {
    let (fixture, store) = try_new_with_iceberg_table().await?;
    // The version with the column `v` is committed when the statement requests the table
    // by name, after the read by path loaded the metadata.
    let log = Arc::clone(&store);
    fixture
        .catalog
        .on_request
        .lock()
        .map_err(|e| e.to_string())?
        .push((
            "i".to_string(),
            Box::pin(async move {
                let (path, contents) = metadata(2);
                let _ = log.put(&path, contents.into()).await;
            }),
        ));
    let plan = union_all(
        [
            read_path("iceberg", LOCATION, &[]),
            read_table(&["i"], &[]),
            read_path("iceberg", LOCATION, &[("metadataAsDataRead", "true")]),
        ]
        .map(|x| select(x, &["k"])),
    )?;
    assert_eq!(iceberg_columns(&fixture, plan).await?, vec![1, 1, 1]);
    // The next statement reads the new version.
    let plan = read(read_path("iceberg", LOCATION, &[]));
    assert_eq!(iceberg_columns(&fixture, plan).await?, vec![2]);
    Ok(())
}

#[tokio::test]
async fn test_iceberg_reads_of_metadata_files_read_their_files() -> TestResult {
    let (fixture, store) = try_new_with_iceberg_table().await?;
    let (path, contents) = metadata(2);
    store.put(&path, contents.into()).await?;
    let file = |version: usize| format!("{LOCATION}metadata/v{version}.metadata.json");
    let (first, second) = (file(1), file(2));
    let plan = union_all(
        [
            read_path(
                "iceberg",
                LOCATION,
                &[("table_type", "ICEBERG"), ("metadata_location", &first)],
            ),
            read_path(
                "iceberg",
                LOCATION,
                &[("table_type", "ICEBERG"), ("metadata_location", &second)],
            ),
        ]
        .map(|x| select(x, &["k"])),
    )?;
    assert_eq!(iceberg_columns(&fixture, plan).await?, vec![1, 2]);
    Ok(())
}

#[tokio::test]
async fn test_iceberg_relative_metadata_locations_read_the_store_of_their_table() -> TestResult {
    let (fixture, _) = try_new_with_iceberg_table().await?;
    // The table `i` in another object store, whose metadata file at the same path has two
    // columns.
    let other = Arc::new(InMemory::new());
    fixture.session.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://other")?.as_ref(),
        Arc::clone(&other) as Arc<dyn object_store::ObjectStore>,
    );
    let (path, _) = metadata(1);
    let (_, contents) = metadata(2);
    other.put(&path, contents.into()).await?;
    let options = [
        ("table_type", "ICEBERG"),
        ("metadata_location", "i/metadata/v1.metadata.json"),
    ];
    let plan = union_all(
        [
            read_path("iceberg", LOCATION, &options),
            read_path("iceberg", "memory://other/i/", &options),
        ]
        .map(|x| select(x, &["k"])),
    )?;
    assert_eq!(iceberg_columns(&fixture, plan).await?, vec![1, 2]);
    Ok(())
}
