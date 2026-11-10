#![expect(clippy::expect_used)]

use super::*;
use crate::procedure::descriptor::IcebergProcedureType;
use crate::procedure::rewrite_manifests::tests::{entry, rewrite_fixture};
use crate::spec::FormatVersion;
use object_store::ObjectStoreExt;

fn operation() -> ExpireSnapshots {
    ExpireSnapshots {
        now_ms: 1000,
        older_than_ms: Some(500),
        retain_last: None,
        snapshot_ids: HashSet::new(),
        clean_metadata: false,
        concurrency: 2,
    }
}

async fn fixture() -> (StoreContext, Url, TableMetadata) {
    let (store, url, mut metadata, _) = rewrite_fixture(
        FormatVersion::V3,
        vec![(0, vec![entry(1, FormatVersion::V3)])],
        false,
    )
    .await;
    let mut second = metadata.snapshots[0].clone();
    second.snapshot_id = 20;
    second.parent_snapshot_id = Some(10);
    second.timestamp_ms = 500;
    second.manifest_list = "metadata/new-list.avro".into();
    let bytes = store
        .prefixed
        .get(&Path::from("metadata/list.avro"))
        .await
        .expect("list")
        .bytes()
        .await
        .expect("bytes");
    store
        .prefixed
        .put(&Path::from("metadata/new-list.avro"), bytes.into())
        .await
        .expect("new list");
    metadata.snapshots.push(second);
    metadata.current_snapshot_id = Some(20);
    (store, url, metadata)
}

#[tokio::test]
async fn retention_strict_boundary_explicit_ids_and_staged_snapshots() {
    let (_, _, mut metadata) = fixture().await;
    let mut op = operation();
    assert_eq!(
        op.select(&metadata).expect("selection").0,
        HashSet::from([10])
    );
    op.retain_last = Some(2);
    assert!(op.select(&metadata).expect("selection").0.is_empty());
    op.snapshot_ids = HashSet::from([10, 999]);
    assert_eq!(
        op.select(&metadata).expect("selection").0,
        HashSet::from([10])
    );
    op.snapshot_ids.insert(20);
    assert!(op.select(&metadata).is_err());
    op = operation();
    let mut staged = metadata.snapshots[1].clone();
    staged.snapshot_id = 30;
    staged.parent_snapshot_id = None;
    metadata.snapshots.push(staged);
    assert_eq!(
        op.select(&metadata).expect("recent staged").0,
        HashSet::from([10])
    );
    metadata.snapshots[2].timestamp_ms = 499;
    assert_eq!(
        op.select(&metadata).expect("old staged").0,
        HashSet::from([10, 30])
    );
    metadata
        .properties
        .insert("gc.enabled".into(), "false".into());
    metadata.snapshots.clear();
    assert!(op.select(&metadata).is_err());
}

#[tokio::test]
async fn refs_age_by_snapshot_and_branch_overrides() {
    let (_, _, mut metadata) = fixture().await;
    metadata.refs.insert(
        "tag".into(),
        SnapshotReference {
            snapshot_id: 10,
            retention: SnapshotRetention::Tag {
                max_ref_age_ms: Some(1000),
            },
        },
    );
    assert!(
        operation()
            .select(&metadata)
            .expect("tag boundary")
            .0
            .is_empty()
    );
    let mut op = operation();
    op.now_ms += 1;
    let (expired, removed) = op.select(&metadata).expect("aged tag");
    assert_eq!(expired, HashSet::from([10]));
    assert_eq!(removed, vec!["tag"]);
    metadata.refs.insert(
        "branch".into(),
        SnapshotReference {
            snapshot_id: 20,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: Some(2),
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        },
    );
    assert!(op.select(&metadata).expect("branch count").0.is_empty());
    op.snapshot_ids.insert(10);
    assert_eq!(
        op.select(&metadata).expect("explicit overrides count").0,
        HashSet::from([10])
    );
    op.snapshot_ids.insert(20);
    assert!(op.select(&metadata).is_err());
}

#[tokio::test]
async fn guard_checks_authority_before_resolving_but_allows_external_same_store() {
    let (store, url, _) = fixture().await;
    assert!(checked_path(&store, &url, "memory://other/table/data").is_err());
    assert!(checked_path(&store, &url, "s3://bucket/table/data").is_err());
    assert!(checked_path(&store, &url, "memory://bucket/elsewhere/data").is_ok());
    assert_eq!(
        checked_path(&store, &url, "metadata/list.avro").expect("relative"),
        checked_path(&store, &url, "memory://bucket/table/metadata/list.avro").expect("absolute")
    );
}

#[tokio::test]
async fn shared_manifests_refresh_and_missing_files_are_safe() {
    let (store, url, metadata) = fixture().await;
    let schema = IcebergProcedureType::ExpireSnapshots
        .descriptor()
        .expect("descriptor")
        .schema();
    let prepared = operation()
        .prepare(&store, &url, &metadata)
        .await
        .expect("prepare");
    assert_eq!(prepared.expiration.candidates.paths.len(), 1);
    // A concurrent restoration observed by refresh protects the candidate.
    let batch = prepared
        .expiration
        .finish(&store, &url, Ok(metadata.clone()), schema.clone())
        .await
        .expect("finish");
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0),
        0
    );
    let prepared = operation()
        .prepare(&store, &url, &metadata)
        .await
        .expect("prepare");
    let mut current = metadata.clone();
    current.snapshots.remove(0);
    store
        .prefixed
        .delete(&Path::from("metadata/list.avro"))
        .await
        .expect("missing file");
    let batch = prepared
        .expiration
        .finish(&store, &url, Ok(current), schema)
        .await
        .expect("finish");
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0),
        1
    );
    assert!(
        store
            .prefixed
            .get(&Path::from("metadata/input-0.avro"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn literal_percent_aliases_preserve_live_paths_and_do_not_touch_double_escaped_keys() {
    let (store, url, mut metadata) = fixture().await;
    let literal = Path::parse("table/metadata/shared%3A.puffin").expect("literal path");
    let double = Path::parse("table/metadata/shared%253A.puffin").expect("double escaped path");
    // Prove the Path API distinction instead of treating URI escapes as decoded bytes.
    assert_eq!(Path::from("table/metadata/shared%3A.puffin"), double);
    for path in [&literal, &double] {
        store
            .base
            .put(path, b"sentinel".to_vec().into())
            .await
            .expect("put");
    }
    for alias in [
        "memory://bucket/table/metadata/shared%3A.puffin",
        "/table/metadata/shared%3A.puffin",
        "metadata/shared%3A.puffin",
    ] {
        assert_eq!(checked_path(&store, &url, alias).expect("alias"), literal);
    }
    let stats = |id, path: &str| {
        serde_json::from_value(serde_json::json!({
            "snapshot-id": id, "statistics-path": path, "file-size-in-bytes": 8,
            "file-footer-size-in-bytes": 0, "blob-metadata": []
        }))
        .expect("statistics")
    };
    metadata.statistics = vec![
        stats(10, "memory://bucket/table/metadata/shared%3A.puffin"),
        stats(20, "/table/metadata/shared%3A.puffin"),
    ];
    let prepared = operation()
        .prepare(&store, &url, &metadata)
        .await
        .expect("prepare aliases");
    assert!(!prepared.expiration.candidates.paths.contains_key(&literal));
    let mut current = metadata.clone();
    current.snapshots.remove(0);
    current.statistics.remove(0);
    let schema = IcebergProcedureType::ExpireSnapshots
        .descriptor()
        .expect("descriptor")
        .schema();
    prepared
        .expiration
        .finish(&store, &url, Ok(current), schema.clone())
        .await
        .expect("finish");
    assert!(store.base.get(&literal).await.is_ok());
    assert!(store.base.get(&double).await.is_ok());
    // Once genuinely unreachable, remove exactly the literal key, not its encoded sibling.
    Expiration {
        candidates: Inventory {
            paths: HashMap::from([(literal.clone(), 5)]),
            specs: HashSet::new(),
        },
        concurrency: 1,
    }
    .finish(
        &store,
        &url,
        Ok({
            let mut current = metadata;
            current.snapshots.clear();
            current.statistics.clear();
            current
        }),
        schema,
    )
    .await
    .expect("finish unreachable");
    assert!(store.base.get(&literal).await.is_err());
    assert!(store.base.get(&double).await.is_ok());
}

#[tokio::test]
async fn windows_drive_aliases_are_rejected_before_publication_on_every_platform() {
    let (store, _, mut metadata) = fixture().await;
    let url = Url::parse("file:///C:/warehouse/t/").expect("Windows URI");
    let literal = checked_path(
        &store,
        &url,
        "file:///C:/warehouse/t/metadata/shared.puffin",
    )
    .expect("URI key");
    store
        .base
        .put(&literal, b"live statistics".to_vec().into())
        .await
        .expect("sentinel");
    for raw in [
        "C:/warehouse/t/metadata/shared.puffin",
        "C:\\warehouse\\t\\metadata\\shared.puffin",
    ] {
        assert!(checked_path(&store, &url, raw).is_err());
        metadata.statistics = vec![
            serde_json::from_value(serde_json::json!({
                "snapshot-id": 20, "statistics-path": raw, "file-size-in-bytes": 15,
                "file-footer-size-in-bytes": 0, "blob-metadata": []
            }))
            .expect("statistics"),
        ];
        assert!(operation().prepare(&store, &url, &metadata).await.is_err());
        assert_eq!(
            store
                .base
                .get(&literal)
                .await
                .expect("preserved sentinel")
                .bytes()
                .await
                .expect("bytes")
                .as_ref(),
            b"live statistics"
        );
    }
}

#[tokio::test]
async fn azure_container_is_part_of_storage_identity() {
    let (store, _, _) = fixture().await;
    let url =
        Url::parse("abfss://container@account.dfs.core.windows.net/table/").expect("Azure URL");
    assert!(
        checked_path(
            &store,
            &url,
            "abfss://container@account.dfs.core.windows.net/external/file"
        )
        .is_ok()
    );
    for path in [
        "abfss://other@account.dfs.core.windows.net/table/file",
        "abfss://container@other.dfs.core.windows.net/table/file",
        "abfs://container@account.dfs.core.windows.net/table/file",
    ] {
        assert!(checked_path(&store, &url, path).is_err());
    }
}

#[tokio::test]
async fn failed_refresh_preserves_all_candidates() {
    let (store, url, metadata) = fixture().await;
    let prepared = operation()
        .prepare(&store, &url, &metadata)
        .await
        .expect("prepare");
    prepared
        .expiration
        .finish(
            &store,
            &url,
            Err(DataFusionError::Execution("refresh failed".into())),
            IcebergProcedureType::ExpireSnapshots
                .descriptor()
                .expect("descriptor")
                .schema(),
        )
        .await
        .expect("best effort");
    assert!(
        store
            .prefixed
            .get(&Path::from("metadata/list.avro"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn publication_faults_never_delete_candidates() {
    use crate::io::test_store::{PutFault, TestStore};
    use crate::procedure::snapshot::commit_snapshot_operation;
    for fault in [PutFault::Reject, PutFault::LostAckUnreadable] {
        let (store, url, metadata) = fixture().await;
        store
            .prefixed
            .put(
                &Path::from("metadata/v1.metadata.json"),
                metadata.to_json().expect("json").into(),
            )
            .await
            .expect("metadata");
        let mut injected = TestStore::new(store.base);
        injected.fault = Some(("v2.metadata.json".into(), fault));
        let injected = Arc::new(injected);
        let ctx = datafusion::execution::TaskContext::default();
        ctx.runtime_env()
            .register_object_store(&url, injected.clone());
        assert!(
            commit_snapshot_operation(
                &ctx,
                &url,
                &[],
                None,
                operation(),
                IcebergProcedureType::ExpireSnapshots
                    .descriptor()
                    .expect("descriptor")
                    .schema()
            )
            .await
            .is_err()
        );
        assert!(injected.deletes.lock().expect("deletes").is_empty());
    }
}

#[test]
fn floor_micros_and_array_nulls_survive_serialization() {
    let procedure = IcebergProcedureType::ExpireSnapshots
        .descriptor()
        .expect("descriptor");
    let invocation = LakeProcedureInvocation {
        procedure,
        arguments: vec![
            LakeProcedureValue::Utf8("t".into()),
            LakeProcedureValue::TimestampMicros(-1),
            LakeProcedureValue::Null,
            LakeProcedureValue::Null,
            LakeProcedureValue::Null,
            LakeProcedureValue::Int64Array(vec![Some(1), None, Some(1)]),
            LakeProcedureValue::Null,
        ],
    };
    let json = serde_json::to_vec(&invocation).expect("serialize");
    let decoded: LakeProcedureInvocation = serde_json::from_slice(&json).expect("deserialize");
    assert_eq!(invocation, decoded);
    let op = ExpireSnapshots::from_invocation(&decoded).expect("arguments");
    assert_eq!(op.older_than_ms, Some(-1));
    assert_eq!(op.snapshot_ids, HashSet::from([1]));
}
