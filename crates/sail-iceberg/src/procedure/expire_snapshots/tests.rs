#![expect(clippy::expect_used)]

use object_store::ObjectStoreExt;

use super::*;
use crate::procedure::descriptor::IcebergProcedureType;
use crate::procedure::rewrite_manifests::tests::{entry, rewrite_fixture};
use crate::spec::FormatVersion;

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
async fn preparation_reads_each_physical_manifest_once_and_preserves_retained_specs() {
    use crate::io::test_store::TestStore;
    use crate::spec::{ManifestListWriteContext, ManifestListWriter};

    for shared_list in [false, true] {
        for noop in [false, true] {
            let (store, url, mut metadata, descriptors) = rewrite_fixture(
                FormatVersion::V3,
                vec![
                    (2, vec![entry(1, FormatVersion::V3)]),
                    (1, vec![entry(2, FormatVersion::V3)]),
                    (3, vec![entry(3, FormatVersion::V3)]),
                ],
                false,
            )
            .await;
            for (name, range, id) in [
                ("metadata/list.avro", 0..2, 10),
                ("metadata/new-list.avro", 1..3, 20),
            ] {
                let mut writer = ManifestListWriter::new();
                for descriptor in &descriptors[range] {
                    writer.append(descriptor.clone());
                }
                let bytes = writer
                    .to_bytes_with_context(ManifestListWriteContext {
                        version: FormatVersion::V3,
                        snapshot_id: id,
                        parent_snapshot_id: None,
                        sequence_number: 0,
                        first_row_id: None,
                    })
                    .expect("list");
                store
                    .prefixed
                    .put(&Path::from(name), bytes.into())
                    .await
                    .expect("write list");
            }
            let mut retained = metadata.snapshots[0].clone();
            retained.snapshot_id = 20;
            retained.timestamp_ms = 500;
            retained.parent_snapshot_id = Some(10);
            if !shared_list {
                retained.manifest_list = "metadata/new-list.avro".into();
            }
            // Metadata order must not determine which paths become candidates.
            metadata.snapshots.insert(0, retained);
            metadata.current_snapshot_id = Some(20);
            let counted = Arc::new(TestStore::new(store.base));
            let store = StoreContext::new(counted.clone(), &url).expect("counted store");
            let mut op = operation();
            op.clean_metadata = true;
            if noop {
                op.older_than_ms = Some(0);
            }
            let prepared = op.prepare(&store, &url, &metadata).await.expect("prepare");
            let mut expected = HashMap::from([
                ("table/metadata/list.avro".to_string(), 1),
                ("table/metadata/input-0.avro".to_string(), 1),
                ("table/metadata/input-1.avro".to_string(), 1),
            ]);
            if !shared_list {
                expected.insert("table/metadata/new-list.avro".into(), 1);
                expected.insert("table/metadata/input-2.avro".into(), 1);
            }
            assert_eq!(*counted.gets.lock().expect("counts"), expected);
            let candidates = &prepared.expiration.candidates;
            assert_eq!(candidates.paths.len(), if noop { 0 } else { 5 });
            if !noop {
                for (raw, kind) in [
                    ("metadata/list.avro", 4),
                    ("metadata/input-0.avro", 3),
                    ("metadata/input-1.avro", 3),
                    ("data/1.parquet", 0),
                    ("data/2.parquet", 0),
                ] {
                    let path = checked_path(&store, &url, raw).expect("path");
                    let candidate = candidates
                        .paths
                        .get(&path)
                        .expect("candidate including shared file");
                    assert_eq!(candidate.kind, kind);
                    assert!(candidate.selected);
                }
            }
            let removed_specs: Vec<_> = prepared
                .updates
                .iter()
                .filter_map(|update| {
                    if let TableUpdate::RemovePartitionSpecs { spec_ids } = update {
                        Some(spec_ids.clone())
                    } else {
                        None
                    }
                })
                .flatten()
                .collect();
            assert_eq!(
                removed_specs,
                if shared_list {
                    vec![3]
                } else if noop {
                    vec![]
                } else {
                    vec![2]
                }
            );

            // The post-publication scan is independent, not satisfied by preparation's cache.
            let mut current = metadata;
            if !noop {
                current.snapshots.retain(|s| s.snapshot_id == 20);
            }
            counted.gets.lock().expect("counts").clear();
            prepared
                .expiration
                .finish(
                    &store,
                    &url,
                    Ok(current),
                    IcebergProcedureType::ExpireSnapshots
                        .descriptor()
                        .expect("descriptor")
                        .schema(),
                )
                .await
                .expect("finish");
            if !noop && !shared_list {
                expected.remove("table/metadata/list.avro");
                expected.remove("table/metadata/input-0.avro");
            }
            assert_eq!(*counted.gets.lock().expect("counts"), expected);
            assert!(
                !counted
                    .deletes
                    .lock()
                    .expect("deletes")
                    .contains(&Path::from("table/data/2.parquet"))
            );
        }
    }
}

#[tokio::test]
async fn selected_inventory_preserves_file_kinds_and_shared_statistics() {
    let mut position = entry(2, FormatVersion::V3);
    position.data_file.content = DataContentType::PositionDeletes;
    let mut equality = entry(3, FormatVersion::V3);
    equality.data_file.content = DataContentType::EqualityDeletes;
    let (store, url, mut metadata, _) = rewrite_fixture(
        FormatVersion::V3,
        vec![(0, vec![entry(1, FormatVersion::V3), position, equality])],
        false,
    )
    .await;
    let mut retained = metadata.snapshots[0].clone();
    retained.snapshot_id = 20;
    retained.timestamp_ms = 500;
    retained.parent_snapshot_id = Some(10);
    metadata.snapshots.insert(0, retained);
    metadata.current_snapshot_id = Some(20);
    for id in [20, 10] {
        metadata.statistics.push(
            serde_json::from_value(serde_json::json!({
                "snapshot-id": id, "statistics-path": "metadata/shared.puffin",
                "file-size-in-bytes": 1, "file-footer-size-in-bytes": 0, "blob-metadata": []
            }))
            .expect("statistics"),
        );
    }
    let prepared = operation()
        .prepare(&store, &url, &metadata)
        .await
        .expect("prepare");
    for (raw, kind) in [
        ("data/1.parquet", 0),
        ("data/2.parquet", 1),
        ("data/3.parquet", 2),
        ("metadata/input-0.avro", 3),
        ("metadata/list.avro", 4),
        ("metadata/shared.puffin", 5),
    ] {
        let path = checked_path(&store, &url, raw).expect("path");
        assert_eq!(
            prepared
                .expiration
                .candidates
                .paths
                .get(&path)
                .expect("candidate")
                .kind,
            kind
        );
    }
    assert_eq!(prepared.expiration.candidates.paths.len(), 6);
}

#[tokio::test]
async fn inventory_limit_counts_retained_paths_and_noop_validates_them() {
    let (store, url, mut metadata) = fixture().await;
    for ids in [HashSet::from([10]), HashSet::new()] {
        assert!(
            inventory_with_limit(&store, &url, &metadata, Some(&ids), 3)
                .await
                .is_err()
        );
        let result = inventory_with_limit(&store, &url, &metadata, Some(&ids), 4)
            .await
            .expect("global union fits");
        assert_eq!(result.paths.len(), if ids.is_empty() { 0 } else { 3 });
    }
    let mut op = operation();
    op.older_than_ms = Some(0);
    metadata.statistics.push(
        serde_json::from_value(serde_json::json!({
            "snapshot-id": 20, "statistics-path": "memory://foreign/table/stats.puffin",
            "file-size-in-bytes": 1, "file-footer-size-in-bytes": 0, "blob-metadata": []
        }))
        .expect("statistics"),
    );
    assert!(op.prepare(&store, &url, &metadata).await.is_err());
    metadata.statistics.clear();
    metadata.snapshots[1].key_id = Some("encrypted".into());
    assert!(op.prepare(&store, &url, &metadata).await.is_err());
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
    assert_eq!(prepared.expiration.candidates.paths.len(), 3);
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
    assert!(prepared.expiration.candidates.paths.contains_key(&literal));
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
            paths: HashMap::from([(
                literal.clone(),
                InventoryEntry {
                    kind: 5,
                    selected: true,
                },
            )]),
            specs: HashSet::new(),
        },
        concurrency: 1,
        table_uuid: metadata.table_uuid,
        location: metadata.location.clone(),
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
async fn disjoint_expirations_keep_shared_candidates_until_fresh_reachability() {
    let (store, url, mut metadata) = fixture().await;
    // A statistics container belongs only to the two disjoint expired snapshots.
    let mut head = metadata.snapshots[1].clone();
    head.snapshot_id = 30;
    head.timestamp_ms = 1000;
    head.parent_snapshot_id = Some(20);
    // Statistics containers model physical sharing independently of manifest encoding.
    metadata.snapshots.push(head);
    metadata.current_snapshot_id = Some(30);
    let shared = "memory://bucket/table/metadata/shared.puffin";
    for id in [10, 20] {
        metadata.statistics.push(
            serde_json::from_value(serde_json::json!({
                "snapshot-id": id, "statistics-path": shared, "file-size-in-bytes": 1,
                "file-footer-size-in-bytes": 1, "blob-metadata": []
            }))
            .expect("statistics"),
        );
    }
    let path = checked_path(&store, &url, shared).expect("path");
    store
        .base
        .put(&path, vec![0].into())
        .await
        .expect("statistics");
    let mut op = operation();
    op.older_than_ms = Some(0);
    op.snapshot_ids = HashSet::from([10]);
    let first = op
        .prepare(&store, &url, &metadata)
        .await
        .expect("first inventory");
    op.snapshot_ids = HashSet::from([20]);
    let second = op
        .prepare(&store, &url, &metadata)
        .await
        .expect("second inventory");
    assert!(first.expiration.candidates.paths.contains_key(&path));
    assert!(second.expiration.candidates.paths.contains_key(&path));
    let schema = IcebergProcedureType::ExpireSnapshots
        .descriptor()
        .expect("descriptor")
        .schema();
    metadata
        .snapshots
        .retain(|snapshot| snapshot.snapshot_id != 10);
    metadata.statistics.retain(|file| file.snapshot_id != 10);
    first
        .expiration
        .finish(&store, &url, Ok(metadata.clone()), schema.clone())
        .await
        .expect("first finish");
    assert!(store.base.get(&path).await.is_ok());
    metadata
        .snapshots
        .retain(|snapshot| snapshot.snapshot_id != 20);
    metadata.statistics.clear();
    second
        .expiration
        .finish(&store, &url, Ok(metadata), schema)
        .await
        .expect("second finish");
    assert!(store.base.get(&path).await.is_err());
}

#[tokio::test]
async fn changed_identity_and_unreadable_retained_inventory_preserve_candidates() {
    for fault in 0..3 {
        let (store, url, mut metadata) = fixture().await;
        let prepared = operation()
            .prepare(&store, &url, &metadata)
            .await
            .expect("prepare");
        metadata.snapshots.remove(0);
        match fault {
            0 => metadata.table_uuid = Some(uuid::Uuid::new_v4()),
            1 => metadata.location.push_str("/other"),
            _ => metadata.snapshots[0].manifest_list = "metadata/unreadable.avro".into(),
        }
        let batch = prepared
            .expiration
            .finish(
                &store,
                &url,
                Ok(metadata),
                IcebergProcedureType::ExpireSnapshots
                    .descriptor()
                    .expect("descriptor")
                    .schema(),
            )
            .await
            .expect("safe finish");
        for column in batch.columns() {
            assert_eq!(
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count")
                    .value(0),
                0
            );
        }
        assert!(
            store
                .prefixed
                .get(&Path::from("metadata/list.avro"))
                .await
                .is_ok()
        );
    }
}

#[tokio::test]
async fn expiration_requirements_guard_observed_heads_but_not_unseen_refs_or_properties() {
    let (store, url, mut metadata) = fixture().await;
    metadata.refs.insert(
        "tag".into(),
        SnapshotReference {
            snapshot_id: 20,
            retention: SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        },
    );
    let mut op = operation();
    op.clean_metadata = true;
    let prepared = op.prepare(&store, &url, &metadata).await.expect("prepare");
    assert!(
        prepared
            .requirements
            .contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: Some(20)
            })
    );
    assert!(
        prepared
            .requirements
            .contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "tag".into(),
                snapshot_id: Some(20)
            })
    );
    assert!(
        prepared
            .requirements
            .contains(&TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id
            })
    );
    assert!(
        prepared
            .requirements
            .contains(&TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_spec_id
            })
    );
    assert_eq!(prepared.requirements.len(), 4);
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
