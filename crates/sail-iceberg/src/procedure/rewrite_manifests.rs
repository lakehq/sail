use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, exec_err, not_impl_err, plan_err};
use object_store::ObjectStoreExt;
use sail_common_datafusion::lakeprocedure::LakeProcedureInvocation;
use serde::Deserialize;

use super::arguments::{optional_bool, optional_i32};
use crate::io::StoreContext;
use crate::spec::manifest_list::{ManifestListWriteContext, ManifestListWriter};
use crate::spec::snapshots::{SnapshotRetention, Summary};
use crate::spec::{
    DataContentType, Datum, FormatVersion, Literal, MAIN_BRANCH, Manifest, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestMetadata, ManifestWriter, Operation, PrimitiveLiteral,
    PrimitiveType, SnapshotBuilder, SnapshotReference, TableMetadata, TableRequirement,
    TableUpdate,
};
use crate::table::files::{
    EntrySelection, ManifestScope, SnapshotScope, manifest_entries, scan_manifests,
};
use crate::table::metadata_loader::table_metadata_location;

const TARGET_SIZE_PROPERTY: &str = "commit.manifest.target-size-bytes";
const DEFAULT_TARGET_SIZE: u64 = 8 * 1024 * 1024;
// This coordinator implementation does not spill or distribute manifest entries.
const MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_ENTRIES: usize = 1_000_000;

pub(crate) struct RewriteManifests {
    spec_id: Option<i32>,
}

pub(super) struct PreparedManifestRewrite {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
    pub manifests: Vec<(String, Vec<u8>)>,
    pub output: RecordBatch,
}

impl RewriteManifests {
    pub(super) fn from_invocation(invocation: &LakeProcedureInvocation) -> Result<Self> {
        // Spark's caching option is a performance hint. This implementation materializes each
        // selected group on the coordinator for both settings, without a Spark dataset cache.
        optional_bool(invocation, "use_caching")?;
        Ok(Self {
            spec_id: optional_i32(invocation, "spec_id")?,
        })
    }

    pub(super) async fn prepare(
        &self,
        store: &StoreContext,
        table_url: &url::Url,
        metadata: &TableMetadata,
        output_schema: SchemaRef,
    ) -> Result<PreparedManifestRewrite> {
        let spec_id = self.spec_id.unwrap_or(metadata.default_spec_id);
        let spec = metadata
            .partition_specs
            .iter()
            .find(|spec| spec.spec_id() == spec_id)
            .ok_or_else(|| DataFusionError::Plan(format!("Invalid spec id {spec_id}")))?;
        let target_size = metadata
            .properties
            .get(TARGET_SIZE_PROPERTY)
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| {
                DataFusionError::Plan(format!("{TARGET_SIZE_PROPERTY} must be a positive integer"))
            })?
            .unwrap_or(DEFAULT_TARGET_SIZE);
        if target_size == 0 {
            return plan_err!("{TARGET_SIZE_PROPERTY} must be positive");
        }
        let mut prepared = PreparedManifestRewrite {
            requirements: vec![],
            updates: vec![],
            manifests: vec![],
            output: output(output_schema.clone(), 0, 0)?,
        };
        let Some(parent) = metadata.current_snapshot() else {
            return Ok(prepared);
        };
        if parent.key_id.is_some() || !metadata.encryption_keys.is_empty() {
            return not_impl_err!("rewrite_manifests does not support encrypted table metadata");
        }
        let manifests = scan_manifests(
            store,
            metadata,
            &ManifestScope {
                snapshots: SnapshotScope::Current,
                content: None,
            },
        )
        .await?;
        let mut selected = HashSet::new();
        for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
            let matching = manifests
                .iter()
                .filter(|manifest| {
                    manifest.partition_spec_id == spec_id && manifest.content == content
                })
                .collect::<Vec<_>>();
            let size = matching.iter().try_fold(0u64, |size, manifest| {
                if manifest.added_files_count.is_none()
                    || manifest.existing_files_count.is_none()
                    || manifest.deleted_files_count.is_none()
                {
                    return exec_err!("No file counts in manifest: {}", manifest.manifest_path);
                }
                let length = u64::try_from(manifest.manifest_length)
                    .map_err(|_| DataFusionError::Execution("Negative manifest length".into()))?;
                size.checked_add(length)
                    .ok_or_else(|| DataFusionError::Execution("Manifest size overflow".into()))
            })?;
            if matching.len() == 1 && size <= target_size {
                continue;
            }
            selected.extend(
                matching
                    .iter()
                    .map(|manifest| manifest.manifest_path.clone()),
            );
        }
        if selected.is_empty() {
            return Ok(prepared);
        }
        let selected_bytes = manifests
            .iter()
            .filter(|m| selected.contains(&m.manifest_path))
            .try_fold(0u64, |size, manifest| {
                size.checked_add(manifest.manifest_length as u64)
            })
            .ok_or_else(|| DataFusionError::Execution("Manifest size overflow".into()))?;
        if selected_bytes > MAX_INPUT_BYTES {
            return not_impl_err!(
                "rewrite_manifests supports at most {MAX_INPUT_BYTES} bytes of input manifests per call"
            );
        }
        if metadata.format_version == FormatVersion::V3
            && (parent.first_row_id.is_none()
                || manifests
                    .iter()
                    .any(|m| m.content == ManifestContentType::Data && m.first_row_id.is_none()))
        {
            return not_impl_err!(
                "rewrite_manifests requires assigned row lineage after a v3 upgrade"
            );
        }
        let first_row_id = if metadata.format_version == FormatVersion::V3 {
            Some(metadata.next_row_id.filter(|id| *id >= 0).ok_or_else(|| {
                DataFusionError::Execution("Missing valid next-row-id for v3 table".into())
            })?)
        } else {
            None
        };
        let snapshot_id = crate::utils::snapshot_id::generate_snapshot_id();
        let sequence_number = if metadata.format_version == FormatVersion::V1 {
            0
        } else {
            metadata
                .last_sequence_number
                .checked_add(1)
                .ok_or_else(|| {
                    DataFusionError::Execution("Iceberg sequence number overflow".into())
                })?
        };
        let mut added = vec![];
        let mut live_entries = 0usize;
        for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
            // Retain the original schema when rewriting historical manifests: using only the
            // current schema would discard metrics for dropped fields on subsequent reads.
            let mut groups: BTreeMap<String, (ManifestMetadata, Vec<ManifestEntry>)> =
                BTreeMap::new();
            for descriptor in manifests
                .iter()
                .filter(|m| m.content == content && selected.contains(&m.manifest_path))
            {
                if descriptor.key_metadata.is_some() {
                    return not_impl_err!("rewrite_manifests does not support encrypted manifests");
                }
                let (mut manifest_metadata, entries) = read_entries(store, descriptor).await?;
                if manifest_metadata.partition_spec != *spec || manifest_metadata.content != content
                {
                    return exec_err!(
                        "Manifest header does not match its descriptor: {}",
                        descriptor.manifest_path
                    );
                }
                manifest_metadata.format_version = metadata.format_version;
                let key = serde_json::to_string(&manifest_metadata.schema)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                let group = groups
                    .entry(key)
                    .or_insert_with(|| (manifest_metadata, vec![]));
                for entry in entries
                    .into_iter()
                    .filter(|entry| EntrySelection::live().matches(entry))
                {
                    live_entries += 1;
                    if live_entries > MAX_ENTRIES {
                        return not_impl_err!(
                            "rewrite_manifests supports at most {MAX_ENTRIES} live entries per call"
                        );
                    }
                    let is_data = entry.data_file.content == DataContentType::Data;
                    if is_data != (content == ManifestContentType::Data) {
                        return exec_err!("Manifest contains a file with incompatible content");
                    }
                    if first_row_id.is_some() && is_data && entry.data_file.first_row_id.is_none() {
                        return not_impl_err!(
                            "rewrite_manifests requires assigned data-file row lineage"
                        );
                    }
                    group.1.push(entry);
                }
            }
            for (_, (manifest_metadata, entries)) in groups {
                let mut keyed = entries
                    .into_iter()
                    .map(|entry| {
                        let key = partition_key(&entry)?;
                        Ok((key, entry))
                    })
                    .collect::<Result<Vec<_>>>()?;
                keyed.sort_by(|left, right| left.0.cmp(&right.0));
                let entries = keyed
                    .into_iter()
                    .map(|(_, entry)| entry)
                    .collect::<Vec<_>>();
                for (mut descriptor, bytes) in encode_manifests(
                    &manifest_metadata,
                    &entries,
                    target_size,
                    sequence_number,
                    snapshot_id,
                )? {
                    let path = format!("metadata/optimized-m-{}.avro", uuid::Uuid::new_v4());
                    descriptor.manifest_path = table_metadata_location(table_url, &path)?;
                    if content == ManifestContentType::Data {
                        // All existing entries have explicit row IDs, so no ID space is allocated.
                        descriptor.first_row_id = first_row_id;
                    }
                    prepared.manifests.push((path, bytes));
                    added.push(descriptor);
                }
            }
        }
        let added_count = added.len();
        let mut list = ManifestListWriter::new();
        for manifest in added.into_iter().chain(
            manifests
                .into_iter()
                .filter(|m| !selected.contains(&m.manifest_path)),
        ) {
            list.append(manifest);
        }
        let list_path = format!("metadata/snap-{snapshot_id}-{}.avro", uuid::Uuid::new_v4());
        let list_bytes = list
            .to_bytes_with_context(ManifestListWriteContext {
                version: metadata.format_version,
                snapshot_id,
                parent_snapshot_id: Some(parent.snapshot_id()),
                sequence_number,
                first_row_id,
            })
            .map_err(DataFusionError::Execution)?;
        let mut summary = Summary::new(Operation::Replace);
        for (key, value) in &parent.summary().additional_properties {
            if key.starts_with("total-") {
                summary = summary.with_property(key, value);
            }
        }
        summary = summary
            .with_property("manifests-replaced", selected.len().to_string())
            .with_property("manifests-created", added_count.to_string())
            .with_property("entries-processed", live_entries.to_string());
        let mut snapshot = SnapshotBuilder::new()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent.snapshot_id())
            .with_sequence_number(sequence_number)
            .with_schema_id(metadata.current_schema_id)
            .with_manifest_list(table_metadata_location(table_url, &list_path)?)
            .with_summary(summary);
        if let Some(first_row_id) = first_row_id {
            snapshot = snapshot.with_first_row_id(first_row_id).with_added_rows(0);
        }
        let retention = metadata
            .refs
            .get(MAIN_BRANCH)
            .map(|reference| reference.retention.clone())
            .unwrap_or(SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            });
        prepared.requirements = vec![
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(parent.snapshot_id()),
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id,
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_spec_id,
            },
        ];
        prepared.updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: snapshot.build().map_err(DataFusionError::Execution)?,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference {
                    snapshot_id,
                    retention,
                },
            },
        ];
        prepared.manifests.push((list_path, list_bytes));
        prepared.output = output(output_schema, selected.len(), added_count)?;
        Ok(prepared)
    }
}

fn output(schema: SchemaRef, rewritten: usize, added: usize) -> Result<RecordBatch> {
    let count = |value| {
        i32::try_from(value)
            .map_err(|_| DataFusionError::Execution("Manifest count exceeds int32".into()))
    };
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![count(rewritten)?])),
            Arc::new(Int32Array::from(vec![count(added)?])),
        ],
    )?)
}

fn partition_key(entry: &ManifestEntry) -> Result<Vec<Option<PrimitiveLiteral>>> {
    entry
        .data_file
        .partition
        .iter()
        .map(|value| match value {
            None => Ok(None),
            Some(Literal::Primitive(value)) => Ok(Some(value.clone())),
            _ => exec_err!("Iceberg partition values must be primitive"),
        })
        .collect()
}

/// Split contiguous partition-clustered ranges using actual encoded size, including headers.
/// A single entry is indivisible and may exceed the target, as with a rolling writer.
fn encode_manifests(
    metadata: &ManifestMetadata,
    entries: &[ManifestEntry],
    target_size: u64,
    sequence_number: i64,
    snapshot_id: i64,
) -> Result<Vec<(ManifestFile, Vec<u8>)>> {
    let mut pending = vec![entries];
    let mut output = vec![];
    while let Some(entries) = pending.pop() {
        if entries.is_empty() {
            continue;
        }
        let mut writer = ManifestWriter::new(Some(snapshot_id), None, metadata.clone());
        for entry in entries {
            writer
                .add_existing_entry(entry.clone())
                .map_err(DataFusionError::Execution)?;
        }
        let bytes = writer
            .to_avro_bytes_v2()
            .map_err(DataFusionError::Execution)?;
        if bytes.len() as u64 > target_size && entries.len() > 1 {
            let split = entries.len() / 2;
            pending.push(&entries[split..]);
            pending.push(&entries[..split]);
            continue;
        }
        let mut descriptor = writer
            .into_manifest_file(String::new(), sequence_number, snapshot_id)
            .map_err(DataFusionError::Execution)?;
        descriptor.manifest_length = i64::try_from(bytes.len())
            .map_err(|_| DataFusionError::Execution("Manifest length exceeds int64".into()))?;
        output.push((descriptor, bytes));
    }
    Ok(output)
}

#[derive(Deserialize)]
struct RawEntry {
    data_file: RawBounds,
}

#[derive(Deserialize)]
struct RawBounds {
    lower_bounds: Option<Vec<RawBound>>,
    upper_bounds: Option<Vec<RawBound>>,
}

#[derive(Deserialize)]
struct RawBound {
    key: i32,
    value: serde_bytes::ByteBuf,
}

fn restore_bounds(raw: Option<Vec<RawBound>>, decoded: &mut HashMap<i32, Datum>) -> Result<()> {
    // Bounds are opaque bytes in Iceberg. The scan reader drops bounds it cannot type
    // (e.g. dropped columns or position-delete metadata). Keep these bytes on rewrite.
    let mut seen = HashSet::new();
    for bound in raw.unwrap_or_default() {
        if !seen.insert(bound.key) {
            return exec_err!("Duplicate Iceberg metric bound for field {}", bound.key);
        }
        let bytes = decoded
            .get(&bound.key)
            .map(Datum::to_bytes)
            .transpose()
            .map_err(DataFusionError::Execution)?;
        if bytes.as_deref() != Some(bound.value.as_ref()) {
            decoded.insert(
                bound.key,
                Datum::new(
                    PrimitiveType::Binary,
                    PrimitiveLiteral::Binary(bound.value.into_vec()),
                ),
            );
        }
    }
    Ok(())
}

async fn read_entries(
    store: &StoreContext,
    descriptor: &ManifestFile,
) -> Result<(ManifestMetadata, Vec<ManifestEntry>)> {
    let (object_store, path) = store.resolve(&descriptor.manifest_path)?;
    let bytes = object_store.get(&path).await?.bytes().await?;
    let (manifest_metadata, raw_entries) = Manifest::decode_avro_entries(&bytes, |_, value| {
        apache_avro::from_value::<RawEntry>(value)
            .map_err(|error| format!("Avro decode manifest bounds error: {error}"))
    })
    .map_err(DataFusionError::Execution)?;
    // Use the common inheritance implementation before changing status to EXISTING.
    let mut entries = manifest_entries(store, descriptor).await?;
    if raw_entries.len() != entries.len() {
        return exec_err!("Manifest entry count changed");
    }
    for (entry, raw) in entries.iter_mut().zip(raw_entries) {
        if EntrySelection::live().matches(entry) {
            restore_bounds(
                raw.data_file.lower_bounds,
                &mut entry.data_file.lower_bounds,
            )?;
            restore_bounds(
                raw.data_file.upper_bounds,
                &mut entry.data_file.upper_bounds,
            )?;
        }
    }
    Ok((manifest_metadata, entries))
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::spec::{DataFile, ManifestStatus, PartitionSpec, Schema};

    fn entry(index: i64, version: FormatVersion) -> ManifestEntry {
        let file: DataFile = serde_json::from_value(serde_json::json!({
            "content": "DATA", "file_path": format!("data/{index}.parquet"),
            "file_format": "PARQUET", "partition": [], "record_count": 2,
            "file_size_in_bytes": 100, "partition_spec_id": 0,
            "block_size_in_bytes": (version == FormatVersion::V1).then_some(67_108_864),
            "first_row_id": (version == FormatVersion::V3).then_some(index * 2)
        }))
        .expect("file");
        ManifestEntry::new(
            ManifestStatus::Added,
            Some(index + 10),
            Some(index),
            Some(index),
            file,
        )
    }

    #[test]
    fn rolling_rewrite_preserves_file_identity_and_entry_lineage() {
        for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let metadata = ManifestMetadata::new(
                Arc::new(Schema::builder().build().expect("schema")),
                0,
                PartitionSpec::unpartitioned_spec(),
                version,
                ManifestContentType::Data,
            );
            let entries = vec![entry(1, version), entry(2, version), entry(3, version)];
            for target_size in [1, DEFAULT_TARGET_SIZE] {
                let output =
                    encode_manifests(&metadata, &entries, target_size, 9, 20).expect("encode");
                assert_eq!(output.len(), if target_size == 1 { 3 } else { 1 });
                let mut actual = vec![];
                for (descriptor, bytes) in output {
                    assert_eq!(descriptor.added_files_count, Some(0));
                    assert_eq!(descriptor.deleted_files_count, Some(0));
                    assert_eq!(descriptor.added_snapshot_id, 20);
                    assert_eq!(descriptor.manifest_length, bytes.len() as i64);
                    let manifest = Manifest::parse_avro(&bytes).expect("manifest");
                    actual.extend(
                        manifest
                            .entries()
                            .iter()
                            .map(|entry| entry.as_ref().clone()),
                    );
                }
                for (actual, original) in actual.iter().zip(&entries) {
                    assert_eq!(actual.status, crate::spec::ManifestStatus::Existing);
                    assert_eq!(actual.snapshot_id, original.snapshot_id);
                    assert_eq!(actual.data_file, original.data_file);
                    if version != FormatVersion::V1 {
                        assert_eq!(actual.sequence_number, original.sequence_number);
                        assert_eq!(actual.file_sequence_number, original.file_sequence_number);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn rewrite_named_partition_types_preserves_entries_and_raw_metrics() {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        use crate::spec::{NestedField, Transform, Type};

        for (primitive, value) in [
            (
                PrimitiveType::Fixed(3),
                PrimitiveLiteral::Binary(b"abc".to_vec()),
            ),
            (
                PrimitiveType::Uuid,
                PrimitiveLiteral::UInt128(0x00112233_4455_6677_8899_aabbccddeeff),
            ),
            (
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(1234),
            ),
        ] {
            let store = StoreContext::new(
                Arc::new(InMemory::new()),
                &url::Url::parse("memory://bucket/table/").expect("URL"),
            )
            .expect("store");
            // Repeated fixed/UUID/decimal types also exercise named-schema references.
            let schema = Schema::builder()
                .with_fields([
                    Arc::new(NestedField::optional(
                        1,
                        "p",
                        Type::Primitive(primitive.clone()),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "q",
                        Type::Primitive(primitive.clone()),
                    )),
                ])
                .build()
                .expect("schema");
            let spec = PartitionSpec::builder()
                .add_field(1, "p", Transform::Identity)
                .add_field(2, "q", Transform::Identity)
                .build();
            let metadata = ManifestMetadata::new(
                Arc::new(schema),
                0,
                spec,
                FormatVersion::V2,
                ManifestContentType::Data,
            );
            let mut original = vec![];
            let mut live = vec![];
            for index in 1..=2 {
                let mut entry = entry(index, FormatVersion::V2);
                entry.data_file.partition = vec![Some(Literal::Primitive(value.clone())); 2];
                entry
                    .data_file
                    .lower_bounds
                    .insert(1, Datum::new(primitive.clone(), value.clone()));
                entry
                    .data_file
                    .upper_bounds
                    .insert(1, Datum::new(primitive.clone(), value.clone()));
                // This bound is deliberately absent from the schema and the typed scan projection.
                entry.data_file.lower_bounds.insert(
                    999,
                    Datum::new(
                        PrimitiveType::Binary,
                        PrimitiveLiteral::Binary(vec![0xff, 0, 0x80]),
                    ),
                );
                entry.data_file.key_metadata = Some(vec![1, 2, 3]);
                original.push(entry.clone());
                let mut writer = ManifestWriter::new(Some(index + 10), None, metadata.clone());
                writer.add_entry(entry);
                // The writer uses Iceberg's length-prefixed FixedByteBufferWriter layout,
                // while the declared schema says fixed. A bare Avro reader cannot read it.
                let bytes = writer.to_avro_bytes_v2().expect("manifest bytes");
                let path = format!("metadata/input-{index}.avro");
                let descriptor = writer
                    .into_manifest_file(path.clone(), index, index + 10)
                    .expect("descriptor");
                store
                    .prefixed
                    .put(&Path::from(path), bytes.into())
                    .await
                    .expect("write input");
                live.extend(
                    read_entries(&store, &descriptor)
                        .await
                        .expect("read input")
                        .1,
                );
            }
            assert_eq!(live, original);
            let output =
                encode_manifests(&metadata, &live, DEFAULT_TARGET_SIZE, 3, 20).expect("rewrite");
            assert_eq!(output.len(), 1);
            let (mut descriptor, bytes) = output.into_iter().next().expect("one manifest");
            descriptor.manifest_path = "metadata/rewritten.avro".into();
            store
                .prefixed
                .put(&Path::from(descriptor.manifest_path.as_str()), bytes.into())
                .await
                .expect("write output");
            for entry in &mut original {
                entry.status = ManifestStatus::Existing;
            }
            assert_eq!(
                read_entries(&store, &descriptor)
                    .await
                    .expect("read rewritten")
                    .1,
                original
            );
            assert_eq!(descriptor.existing_files_count, Some(2));
            assert_eq!(descriptor.min_sequence_number, 1);
        }
    }

    #[test]
    fn opaque_bounds_survive_typed_reader_projection() {
        let raw = vec![0xff, 0, 0x80];
        let mut decoded = HashMap::new();
        restore_bounds(
            Some(vec![RawBound {
                key: 2147483546,
                value: raw.clone().into(),
            }]),
            &mut decoded,
        )
        .expect("restore raw bytes");
        assert_eq!(decoded[&2147483546].to_bytes().expect("bytes"), raw);
        assert!(
            restore_bounds(
                Some(vec![
                    RawBound {
                        key: 1,
                        value: vec![1].into()
                    },
                    RawBound {
                        key: 1,
                        value: vec![2].into()
                    },
                ]),
                &mut decoded
            )
            .is_err()
        );
    }
}
