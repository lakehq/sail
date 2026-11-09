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
use crate::spec::manifest::deserialize_optional_vec;
use crate::spec::manifest_list::{ManifestListWriteContext, ManifestListWriter};
use crate::spec::snapshots::{SnapshotRetention, Summary};
use crate::spec::{
    DataContentType, Datum, FormatVersion, Literal, MAIN_BRANCH, Manifest, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestMetadata, ManifestStatus, ManifestWriter, Operation,
    PrimitiveLiteral, PrimitiveType, SnapshotBuilder, SnapshotReference, TableMetadata,
    TableRequirement, TableUpdate,
};
use crate::table::files::{
    EntrySelection, ManifestScope, SnapshotScope, inherit_manifest_entries, scan_manifests,
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

struct RewriteGroup {
    metadata: ManifestMetadata,
    sources: Vec<ManifestFile>,
    entries: Vec<(usize, usize, ManifestEntry)>,
    needs_normalization: bool,
}

struct EncodedManifest {
    descriptor: ManifestFile,
    bytes: Vec<u8>,
    range: std::ops::Range<usize>,
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
        self.prepare_with_entry_limit(store, table_url, metadata, output_schema, MAX_ENTRIES)
            .await
    }

    async fn prepare_with_entry_limit(
        &self,
        store: &StoreContext,
        table_url: &url::Url,
        metadata: &TableMetadata,
        output_schema: SchemaRef,
        max_entries: usize,
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
        let mut manifests = scan_manifests(
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
        let mut selected_bytes = manifests
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
        let mut remaining_entries = max_entries;
        let mut replaced = HashSet::new();
        for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
            // Retain the original schema when rewriting historical manifests: using only the
            // current schema would discard metrics for dropped fields on subsequent reads.
            let mut groups: BTreeMap<String, RewriteGroup> = BTreeMap::new();
            for descriptor in manifests
                .iter_mut()
                .filter(|m| m.content == content && selected.contains(&m.manifest_path))
            {
                let (mut manifest_metadata, entries, mut needs_normalization) =
                    read_entries(store, descriptor, &mut remaining_entries).await?;
                needs_normalization |= manifest_metadata.format_version != metadata.format_version;
                if metadata.format_version != FormatVersion::V1 && missing_counts(descriptor) {
                    needs_normalization = true;
                    populate_counts(descriptor, &entries)?;
                }
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
                let group = groups.entry(key).or_insert_with(|| RewriteGroup {
                    metadata: manifest_metadata,
                    sources: vec![],
                    entries: vec![],
                    needs_normalization: false,
                });
                group.needs_normalization |= needs_normalization;
                let source = group.sources.len();
                group.sources.push(descriptor.clone());
                for (ordinal, entry) in entries
                    .into_iter()
                    .enumerate()
                    .filter(|(_, entry)| EntrySelection::live().matches(entry))
                {
                    let is_data = entry.data_file.content == DataContentType::Data;
                    if is_data != (content == ManifestContentType::Data) {
                        return exec_err!("Manifest contains a file with incompatible content");
                    }
                    if first_row_id.is_some() && is_data && entry.data_file.first_row_id.is_none() {
                        return not_impl_err!(
                            "rewrite_manifests requires assigned data-file row lineage"
                        );
                    }
                    group.entries.push((source, ordinal, entry));
                }
            }
            for (_, group) in groups {
                if group.sources.len() == 1
                    && group.sources[0].manifest_length as u64 <= target_size
                {
                    continue;
                }
                let mut keyed = group
                    .entries
                    .into_iter()
                    .map(|(source, ordinal, entry)| {
                        let key = partition_key(&entry)?;
                        Ok((key, (source, ordinal), entry))
                    })
                    .collect::<Result<Vec<_>>>()?;
                keyed.sort_by(|left, right| left.0.cmp(&right.0));
                let (origins, entries): (Vec<_>, Vec<_>) = keyed
                    .into_iter()
                    .map(|(_, origin, entry)| (origin, entry))
                    .unzip();
                let candidates = encode_manifests(
                    &group.metadata,
                    &entries,
                    target_size,
                    sequence_number,
                    snapshot_id,
                )?;
                // Avoid rewriting identical groups, including indivisible oversized entries.
                // Equal manifest counts alone do not establish that clustering is unchanged.
                if !group.needs_normalization
                    && same_source_groups(&candidates, &origins, group.sources.len())
                {
                    continue;
                }
                replaced.extend(group.sources.into_iter().map(|source| source.manifest_path));
                live_entries += entries.len();
                for EncodedManifest {
                    mut descriptor,
                    bytes,
                    ..
                } in candidates
                {
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
        if replaced.is_empty() {
            return Ok(prepared);
        }
        // V1 lists may omit counts even after a table upgrade. A new V2+ list requires
        // actual counts for retained manifests, including those outside the selected spec.
        if metadata.format_version != FormatVersion::V1 {
            for descriptor in manifests.iter_mut().filter(|descriptor| {
                !replaced.contains(&descriptor.manifest_path) && missing_counts(descriptor)
            }) {
                let length = u64::try_from(descriptor.manifest_length)
                    .map_err(|_| DataFusionError::Execution("Negative manifest length".into()))?;
                selected_bytes = selected_bytes
                    .checked_add(length)
                    .ok_or_else(|| DataFusionError::Execution("Manifest size overflow".into()))?;
                if selected_bytes > MAX_INPUT_BYTES {
                    return not_impl_err!(
                        "rewrite_manifests supports at most {MAX_INPUT_BYTES} bytes of input manifests per call"
                    );
                }
                let (_, entries, _) =
                    read_entries(store, descriptor, &mut remaining_entries).await?;
                populate_counts(descriptor, &entries)?;
            }
        }
        let added_count = added.len();
        let mut list = ManifestListWriter::new();
        for manifest in added.into_iter().chain(
            manifests
                .into_iter()
                .filter(|m| !replaced.contains(&m.manifest_path)),
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
            .with_property("manifests-replaced", replaced.len().to_string())
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
        prepared.output = output(output_schema, replaced.len(), added_count)?;
        Ok(prepared)
    }
}

fn missing_counts(descriptor: &ManifestFile) -> bool {
    descriptor.added_files_count.is_none()
        || descriptor.existing_files_count.is_none()
        || descriptor.deleted_files_count.is_none()
        || descriptor.added_rows_count.is_none()
        || descriptor.existing_rows_count.is_none()
        || descriptor.deleted_rows_count.is_none()
}

fn populate_counts(descriptor: &mut ManifestFile, entries: &[ManifestEntry]) -> Result<()> {
    let mut counts = [(0i32, 0i64); 3];
    for entry in entries {
        let index = match entry.status {
            ManifestStatus::Added => 0,
            ManifestStatus::Existing => 1,
            ManifestStatus::Deleted => 2,
        };
        let (files, rows) = &mut counts[index];
        *files = files
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("Manifest file count overflow".into()))?;
        *rows = i64::try_from(entry.data_file.record_count)
            .ok()
            .and_then(|count| rows.checked_add(count))
            .ok_or_else(|| DataFusionError::Execution("Manifest row count overflow".into()))?;
    }
    descriptor.added_files_count.get_or_insert(counts[0].0);
    descriptor.existing_files_count.get_or_insert(counts[1].0);
    descriptor.deleted_files_count.get_or_insert(counts[2].0);
    descriptor.added_rows_count.get_or_insert(counts[0].1);
    descriptor.existing_rows_count.get_or_insert(counts[1].1);
    descriptor.deleted_rows_count.get_or_insert(counts[2].1);
    Ok(())
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

fn same_source_groups(
    candidates: &[EncodedManifest],
    origins: &[(usize, usize)],
    source_count: usize,
) -> bool {
    candidates.len() == source_count
        && candidates.iter().enumerate().all(|(source, candidate)| {
            origins[candidate.range.clone()]
                .iter()
                .enumerate()
                .all(|(ordinal, origin)| *origin == (source, ordinal))
        })
}

/// Split contiguous partition-clustered ranges using actual encoded size, including headers.
/// A single entry is indivisible and may exceed the target, as with a rolling writer.
fn encode_manifests(
    metadata: &ManifestMetadata,
    entries: &[ManifestEntry],
    target_size: u64,
    sequence_number: i64,
    snapshot_id: i64,
) -> Result<Vec<EncodedManifest>> {
    let mut pending = std::iter::once(0..entries.len()).collect::<Vec<_>>();
    let mut output = vec![];
    while let Some(range) = pending.pop() {
        let chunk = &entries[range.clone()];
        if chunk.is_empty() {
            continue;
        }
        let mut writer = ManifestWriter::new(Some(snapshot_id), None, metadata.clone());
        for entry in chunk {
            writer
                .add_existing_entry(entry.clone())
                .map_err(DataFusionError::Execution)?;
        }
        let bytes = writer
            .to_avro_bytes_v2()
            .map_err(DataFusionError::Execution)?;
        if bytes.len() as u64 > target_size && chunk.len() > 1 {
            let split = range.start + chunk.len() / 2;
            pending.push(split..range.end);
            pending.push(range.start..split);
            continue;
        }
        let mut descriptor = writer
            .into_manifest_file(String::new(), sequence_number, snapshot_id)
            .map_err(DataFusionError::Execution)?;
        descriptor.manifest_length = i64::try_from(bytes.len())
            .map_err(|_| DataFusionError::Execution("Manifest length exceeds int64".into()))?;
        output.push(EncodedManifest {
            descriptor,
            bytes,
            range,
        });
    }
    Ok(output)
}

#[derive(Deserialize)]
struct RawEntry {
    data_file: RawBounds,
}

#[derive(Deserialize)]
struct RawBounds {
    #[serde(default, deserialize_with = "deserialize_optional_vec")]
    lower_bounds: Option<Vec<RawBound>>,
    #[serde(default, deserialize_with = "deserialize_optional_vec")]
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
    remaining_entries: &mut usize,
) -> Result<(ManifestMetadata, Vec<ManifestEntry>, bool)> {
    if descriptor.key_metadata.is_some() {
        return not_impl_err!("rewrite_manifests does not support encrypted manifests");
    }
    let (object_store, path) = store.resolve(&descriptor.manifest_path)?;
    let bytes = object_store.get(&path).await?.bytes().await?;
    let (manifest_metadata, decoded) =
        Manifest::decode_avro_entries_with_limit(&bytes, *remaining_entries, |entry, value| {
            apache_avro::from_value::<RawEntry>(value)
                .map(|raw| (entry, raw))
                .map_err(|error| format!("Avro decode manifest bounds error: {error}"))
        })
        .map_err(DataFusionError::Execution)?;
    *remaining_entries -= decoded.len();
    let (mut entries, raw_entries): (Vec<_>, Vec<_>) = decoded.into_iter().unzip();
    let needs_normalization = entries.iter().any(|entry| {
        entry.status != ManifestStatus::Existing
            || entry.snapshot_id.is_none()
            || (manifest_metadata.format_version != FormatVersion::V1
                && (entry.sequence_number.is_none() || entry.file_sequence_number.is_none()))
            || (entry.data_file.content == DataContentType::Data
                && entry.data_file.first_row_id.is_none()
                && descriptor.first_row_id.is_some())
    });
    // Inherit using the source format, before upgrading or discarding deleted entries.
    inherit_manifest_entries(descriptor, manifest_metadata.format_version, &mut entries)?;
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
    Ok((manifest_metadata, entries, needs_normalization))
}

#[cfg(test)]
#[expect(clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use crate::spec::{DataFile, ManifestStatus, PartitionSpec, Schema};

    pub(crate) fn entry(index: i64, version: FormatVersion) -> ManifestEntry {
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

    pub(crate) async fn rewrite_fixture(
        version: FormatVersion,
        inputs: Vec<(i32, Vec<ManifestEntry>)>,
        unknown_counts: bool,
    ) -> (StoreContext, url::Url, TableMetadata, Vec<ManifestFile>) {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let url = url::Url::parse("memory://bucket/table/").expect("URL");
        let store = StoreContext::new(Arc::new(InMemory::new()), &url).expect("store");
        let schema = Schema::builder().build().expect("schema");
        let mut specs = vec![PartitionSpec::unpartitioned_spec()];
        let mut descriptors = vec![];
        let mut list = ManifestListWriter::new();
        for (index, (spec_id, entries)) in inputs.into_iter().enumerate() {
            let spec: PartitionSpec = serde_json::from_value(serde_json::json!({
                "spec-id": spec_id, "fields": []
            }))
            .expect("spec");
            if !specs.contains(&spec) {
                specs.push(spec.clone());
            }
            let mut writer = ManifestWriter::new(
                Some(10),
                None,
                ManifestMetadata::new(
                    Arc::new(schema.clone()),
                    0,
                    spec,
                    version,
                    ManifestContentType::Data,
                ),
            );
            for mut entry in entries {
                entry.data_file.partition_spec_id = spec_id;
                writer.add_entry(entry);
            }
            let bytes = writer.to_avro_bytes_v2().expect("manifest");
            let path = format!("metadata/input-{index}.avro");
            let mut descriptor = writer
                .into_manifest_file(path.clone(), 0, 10)
                .expect("descriptor");
            descriptor.manifest_length = bytes.len() as i64;
            if version == FormatVersion::V1 {
                descriptor.min_sequence_number = 0;
            }
            if unknown_counts {
                descriptor.added_files_count = None;
                descriptor.existing_files_count = None;
                descriptor.deleted_files_count = None;
                descriptor.added_rows_count = None;
                descriptor.existing_rows_count = None;
                descriptor.deleted_rows_count = None;
            }
            store
                .prefixed
                .put(&Path::from(path), bytes.into())
                .await
                .expect("write");
            list.append(descriptor.clone());
            descriptors.push(descriptor);
        }
        let list_bytes = list
            .to_bytes_with_context(ManifestListWriteContext {
                version,
                snapshot_id: 10,
                parent_snapshot_id: None,
                sequence_number: 0,
                first_row_id: None,
            })
            .expect("list");
        store
            .prefixed
            .put(&Path::from("metadata/list.avro"), list_bytes.into())
            .await
            .expect("write list");
        let metadata = serde_json::from_value(serde_json::json!({
            "format-version": version,
            "location": url.as_str(),
            "last-updated-ms": 0,
            "last-column-id": 0,
            "schemas": [schema],
            "current-schema-id": 0,
            "partition-specs": specs,
            "current-snapshot-id": 10,
            "snapshots": [{
                "snapshot-id": 10, "sequence-number": 0, "timestamp-ms": 0,
                "manifest-list": "metadata/list.avro", "summary": {"operation": "append"}
            }]
        }))
        .expect("table metadata");
        (store, url, metadata, descriptors)
    }

    pub(crate) fn operation() -> RewriteManifests {
        RewriteManifests { spec_id: None }
    }

    pub(crate) fn output_schema() -> SchemaRef {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![
            Field::new("rewritten_manifests_count", DataType::Int32, false),
            Field::new("added_manifests_count", DataType::Int32, false),
        ]))
    }

    #[tokio::test]
    async fn optional_v1_counts_survive_rewrite_and_upgrade() {
        use futures::TryStreamExt;

        use crate::spec::ManifestList;

        let retained_entries = [
            ManifestStatus::Added,
            ManifestStatus::Existing,
            ManifestStatus::Deleted,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, status)| {
            let mut entry = entry(index as i64 + 3, FormatVersion::V1);
            entry.status = status;
            entry
        })
        .collect::<Vec<_>>();
        for upgraded in [false, true] {
            let (store, url, mut metadata, descriptors) = rewrite_fixture(
                FormatVersion::V1,
                vec![
                    (0, vec![entry(1, FormatVersion::V1)]),
                    (0, vec![entry(2, FormatVersion::V1)]),
                    (1, retained_entries.clone()),
                ],
                true,
            )
            .await;
            if upgraded {
                metadata.format_version = FormatVersion::V2;
                let before = store
                    .prefixed
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("objects");
                let error = RewriteManifests { spec_id: None }
                    .prepare_with_entry_limit(&store, &url, &metadata, output_schema(), 4)
                    .await
                    .err()
                    .expect("retained entries share budget");
                assert!(error.to_string().contains("Manifest entry limit exceeded"));
                assert_eq!(
                    store
                        .prefixed
                        .list(None)
                        .try_collect::<Vec<_>>()
                        .await
                        .expect("objects"),
                    before
                );
            }
            let counting = Arc::new(crate::io::test_store::TestStore::new(store.base.clone()));
            let store = StoreContext::new(counting.clone(), &url).expect("counting store");
            let prepared = RewriteManifests { spec_id: None }
                .prepare_with_entry_limit(&store, &url, &metadata, output_schema(), 5)
                .await
                .expect("prepare");
            let gets = counting.gets.lock().expect("get counts");
            assert_eq!(gets.len(), if upgraded { 4 } else { 3 });
            assert!(gets.values().all(|count| *count == 1), "{gets:?}");
            drop(gets);
            assert_eq!(
                prepared
                    .output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("count")
                    .value(0),
                2
            );
            assert_eq!(
                prepared
                    .output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("count")
                    .value(0),
                1
            );
            let (_, list) = prepared.manifests.last().expect("list");
            let list =
                ManifestList::parse_with_version(list, metadata.format_version).expect("list");
            assert_eq!(list.entries().len(), 2);
            let retained = list
                .entries()
                .iter()
                .find(|m| m.manifest_path == descriptors[2].manifest_path)
                .expect("retained");
            if upgraded {
                assert_eq!(retained.added_files_count, Some(1));
                assert_eq!(retained.added_rows_count, Some(2));
                assert_eq!(retained.existing_files_count, Some(1));
                assert_eq!(retained.deleted_rows_count, Some(2));
                let mut expected = descriptors[2].clone();
                expected.added_files_count = Some(1);
                expected.existing_files_count = Some(1);
                expected.deleted_files_count = Some(1);
                expected.added_rows_count = Some(2);
                expected.existing_rows_count = Some(2);
                expected.deleted_rows_count = Some(2);
                assert_eq!(retained, &expected);
            } else {
                assert_eq!(retained, &descriptors[2]);
            }
        }
        let (store, url, metadata, _) = rewrite_fixture(
            FormatVersion::V1,
            vec![(0, vec![entry(1, FormatVersion::V1)])],
            true,
        )
        .await;
        let prepared = RewriteManifests { spec_id: None }
            .prepare(&store, &url, &metadata, output_schema())
            .await
            .expect("no-op");
        assert!(prepared.updates.is_empty());
        assert!(prepared.requirements.is_empty());
        assert!(prepared.manifests.is_empty());
    }

    #[tokio::test]
    async fn entry_budget_counts_deleted_records_across_manifests_before_staging() {
        use futures::TryStreamExt;

        for statuses in [
            [
                ManifestStatus::Added,
                ManifestStatus::Existing,
                ManifestStatus::Added,
            ],
            [ManifestStatus::Deleted; 3],
            [
                ManifestStatus::Deleted,
                ManifestStatus::Added,
                ManifestStatus::Existing,
            ],
        ] {
            let entries = statuses
                .into_iter()
                .enumerate()
                .map(|(index, status)| {
                    let mut entry = entry(index as i64, FormatVersion::V2);
                    entry.status = status;
                    entry
                })
                .collect::<Vec<_>>();
            // Both a single oversized manifest and a cumulative cross-manifest limit.
            for inputs in [
                vec![(0, entries.clone())],
                vec![(0, entries[..2].to_vec()), (0, entries[2..].to_vec())],
            ] {
                let (store, url, mut metadata, _) =
                    rewrite_fixture(FormatVersion::V2, inputs, false).await;
                metadata
                    .properties
                    .insert(TARGET_SIZE_PROPERTY.into(), "1".into());
                let before = store
                    .prefixed
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("objects");
                let metadata_before = serde_json::to_value(&metadata).expect("metadata");
                for limit in [0, 2, 3] {
                    let result = RewriteManifests { spec_id: None }
                        .prepare_with_entry_limit(&store, &url, &metadata, output_schema(), limit)
                        .await;
                    if limit < 3 {
                        let error = result.err().expect("entry limit");
                        assert!(
                            error.to_string().contains("Manifest entry limit exceeded"),
                            "{error}"
                        );
                    } else {
                        let prepared = result.expect("exact budget");
                        assert!(!prepared.updates.is_empty());
                        let snapshot = prepared
                            .updates
                            .iter()
                            .find_map(|update| match update {
                                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                                _ => None,
                            })
                            .expect("snapshot");
                        let live = statuses
                            .iter()
                            .filter(|status| **status != ManifestStatus::Deleted)
                            .count();
                        assert_eq!(
                            snapshot.summary().additional_properties["entries-processed"],
                            live.to_string()
                        );
                    }
                    assert_eq!(
                        store
                            .prefixed
                            .list(None)
                            .try_collect::<Vec<_>>()
                            .await
                            .expect("objects"),
                        before
                    );
                    assert_eq!(
                        serde_json::to_value(&metadata).expect("metadata"),
                        metadata_before
                    );
                }
            }
        }
    }

    #[test]
    fn equal_counts_only_skip_identical_source_groups_and_entry_order() {
        let metadata = ManifestMetadata::new(
            Arc::new(Schema::builder().build().expect("schema")),
            0,
            PartitionSpec::unpartitioned_spec(),
            FormatVersion::V2,
            ManifestContentType::Data,
        );
        let entries = (0..4)
            .map(|i| entry(i, FormatVersion::V2))
            .collect::<Vec<_>>();
        let target = encode_manifests(&metadata, &entries[..2], u64::MAX, 1, 20).expect("encode")[0]
            .bytes
            .len() as u64;
        let chunks = encode_manifests(&metadata, &entries, target, 1, 20).expect("chunks");
        assert_eq!(chunks.len(), 2);
        assert!(same_source_groups(
            &chunks,
            &[(0, 0), (0, 1), (1, 0), (1, 1)],
            2
        ));
        assert!(!same_source_groups(
            &chunks,
            &[(0, 0), (1, 0), (1, 1), (0, 1)],
            2
        ));
        assert!(!same_source_groups(
            &chunks,
            &[(0, 1), (0, 0), (1, 0), (1, 1)],
            2
        ));
    }

    #[tokio::test]
    async fn normalized_equal_count_groups_are_reclustered_before_becoming_stable() {
        use object_store::path::Path;

        use crate::spec::{NestedField, Transform, Type};

        let (store, url, mut table, _) = rewrite_fixture(FormatVersion::V2, vec![], false).await;
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "p",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder()
            .add_field(1, "p", Transform::Identity)
            .build();
        table.schemas = vec![schema.clone()];
        table.partition_specs = vec![spec.clone()];
        let header = ManifestMetadata::new(
            Arc::new(schema),
            0,
            spec,
            FormatVersion::V2,
            ManifestContentType::Data,
        );
        let entries = (1..=4)
            .map(|i| {
                let mut entry = entry(i, FormatVersion::V2);
                entry.status = ManifestStatus::Existing;
                entry.data_file.partition =
                    vec![Some(Literal::Primitive(PrimitiveLiteral::Long(i)))];
                entry
            })
            .collect::<Vec<_>>();
        // Measure the exact normalized candidate pairs, not their ADDED source encoding.
        let target = [&entries[..2], &entries[2..]]
            .into_iter()
            .map(|entries| {
                encode_manifests(&header, entries, u64::MAX, 1, 10).expect("pair")[0]
                    .bytes
                    .len() as u64
            })
            .max()
            .expect("target");
        let mut list = ManifestListWriter::new();
        for (index, group) in [[0, 3], [1, 2]].into_iter().enumerate() {
            let entries = group.map(|index| entries[index].clone());
            let EncodedManifest {
                mut descriptor,
                bytes,
                ..
            } = encode_manifests(&header, &entries, u64::MAX, 1, 10)
                .expect("source")
                .remove(0);
            descriptor.manifest_path = format!("metadata/source-{index}.avro");
            store
                .prefixed
                .put(&Path::from(descriptor.manifest_path.as_str()), bytes.into())
                .await
                .expect("source");
            list.append(descriptor);
        }
        store
            .prefixed
            .put(
                &Path::from("metadata/list.avro"),
                list.to_bytes_with_context(ManifestListWriteContext {
                    version: FormatVersion::V2,
                    snapshot_id: 10,
                    parent_snapshot_id: None,
                    sequence_number: 1,
                    first_row_id: None,
                })
                .expect("list")
                .into(),
            )
            .await
            .expect("write list");
        table
            .properties
            .insert(TARGET_SIZE_PROPERTY.into(), target.to_string());
        let prepared = operation()
            .prepare(&store, &url, &table, output_schema())
            .await
            .expect("regroup");
        assert_eq!(
            prepared.output,
            output(output_schema(), 2, 2).expect("output")
        );
        for (path, bytes) in &prepared.manifests {
            store
                .prefixed
                .put(&Path::from(path.as_str()), bytes.clone().into())
                .await
                .expect("stage");
        }
        crate::operations::metadata_commit::apply_snapshot_updates(
            &url,
            &mut table,
            &prepared.updates,
            crate::operations::metadata_commit::PreviousMetadataFile::ObjectPath(
                "metadata/v1.metadata.json",
            ),
        )
        .expect("apply");
        let repeated = operation()
            .prepare(&store, &url, &table, output_schema())
            .await
            .expect("repeat");
        assert!(repeated.manifests.is_empty());
        assert!(repeated.updates.is_empty());
        assert_eq!(
            repeated.output,
            output(output_schema(), 0, 0).expect("output")
        );
    }

    #[tokio::test]
    async fn oversized_existing_singletons_are_stable_but_added_entries_are_normalized() {
        for status in [ManifestStatus::Existing, ManifestStatus::Added] {
            for count in [1, 4] {
                let inputs = (0..count)
                    .map(|i| {
                        let mut entry = entry(i, FormatVersion::V2);
                        entry.status = status;
                        (0, vec![entry])
                    })
                    .collect();
                let (store, url, mut metadata, _) =
                    rewrite_fixture(FormatVersion::V2, inputs, false).await;
                metadata
                    .properties
                    .insert(TARGET_SIZE_PROPERTY.into(), "1".into());
                let prepared = RewriteManifests { spec_id: None }
                    .prepare(&store, &url, &metadata, output_schema())
                    .await
                    .expect("prepare");
                if status == ManifestStatus::Existing {
                    assert!(prepared.manifests.is_empty());
                    assert!(prepared.updates.is_empty());
                    assert!(prepared.requirements.is_empty());
                    assert_eq!(
                        prepared.output,
                        output(output_schema(), 0, 0).expect("output")
                    );
                } else {
                    assert_eq!(prepared.manifests.len(), count as usize + 1);
                    assert!(!prepared.updates.is_empty());
                }
            }
        }
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
                for EncodedManifest {
                    descriptor, bytes, ..
                } in output
                {
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
            let mut remaining_entries = MAX_ENTRIES;
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
                    read_entries(&store, &descriptor, &mut remaining_entries)
                        .await
                        .expect("read input")
                        .1,
                );
            }
            assert_eq!(live, original);
            let output =
                encode_manifests(&metadata, &live, DEFAULT_TARGET_SIZE, 3, 20).expect("rewrite");
            assert_eq!(output.len(), 1);
            let EncodedManifest {
                mut descriptor,
                bytes,
                ..
            } = output.into_iter().next().expect("one manifest");
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
                read_entries(&store, &descriptor, &mut remaining_entries)
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
    fn raw_bounds_accept_optional_avro_encodings() {
        use apache_avro::types::Value;

        let bound = Value::Record(vec![
            ("key".into(), Value::Int(999)),
            ("value".into(), Value::Bytes(vec![0xff, 0, 0x80])),
        ]);
        let array = Value::Array(vec![bound.clone()]);
        for value in [
            Some(array.clone()),
            Some(Value::Union(1, Box::new(array))),
            Some(Value::Null),
            Some(Value::Union(0, Box::new(Value::Null))),
            None,
        ] {
            let fields = value
                .clone()
                .map(|value| {
                    vec![
                        ("lower_bounds".into(), value.clone()),
                        ("upper_bounds".into(), value),
                    ]
                })
                .unwrap_or_default();
            let raw: RawBounds = apache_avro::from_value(&Value::Record(fields)).expect("bounds");
            for bounds in [raw.lower_bounds, raw.upper_bounds] {
                let mut decoded = HashMap::new();
                restore_bounds(bounds, &mut decoded).expect("restore");
                if matches!(value, Some(Value::Array(_) | Value::Union(1, _))) {
                    assert_eq!(
                        decoded[&999].to_bytes().expect("bytes"),
                        vec![0xff, 0, 0x80]
                    );
                } else {
                    assert!(decoded.is_empty());
                }
            }
        }
        for field in ["lower_bounds", "upper_bounds"] {
            for value in [
                Value::String("invalid".into()),
                Value::Array(vec![Value::Record(vec![("key".into(), Value::Int(1))])]),
                Value::Array(vec![Value::Record(vec![
                    ("key".into(), Value::String("1".into())),
                    ("value".into(), Value::Bytes(vec![1])),
                ])]),
            ] {
                assert!(
                    apache_avro::from_value::<RawBounds>(&Value::Record(vec![(
                        field.into(),
                        value
                    ),]))
                    .is_err()
                );
            }
            let raw: RawBounds = apache_avro::from_value(&Value::Record(vec![(
                field.into(),
                Value::Array(vec![bound.clone(), bound.clone()]),
            )]))
            .expect("duplicate bounds decode");
            assert!(
                restore_bounds(raw.lower_bounds.or(raw.upper_bounds), &mut HashMap::new()).is_err()
            );
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
