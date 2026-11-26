// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, not_impl_err, plan_err};
use url::Url;

use super::manifest::{missing_counts, populate_counts, read_entries};
use super::snapshot::operation::{
    PreparedSnapshotOperation, ancestor_chain, main_snapshot_id, prepare_main_reference_update,
    snapshot_change_batch,
};
use crate::io::{StoreContext, load_snapshot_manifest_list};
use crate::operations::snapshot::assign_parent_row_ids;
use crate::operations::write::partition::build_partition_dir;
use crate::spec::delete_index::PartitionKey;
use crate::spec::manifest_list::{ManifestListWriteContext, ManifestListWriter};
use crate::spec::partition::UnboundPartitionSpec;
use crate::spec::schema::Schema;
use crate::spec::snapshots::summary as keys;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, ManifestWriter, Operation, Snapshot, SnapshotBuilder, Summary, TableMetadata,
    TableRequirement, TableUpdate,
};
use crate::table::files::manifest_entries;
use crate::table::metadata_loader::table_metadata_location;

const STAGED_WAP_ID: &str = "wap.id";
const PUBLISHED_WAP_ID: &str = "published-wap-id";
const SOURCE_SNAPSHOT_ID: &str = "source-snapshot-id";

pub(crate) enum CherryPick {
    Snapshot(i64),
    Wap(String),
}

impl CherryPick {
    pub(super) fn resolve<'a>(&self, metadata: &'a TableMetadata) -> Result<&'a Snapshot> {
        match self {
            Self::Snapshot(id) => metadata
                .snapshots
                .iter()
                .find(|s| s.snapshot_id == *id)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Cannot cherry-pick unknown snapshot ID: {id}"))
                }),
            Self::Wap(id) => {
                let mut matches = metadata
                    .snapshots
                    .iter()
                    .filter(|s| s.summary.additional_properties.get(STAGED_WAP_ID) == Some(id));
                let snapshot = matches.next().ok_or_else(|| {
                    DataFusionError::Plan(format!("Cannot apply unknown WAP ID '{id}'"))
                })?;
                if matches.next().is_some() {
                    return plan_err!(
                        "Cannot apply non-unique WAP ID. Found multiple snapshots with WAP ID '{id}'"
                    );
                }
                Ok(snapshot)
            }
        }
    }
}

// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/CherryPickOperation.java#L67-L213
pub(super) async fn prepare(
    store: &StoreContext,
    url: &Url,
    metadata: &TableMetadata,
    source: &Snapshot,
    schema: SchemaRef,
    catalog_commit: bool,
) -> Result<PreparedSnapshotOperation> {
    let id = source.snapshot_id;
    let current = main_snapshot_id(metadata);
    let ancestors = current
        .map(|id| ancestor_chain(metadata, id))
        .transpose()?
        .unwrap_or_default();
    let props = &source.summary.additional_properties;
    let dynamic = source.summary.operation == Operation::Overwrite
        && props
            .get(keys::REPLACE_PARTITIONS)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let fast_forward = current == source.parent_snapshot_id;
    if dynamic
        && source
            .parent_snapshot_id
            .is_some_and(|id| !ancestors.iter().any(|s| s.snapshot_id == id))
    {
        return plan_err!(
            "Cannot cherry-pick overwrite not based on an ancestor of the current state: {id}"
        );
    }
    if source.summary.operation == Operation::Append || dynamic {
        if let Some(wap) = props.get(STAGED_WAP_ID).filter(|v| !v.is_empty())
            && ancestors.iter().any(|s| {
                [STAGED_WAP_ID, PUBLISHED_WAP_ID]
                    .iter()
                    .any(|k| s.summary.additional_properties.get(*k) == Some(wap))
            })
        {
            return plan_err!(
                "Duplicate request to cherry pick wap id that was published already: {wap}"
            );
        }
    } else if !fast_forward {
        return plan_err!(
            "Cannot cherry-pick snapshot {id}: not append, dynamic overwrite, or fast-forward"
        );
    }
    if fast_forward {
        let output = snapshot_change_batch(schema, Some(id), id, false)?;
        return prepare_main_reference_update(metadata, current, id, output);
    }
    let source_id = id.to_string();
    for ancestor in &ancestors {
        if ancestor.snapshot_id == id {
            return plan_err!("Cannot cherrypick snapshot {id}: already an ancestor");
        }
        if ancestor
            .summary
            .additional_properties
            .get(SOURCE_SNAPSHOT_ID)
            == Some(&source_id)
        {
            return plan_err!(
                "Cannot cherrypick snapshot {id}: already picked to create ancestor {}",
                ancestor.snapshot_id
            );
        }
    }

    if !metadata.encryption_keys.is_empty() {
        return not_impl_err!("Cherry-pick cannot write encrypted Iceberg metadata");
    }
    let snapshot_id = crate::utils::snapshot_id::generate_unique_snapshot_id(metadata);
    let sequence_number = match metadata.format_version {
        FormatVersion::V1 => 0,
        _ => metadata
            .last_sequence_number
            .checked_add(1)
            .ok_or_else(|| exec_datafusion_err!("Iceberg sequence number overflow"))?,
    };
    let output = snapshot_change_batch(schema, Some(id), snapshot_id, false)?;
    let mut prepared = prepare_main_reference_update(metadata, current, snapshot_id, output)?;
    let mut replay = Replay {
        url,
        context: ManifestListWriteContext {
            version: metadata.format_version,
            snapshot_id,
            parent_snapshot_id: current,
            sequence_number,
            first_row_id: (metadata.format_version == FormatVersion::V3)
                .then_some(metadata.next_row_id.unwrap_or(0)),
        },
        manifests: Vec::new(),
        artifacts: Vec::new(),
        deleted_paths: HashSet::new(),
        partitions: HashSet::new(),
        partition_metrics: HashMap::new(),
        partition_limit: metadata
            .properties
            .get("write.summary.partition-limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        metrics: HashMap::new(),
        replaced_manifests: 0,
    };
    replay.pick_changes(store, source, dynamic).await?;
    if dynamic {
        for ancestor in ancestors
            .iter()
            .take_while(|s| Some(s.snapshot_id) != source.parent_snapshot_id)
        {
            replay.validate_partitions(store, ancestor).await?;
        }
    }
    replay
        .apply_to_parent(
            store,
            ancestors.first().copied(),
            metadata.last_sequence_number,
        )
        .await?;
    let mut summary = replay.summary(source, ancestors.first().copied());
    summary = summary.with_property(SOURCE_SNAPSHOT_ID, source_id);
    if let Some(wap) = props.get(STAGED_WAP_ID) {
        summary = summary.with_property(PUBLISHED_WAP_ID, wap);
    }
    let list_path = format!("metadata/snap-{snapshot_id}-{}.avro", uuid::Uuid::new_v4());
    let mut snapshot = SnapshotBuilder::new()
        .with_snapshot_id(snapshot_id)
        .with_schema_id(metadata.current_schema_id)
        .with_sequence_number(sequence_number)
        .with_manifest_list(table_metadata_location(url, &list_path)?)
        .with_summary(summary);
    if let Some(parent) = current {
        snapshot = snapshot.with_parent_snapshot_id(parent);
    }
    if let Some(start) = replay.context.first_row_id {
        let next = assign_parent_row_ids(&mut replay.manifests, start)
            .map_err(DataFusionError::Execution)?;
        snapshot = snapshot
            .with_first_row_id(start)
            .with_added_rows(next - start);
    }
    let mut list = ManifestListWriter::new();
    for manifest in replay.manifests {
        list.append(manifest);
    }
    let bytes = list
        .to_bytes_with_context(replay.context)
        .map_err(DataFusionError::Execution)?;
    replay.artifacts.push((list_path, bytes));
    prepared.manifests = replay.artifacts;
    prepared.updates.insert(
        0,
        TableUpdate::AddSnapshot {
            snapshot: snapshot.build().map_err(DataFusionError::Execution)?,
        },
    );
    prepared
        .requirements
        .push(TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id,
        });
    if catalog_commit {
        // REST has no assert-snapshot-exists requirement. Setting a tag validates
        // its target while applying the updates; removing it in the same commit
        // leaves the table's refs unchanged. A concurrent expiration must win or
        // lose atomically with the publication that reuses the source files.
        let guard = format!("sail-cherrypick-{}", uuid::Uuid::new_v4());
        prepared
            .requirements
            .push(TableRequirement::RefSnapshotIdMatch {
                r#ref: guard.clone(),
                snapshot_id: None,
            });
        prepared.updates.insert(
            0,
            TableUpdate::SetSnapshotRef {
                ref_name: guard.clone(),
                reference: crate::spec::SnapshotReference {
                    snapshot_id: id,
                    retention: crate::spec::snapshots::SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                },
            },
        );
        prepared
            .updates
            .push(TableUpdate::RemoveSnapshotRef { ref_name: guard });
    }
    Ok(prepared)
}

struct Replay<'a> {
    url: &'a Url,
    context: ManifestListWriteContext,
    manifests: Vec<ManifestFile>,
    artifacts: Vec<(String, Vec<u8>)>,
    deleted_paths: HashSet<String>,
    partitions: HashSet<PartitionKey>,
    partition_metrics: HashMap<String, HashMap<&'static str, u64>>,
    partition_limit: usize,
    metrics: HashMap<&'static str, u64>,
    replaced_manifests: usize,
}

impl Replay<'_> {
    async fn manifest_list(
        &self,
        store: &StoreContext,
        snapshot: &Snapshot,
    ) -> Result<Vec<ManifestFile>> {
        if snapshot.key_id.is_some() {
            return not_impl_err!("Cherry-pick cannot read encrypted Iceberg manifest lists");
        }
        Ok(load_snapshot_manifest_list(store, snapshot)
            .await?
            .into_entries())
    }

    async fn pick_changes(
        &mut self,
        store: &StoreContext,
        source: &Snapshot,
        dynamic: bool,
    ) -> Result<()> {
        let mut added_paths = HashSet::new();
        for descriptor in self
            .manifest_list(store, source)
            .await?
            .into_iter()
            .filter(|m| {
                m.content == ManifestContentType::Data && m.added_snapshot_id == source.snapshot_id
            })
        {
            let (mut header, entries, _) = read_entries(store, &descriptor, usize::MAX).await?;
            header.format_version = self.context.version;
            let spec = UnboundPartitionSpec::from(&header.partition_spec);
            let schema = header.schema.clone();
            let mut writer = ManifestWriter::new(Some(self.context.snapshot_id), None, header);
            let mut has_additions = false;
            for entry in entries {
                let mut file = entry.data_file;
                match entry.status {
                    ManifestStatus::Added
                        if added_paths.insert((file.partition_spec_id, file.file_path.clone())) =>
                    {
                        // Replayed additions inherit fresh row IDs from the publishing snapshot.
                        file.first_row_id = None;
                        self.record(&file, true, &spec, &schema)?;
                        writer.add(file);
                        has_additions = true;
                    }
                    ManifestStatus::Deleted if dynamic => {
                        self.deleted_paths.insert(file.file_path);
                    }
                    _ => {}
                }
            }
            if has_additions {
                self.write_manifest(writer)?;
            }
        }
        Ok(())
    }

    async fn validate_partitions(&self, store: &StoreContext, since: &Snapshot) -> Result<()> {
        for manifest in self
            .manifest_list(store, since)
            .await?
            .into_iter()
            .filter(|m| {
                m.content == ManifestContentType::Data && m.added_snapshot_id == since.snapshot_id
            })
        {
            for entry in manifest_entries(store, &manifest).await? {
                let file = entry.data_file;
                if entry.status == ManifestStatus::Added
                    && self
                        .partitions
                        .contains(&PartitionKey::new(file.partition_spec_id, &file.partition))
                {
                    return plan_err!(
                        "Cannot cherry-pick replace partitions with changed partition: {:?}",
                        file.partition
                    );
                }
            }
        }
        Ok(())
    }

    async fn apply_to_parent(
        &mut self,
        store: &StoreContext,
        parent: Option<&Snapshot>,
        last_sequence: i64,
    ) -> Result<()> {
        let deleted_paths = std::mem::take(&mut self.deleted_paths);
        let mut missing = deleted_paths.clone();
        let parents = match parent {
            Some(snapshot) => self.manifest_list(store, snapshot).await?,
            None => Vec::new(),
        };
        let (data, deletes): (Vec<_>, Vec<_>) = parents
            .into_iter()
            .partition(|m| m.content == ManifestContentType::Data);
        for descriptor in data {
            self.filter_manifest(store, descriptor, !deleted_paths.is_empty(), |entry| {
                let path = &entry.data_file.file_path;
                missing.remove(path);
                deleted_paths.contains(path)
            })
            .await?;
        }
        if !missing.is_empty() {
            return plan_err!(
                "Cannot cherry-pick overwrite: missing required files to delete: {missing:?}"
            );
        }
        let min_sequence = self
            .manifests
            .iter()
            .map(|m| m.min_sequence_number)
            .fold(last_sequence, i64::min);
        for descriptor in deletes {
            let may_remove =
                descriptor.min_sequence_number < min_sequence || !deleted_paths.is_empty();
            self.filter_manifest(store, descriptor, may_remove, |entry| {
                entry
                    .sequence_number
                    .is_some_and(|seq| seq > 0 && seq < min_sequence)
                    || (entry.data_file.is_deletion_vector()
                        && entry
                            .data_file
                            .referenced_data_file
                            .as_ref()
                            .is_some_and(|path| deleted_paths.contains(path)))
            })
            .await?;
        }
        Ok(())
    }

    async fn filter_manifest(
        &mut self,
        store: &StoreContext,
        mut descriptor: ManifestFile,
        may_remove: bool,
        mut remove: impl FnMut(&ManifestEntry) -> bool,
    ) -> Result<()> {
        if descriptor.added_files_count == Some(0) && descriptor.existing_files_count == Some(0) {
            return Ok(());
        }
        let fill_counts = self.context.version != FormatVersion::V1 && missing_counts(&descriptor);
        if !may_remove && !fill_counts {
            self.manifests.push(descriptor);
            return Ok(());
        }
        let (mut header, entries, _) = read_entries(store, &descriptor, usize::MAX).await?;
        if fill_counts {
            populate_counts(&mut descriptor, &entries)?;
        }
        let (deleted, kept): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .filter(|e| e.status != ManifestStatus::Deleted)
            .partition(&mut remove);
        if deleted.is_empty() {
            self.manifests.push(descriptor);
            return Ok(());
        }
        header.format_version = self.context.version;
        let spec = UnboundPartitionSpec::from(&header.partition_spec);
        let schema = header.schema.clone();
        let mut writer = ManifestWriter::new(Some(self.context.snapshot_id), None, header);
        for entry in kept {
            writer
                .add_existing_entry(entry)
                .map_err(DataFusionError::Execution)?;
        }
        for entry in deleted {
            self.record(&entry.data_file, false, &spec, &schema)?;
            writer
                .add_deleted_entry(entry)
                .map_err(DataFusionError::Execution)?;
        }
        self.replaced_manifests += 1;
        self.write_manifest(writer)
    }

    fn write_manifest(&mut self, writer: ManifestWriter) -> Result<()> {
        let path = format!("metadata/{}.avro", uuid::Uuid::new_v4());
        let bytes = writer
            .to_avro_bytes_v2()
            .map_err(DataFusionError::Execution)?;
        let mut descriptor = writer
            .into_manifest_file(
                table_metadata_location(self.url, &path)?,
                self.context.sequence_number,
                self.context.snapshot_id,
            )
            .map_err(DataFusionError::Execution)?;
        descriptor.manifest_length = bytes.len() as i64;
        self.manifests.push(descriptor);
        self.artifacts.push((path, bytes));
        Ok(())
    }

    fn record(
        &mut self,
        file: &DataFile,
        added: bool,
        spec: &UnboundPartitionSpec,
        schema: &Schema,
    ) -> Result<()> {
        self.partitions
            .insert(PartitionKey::new(file.partition_spec_id, &file.partition));
        let path = build_partition_dir(spec, schema, &file.partition)
            .map_err(DataFusionError::Execution)?;
        let include_details =
            self.partition_metrics.len() <= self.partition_limit && self.partition_limit > 0;
        let partition = self.partition_metrics.entry(path).or_default();
        let mut count = |key, value| -> Result<()> {
            let n = self.metrics.entry(key).or_default();
            *n = n
                .checked_add(value)
                .ok_or_else(|| exec_datafusion_err!("Iceberg snapshot summary overflow: {key}"))?;
            if include_details {
                *partition.entry(key).or_default() += value;
            }
            Ok(())
        };
        let bytes = match file.content_size_in_bytes {
            Some(size) => u64::try_from(size)
                .map_err(|_| exec_datafusion_err!("Invalid Iceberg delete content size"))?,
            None => file.file_size_in_bytes,
        };
        if added {
            count("added-files-size", bytes)?;
            count(keys::ADDED_DATA_FILES, 1)?;
            return count(keys::ADDED_RECORDS, file.record_count);
        }
        count(keys::REMOVED_FILES_SIZE, bytes)?;
        match file.content {
            DataContentType::Data => {
                count(keys::DELETED_DATA_FILES, 1)?;
                count(keys::DELETED_RECORDS, file.record_count)
            }
            DataContentType::PositionDeletes => {
                count(keys::REMOVED_DELETE_FILES, 1)?;
                count(
                    if file.is_deletion_vector() {
                        keys::REMOVED_DVS
                    } else {
                        keys::REMOVED_POSITION_DELETE_FILES
                    },
                    1,
                )?;
                count(keys::REMOVED_POSITION_DELETES, file.record_count)
            }
            DataContentType::EqualityDeletes => {
                count(keys::REMOVED_DELETE_FILES, 1)?;
                count(keys::REMOVED_EQUALITY_DELETE_FILES, 1)?;
                count(keys::REMOVED_EQUALITY_DELETES, file.record_count)
            }
        }
    }

    fn summary(&self, source: &Snapshot, parent: Option<&Snapshot>) -> Summary {
        let mut summary = Summary::new(source.summary.operation.clone())
            .with_property("changed-partition-count", self.partition_metrics.len())
            .with_property("manifests-created", self.artifacts.len())
            .with_property(
                "manifests-kept",
                self.manifests.len() - self.artifacts.len(),
            )
            .with_property("manifests-replaced", self.replaced_manifests);
        if !self.partition_metrics.is_empty()
            && self.partition_metrics.len() <= self.partition_limit
        {
            summary = summary.with_property("partition-summaries-included", true);
            for (path, metrics) in &self.partition_metrics {
                if path.is_empty() {
                    continue;
                }
                let counts = [
                    keys::ADDED_DATA_FILES,
                    keys::DELETED_DATA_FILES,
                    keys::REMOVED_EQUALITY_DELETE_FILES,
                    keys::REMOVED_POSITION_DELETE_FILES,
                    keys::REMOVED_DELETE_FILES,
                    keys::REMOVED_DVS,
                    keys::ADDED_RECORDS,
                    keys::DELETED_RECORDS,
                    "added-files-size",
                    keys::REMOVED_FILES_SIZE,
                    keys::REMOVED_POSITION_DELETES,
                    keys::REMOVED_EQUALITY_DELETES,
                ]
                .into_iter()
                .filter_map(|key| {
                    metrics
                        .get(key)
                        .filter(|n| **n > 0)
                        .map(|value| format!("{key}={value}"))
                })
                .collect::<Vec<_>>()
                .join(",");
                summary = summary.with_property(format!("partitions.{path}"), counts);
            }
        }
        for (&key, &value) in &self.metrics {
            if value > 0 {
                summary = summary.with_property(key, value);
            }
        }
        for (total, added, removed) in [
            (
                keys::TOTAL_DATA_FILES,
                keys::ADDED_DATA_FILES,
                keys::DELETED_DATA_FILES,
            ),
            (
                keys::TOTAL_RECORDS,
                keys::ADDED_RECORDS,
                keys::DELETED_RECORDS,
            ),
            (
                keys::TOTAL_FILES_SIZE,
                "added-files-size",
                keys::REMOVED_FILES_SIZE,
            ),
            (
                keys::TOTAL_DELETE_FILES,
                keys::ADDED_DELETE_FILES,
                keys::REMOVED_DELETE_FILES,
            ),
            (
                keys::TOTAL_POSITION_DELETES,
                keys::ADDED_POSITION_DELETES,
                keys::REMOVED_POSITION_DELETES,
            ),
            (
                keys::TOTAL_EQUALITY_DELETES,
                keys::ADDED_EQUALITY_DELETES,
                keys::REMOVED_EQUALITY_DELETES,
            ),
        ] {
            let previous = match parent {
                None => Some(0_u64),
                Some(s) => s
                    .summary
                    .additional_properties
                    .get(total)
                    .and_then(|v| v.parse().ok()),
            };
            if let Some(value) = previous
                .and_then(|n| n.checked_add(self.metrics.get(added).copied().unwrap_or(0)))
                .and_then(|n| n.checked_sub(self.metrics.get(removed).copied().unwrap_or(0)))
            {
                summary = summary.with_property(total, value);
            }
        }
        summary
    }
}
