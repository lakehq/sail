// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};

use super::{ActionCommit, Transaction};
use crate::io::StoreContext;
use crate::spec::manifest::{ManifestEntry, ManifestWriter, ManifestWriterBuilder};
use crate::spec::manifest_list::ManifestListWriter;
use crate::spec::{
    DataFile, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile, ManifestStatus,
    Operation, PartitionSpec, Schema, SnapshotBuilder, SnapshotReference, SnapshotRetention,
    TableRequirement, TableUpdate,
};
use crate::utils::join_table_uri;

fn active_file_count(manifest: &crate::spec::manifest_list::ManifestFile) -> Option<i64> {
    Some(i64::from(manifest.added_files_count?) + i64::from(manifest.existing_files_count?))
}

fn active_row_count(manifest: &crate::spec::manifest_list::ManifestFile) -> Option<i64> {
    Some(manifest.added_rows_count? + manifest.existing_rows_count?)
}

#[derive(Debug, Clone, Copy)]
enum DeleteRowTotalsBase<'a> {
    Empty,
    Parent(&'a crate::spec::snapshots::Summary),
}

impl DeleteRowTotalsBase<'_> {
    fn previous_total(self, key: &str) -> Option<u64> {
        match self {
            Self::Empty => Some(0),
            Self::Parent(parent_summary) => {
                parent_summary.additional_properties.get(key)?.parse().ok()
            }
        }
    }
}

fn with_manifest_totals<'a>(
    mut summary: crate::spec::snapshots::Summary,
    delete_totals_base: DeleteRowTotalsBase<'_>,
    manifests: impl IntoIterator<Item = &'a crate::spec::manifest_list::ManifestFile>,
    added_position_deletes: u64,
    added_equality_deletes: u64,
) -> crate::spec::snapshots::Summary {
    let mut total_data_files = Some(0);
    let mut total_delete_files = Some(0);
    let mut total_records = Some(0);
    for manifest in manifests {
        match manifest.content {
            ManifestContentType::Data => {
                total_data_files = total_data_files
                    .zip(active_file_count(manifest))
                    .map(|(total, active)| total + active);
                total_records = total_records
                    .zip(active_row_count(manifest))
                    .map(|(total, active)| total + active);
            }
            ManifestContentType::Deletes => {
                total_delete_files = total_delete_files
                    .zip(active_file_count(manifest))
                    .map(|(total, active)| total + active);
            }
        }
    }

    if let Some(total_data_files) = total_data_files {
        summary = summary.with_property("total-data-files", total_data_files.to_string());
    }
    if let Some(total_delete_files) = total_delete_files {
        summary = summary.with_property("total-delete-files", total_delete_files.to_string());
    }
    if let Some(total_records) = total_records {
        summary = summary.with_property("total-records", total_records.to_string());
    }

    if let Some(previous_position_deletes) =
        delete_totals_base.previous_total("total-position-deletes")
    {
        summary = summary.with_property(
            "total-position-deletes",
            (previous_position_deletes + added_position_deletes).to_string(),
        );
    }
    if let Some(previous_equality_deletes) =
        delete_totals_base.previous_total("total-equality-deletes")
    {
        summary = summary.with_property(
            "total-equality-deletes",
            (previous_equality_deletes + added_equality_deletes).to_string(),
        );
    }
    summary
}

fn manifest_inputs(
    manifest_metadata: &crate::spec::manifest::ManifestMetadata,
    partition_specs: &[PartitionSpec],
    files: &[DataFile],
    content: ManifestContentType,
) -> Result<Vec<(crate::spec::manifest::ManifestMetadata, Vec<DataFile>)>, String> {
    let mut files_by_spec = BTreeMap::<i32, Vec<DataFile>>::new();
    for file in files {
        files_by_spec
            .entry(file.partition_spec_id)
            .or_default()
            .push(file.clone());
    }

    let manifest_kind = match content {
        ManifestContentType::Data => "data",
        ManifestContentType::Deletes => "delete",
    };

    files_by_spec
        .into_iter()
        .map(|(spec_id, files)| {
            let partition_spec = partition_specs
                .iter()
                .chain(std::iter::once(&manifest_metadata.partition_spec))
                .find(|spec| spec.spec_id() == spec_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "cannot write Iceberg {manifest_kind} manifest: partition spec {spec_id} is missing from table metadata"
                    )
                })?;
            let mut metadata = manifest_metadata.clone();
            metadata.partition_spec = partition_spec;
            metadata.content = content.clone();
            Ok((metadata, files))
        })
        .collect()
}

fn data_manifest_inputs(
    manifest_metadata: &crate::spec::manifest::ManifestMetadata,
    partition_specs: &[PartitionSpec],
    data_files: &[DataFile],
) -> Result<Vec<(crate::spec::manifest::ManifestMetadata, Vec<DataFile>)>, String> {
    manifest_inputs(
        manifest_metadata,
        partition_specs,
        data_files,
        ManifestContentType::Data,
    )
}

fn delete_manifest_inputs(
    manifest_metadata: &crate::spec::manifest::ManifestMetadata,
    partition_specs: &[PartitionSpec],
    delete_files: &[DataFile],
) -> Result<Vec<(crate::spec::manifest::ManifestMetadata, Vec<DataFile>)>, String> {
    manifest_inputs(
        manifest_metadata,
        partition_specs,
        delete_files,
        ManifestContentType::Deletes,
    )
}

fn validate_delete_files_for_format(
    format_version: FormatVersion,
    delete_files: &[DataFile],
) -> Result<(), String> {
    if delete_files
        .iter()
        .any(|file| file.content == crate::spec::DataContentType::Data)
    {
        return Err("Iceberg delete manifest input contains a data file".to_string());
    }
    if format_version == FormatVersion::V1 && !delete_files.is_empty() {
        return Err("Iceberg v1 snapshots cannot add delete files".to_string());
    }
    if format_version == FormatVersion::V3
        && delete_files
            .iter()
            .any(|file| file.content == crate::spec::DataContentType::PositionDeletes)
    {
        return Err(
            "Iceberg v3 snapshots cannot add position delete files; v3 requires deletion vectors"
                .to_string(),
        );
    }
    Ok(())
}

fn manifest_counts_are_complete(manifest_file: &crate::spec::manifest_list::ManifestFile) -> bool {
    manifest_file.added_files_count.is_some()
        && manifest_file.existing_files_count.is_some()
        && manifest_file.deleted_files_count.is_some()
        && manifest_file.added_rows_count.is_some()
        && manifest_file.existing_rows_count.is_some()
        && manifest_file.deleted_rows_count.is_some()
}

async fn populate_retained_manifest_counts(
    store_ctx: &StoreContext,
    manifest_file: &mut crate::spec::manifest_list::ManifestFile,
) -> Result<(), String> {
    if manifest_counts_are_complete(manifest_file) {
        return Ok(());
    }

    let manifest = crate::io::load_manifest(store_ctx, manifest_file.manifest_path.as_str())
        .await
        .map_err(|error| {
            format!(
                "failed to load retained manifest {} to recover missing counts: {error}",
                manifest_file.manifest_path
            )
        })?;
    let mut file_counts = [0i32; 3];
    let mut row_counts = [0i64; 3];
    for entry in manifest.entries() {
        let index = match entry.status {
            ManifestStatus::Added => 0,
            ManifestStatus::Existing => 1,
            ManifestStatus::Deleted => 2,
        };
        file_counts[index] = file_counts[index]
            .checked_add(1)
            .ok_or_else(|| "Iceberg manifest file count overflow".to_string())?;
        let record_count = i64::try_from(entry.data_file.record_count)
            .map_err(|_| "Iceberg manifest row count overflow".to_string())?;
        row_counts[index] = row_counts[index]
            .checked_add(record_count)
            .ok_or_else(|| "Iceberg manifest row count overflow".to_string())?;
    }

    manifest_file
        .added_files_count
        .get_or_insert(file_counts[0]);
    manifest_file
        .existing_files_count
        .get_or_insert(file_counts[1]);
    manifest_file
        .deleted_files_count
        .get_or_insert(file_counts[2]);
    manifest_file.added_rows_count.get_or_insert(row_counts[0]);
    manifest_file
        .existing_rows_count
        .get_or_insert(row_counts[1]);
    manifest_file
        .deleted_rows_count
        .get_or_insert(row_counts[2]);
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotUpdateKind {
    FastAppend,
    FullOverwrite,
    RowDelta,
    CopyOnWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotChanges {
    added_data_files: usize,
    added_delete_files: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RewriteStats {
    parent_live_files: i64,
    parent_live_rows: i64,
    deleted_files: i64,
    deleted_rows: i64,
}

impl SnapshotChanges {
    fn from_added_files(added_data_files: &[DataFile], added_delete_files: &[DataFile]) -> Self {
        Self {
            added_data_files: added_data_files.len(),
            added_delete_files: added_delete_files.len(),
        }
    }

    fn adds_data_files(self) -> bool {
        self.added_data_files > 0
    }

    fn adds_delete_files(self) -> bool {
        self.added_delete_files > 0
    }
}

impl SnapshotUpdateKind {
    fn summary_operation(self, changes: SnapshotChanges) -> Operation {
        match self {
            Self::FastAppend => Operation::Append,
            Self::FullOverwrite => Operation::Overwrite,
            Self::RowDelta if changes.adds_data_files() && !changes.adds_delete_files() => {
                Operation::Append
            }
            Self::RowDelta if changes.adds_delete_files() && !changes.adds_data_files() => {
                Operation::Delete
            }
            Self::RowDelta => Operation::Overwrite,
            Self::CopyOnWrite => Operation::Overwrite,
        }
    }

    fn carries_parent_manifests(self) -> bool {
        !matches!(self, Self::FullOverwrite)
    }

    fn is_targeted_rewrite(self) -> bool {
        matches!(self, Self::CopyOnWrite)
    }

    fn delete_totals_base<'a>(
        self,
        parent_summary: &'a crate::spec::snapshots::Summary,
        is_bootstrap: bool,
    ) -> DeleteRowTotalsBase<'a> {
        if is_bootstrap || !self.carries_parent_manifests() {
            DeleteRowTotalsBase::Empty
        } else {
            DeleteRowTotalsBase::Parent(parent_summary)
        }
    }
}

pub struct SnapshotProducer<'a> {
    pub tx: &'a Transaction,
    pub added_data_files: Vec<DataFile>,
    pub added_delete_files: Vec<DataFile>,
    pub removed_data_file_paths: Vec<String>,
    pub store_ctx: Option<StoreContext>,
    pub manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
    pub partition_specs: Vec<PartitionSpec>,
    pub write_path_mode: crate::utils::WritePathMode,
    /// If true, create a snapshot with no parent (for bootstrap scenarios)
    pub is_bootstrap: bool,
    pub row_lineage_start_row_id: Option<i64>,
}

pub(crate) struct PreparedSnapshotCommit {
    action_commit: ActionCommit,
    store_ctx: StoreContext,
    created_paths: Vec<ObjectPath>,
}

impl PreparedSnapshotCommit {
    pub(crate) fn action_commit(&self) -> &ActionCommit {
        &self.action_commit
    }

    pub(crate) fn into_action_commit(self) -> ActionCommit {
        self.action_commit
    }

    pub(crate) async fn cleanup(self) {
        cleanup_created_paths(&self.store_ctx, &self.created_paths).await;
    }
}

async fn cleanup_created_paths(store_ctx: &StoreContext, paths: &[ObjectPath]) {
    let paths = paths.iter().rev().cloned().collect::<Vec<_>>();
    let locations = futures::stream::iter(paths.into_iter().map(Ok));
    let mut deletions = store_ctx.prefixed.delete_stream(Box::pin(locations));
    while let Some(result) = deletions.next().await {
        match result {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                log::warn!("Failed to remove an uncommitted Iceberg object: {error}");
            }
        }
    }
}

impl<'a> SnapshotProducer<'a> {
    pub fn new(
        tx: &'a Transaction,
        added_data_files: Vec<DataFile>,
        store_ctx: Option<StoreContext>,
        manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
    ) -> Self {
        Self {
            tx,
            added_data_files,
            added_delete_files: Vec::new(),
            removed_data_file_paths: Vec::new(),
            store_ctx,
            manifest_metadata,
            partition_specs: Vec::new(),
            write_path_mode: crate::utils::WritePathMode::Absolute,
            is_bootstrap: false,
            row_lineage_start_row_id: None,
        }
    }

    pub fn with_write_path_mode(mut self, mode: crate::utils::WritePathMode) -> Self {
        self.write_path_mode = mode;
        self
    }

    /// Enable bootstrap mode: create a snapshot with no parent.
    /// This is used when creating the first snapshot for a table.
    pub fn with_bootstrap(mut self, is_bootstrap: bool) -> Self {
        self.is_bootstrap = is_bootstrap;
        self
    }

    pub fn with_row_lineage_start_row_id(mut self, start_row_id: Option<i64>) -> Self {
        self.row_lineage_start_row_id = start_row_id;
        self
    }

    pub fn with_added_delete_files(mut self, delete_files: Vec<DataFile>) -> Self {
        self.added_delete_files = delete_files;
        self
    }

    pub fn with_removed_data_file_paths(mut self, removed_data_file_paths: Vec<String>) -> Self {
        self.removed_data_file_paths = removed_data_file_paths;
        self
    }

    pub fn with_partition_specs(mut self, partition_specs: Vec<PartitionSpec>) -> Self {
        self.partition_specs = partition_specs;
        self
    }

    async fn write_rewritten_manifest(
        &self,
        store_ctx: &StoreContext,
        writer: ManifestWriter,
        sequence_number: i64,
        min_sequence_number: i64,
        snapshot_id: i64,
        first_row_id: Option<i64>,
        created_paths: &mut Vec<ObjectPath>,
    ) -> Result<ManifestFile, String> {
        let manifest_bytes = writer.to_avro_bytes_v2()?;
        let manifest_len = i64::try_from(manifest_bytes.len())
            .map_err(|_| "Iceberg manifest length exceeds i64".to_string())?;
        let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
        let manifest_path = ObjectPath::from(manifest_rel.as_str());
        let mut manifest_file = writer.into_manifest_file(
            join_table_uri(self.tx.table_uri(), &manifest_rel, &self.write_path_mode),
            sequence_number,
            snapshot_id,
        );
        manifest_file.manifest_length = manifest_len;
        manifest_file.min_sequence_number = min_sequence_number;
        manifest_file.first_row_id = first_row_id;
        store_ctx
            .prefixed
            .put(
                &manifest_path,
                object_store::PutPayload::from(Bytes::from(manifest_bytes)),
            )
            .await
            .map_err(|error| error.to_string())?;
        created_paths.push(manifest_path);
        Ok(manifest_file)
    }

    fn materialize_inherited_entry(
        mut entry: ManifestEntry,
        manifest_file: &ManifestFile,
        inherited_next_row_id: &mut Option<i64>,
    ) -> Result<ManifestEntry, String> {
        entry.snapshot_id = entry.snapshot_id.or(Some(manifest_file.added_snapshot_id));
        if matches!(entry.status, ManifestStatus::Added) || manifest_file.sequence_number == 0 {
            entry.sequence_number = entry
                .sequence_number
                .or(Some(manifest_file.sequence_number));
            entry.file_sequence_number = entry
                .file_sequence_number
                .or(Some(manifest_file.sequence_number));
        } else if entry.sequence_number.is_none() || entry.file_sequence_number.is_none() {
            return Err(
                "existing and deleted manifest entries require explicit data and file sequence numbers"
                    .to_string(),
            );
        }

        if entry.data_file.first_row_id.is_none() {
            entry.data_file.first_row_id = *inherited_next_row_id;
            if let Some(next_row_id) = inherited_next_row_id {
                let record_count = i64::try_from(entry.data_file.record_count)
                    .map_err(|_| "Iceberg data file record count exceeds i64".to_string())?;
                *next_row_id = next_row_id
                    .checked_add(record_count)
                    .ok_or_else(|| "Iceberg row lineage id overflow".to_string())?;
            }
        }
        Ok(entry)
    }

    async fn rewrite_parent_manifests(
        &self,
        store_ctx: &StoreContext,
        parent_manifests: Vec<ManifestFile>,
        removed_data_file_paths: &HashSet<String>,
        sequence_number: i64,
        snapshot_id: i64,
        created_paths: &mut Vec<ObjectPath>,
    ) -> Result<(Vec<ManifestFile>, RewriteStats), String> {
        enum PlannedManifest {
            Reuse(ManifestFile),
            Rewrite {
                writer: ManifestWriter,
                min_sequence_number: i64,
                first_row_id: Option<i64>,
            },
        }

        let mut planned_manifests = Vec::with_capacity(parent_manifests.len());
        let mut found_paths = HashSet::new();
        let mut stats = RewriteStats::default();

        for parent_manifest_file in parent_manifests {
            if parent_manifest_file.content != ManifestContentType::Data {
                planned_manifests.push(PlannedManifest::Reuse(parent_manifest_file));
                continue;
            }
            let manifest = crate::io::load_manifest(store_ctx, &parent_manifest_file.manifest_path)
                .await
                .map_err(|error| {
                    format!(
                        "failed to load Iceberg parent manifest {}: {error}",
                        parent_manifest_file.manifest_path
                    )
                })?;
            let contains_removed_path = manifest.entries().iter().any(|entry| {
                matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                ) && removed_data_file_paths.contains(&entry.data_file.file_path)
            });
            for entry in manifest.entries().iter().filter(|entry| {
                matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                )
            }) {
                stats.parent_live_files = stats
                    .parent_live_files
                    .checked_add(1)
                    .ok_or_else(|| "Iceberg parent data file count overflow".to_string())?;
                stats.parent_live_rows =
                    stats
                        .parent_live_rows
                        .checked_add(i64::try_from(entry.data_file.record_count).map_err(|_| {
                            "Iceberg data file record count exceeds i64".to_string()
                        })?)
                        .ok_or_else(|| "Iceberg parent record count overflow".to_string())?;
                if removed_data_file_paths.contains(&entry.data_file.file_path) {
                    found_paths.insert(entry.data_file.file_path.clone());
                }
            }
            if !contains_removed_path {
                planned_manifests.push(PlannedManifest::Reuse(parent_manifest_file));
                continue;
            }

            let (entries, metadata) = manifest.into_parts();
            let mut writer = ManifestWriterBuilder::new(Some(snapshot_id), None, metadata).build();
            let mut inherited_next_row_id = parent_manifest_file.first_row_id;
            let mut min_sequence_number = sequence_number;
            for entry in entries {
                if !matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                ) {
                    continue;
                }
                let entry = Self::materialize_inherited_entry(
                    entry.as_ref().clone(),
                    &parent_manifest_file,
                    &mut inherited_next_row_id,
                )?;
                min_sequence_number = min_sequence_number.min(entry.sequence_number.unwrap_or(0));
                if removed_data_file_paths.contains(&entry.data_file.file_path) {
                    stats.deleted_files = stats
                        .deleted_files
                        .checked_add(1)
                        .ok_or_else(|| "Iceberg deleted data file count overflow".to_string())?;
                    stats.deleted_rows = stats
                        .deleted_rows
                        .checked_add(i64::try_from(entry.data_file.record_count).map_err(|_| {
                            "Iceberg data file record count exceeds i64".to_string()
                        })?)
                        .ok_or_else(|| "Iceberg deleted record count overflow".to_string())?;
                    writer.add_deleted_entry(entry)?;
                } else {
                    writer.add_existing_entry(entry)?;
                }
            }
            planned_manifests.push(PlannedManifest::Rewrite {
                writer,
                min_sequence_number,
                first_row_id: parent_manifest_file.first_row_id,
            });
        }

        let mut missing_paths = removed_data_file_paths
            .difference(&found_paths)
            .cloned()
            .collect::<Vec<_>>();
        if !missing_paths.is_empty() {
            missing_paths.sort();
            return Err(format!(
                "Iceberg data files are not live in the expected parent snapshot: {}",
                missing_paths.join(", ")
            ));
        }

        let mut output_manifests = Vec::with_capacity(planned_manifests.len());
        for manifest in planned_manifests {
            match manifest {
                PlannedManifest::Reuse(manifest_file) => output_manifests.push(manifest_file),
                PlannedManifest::Rewrite {
                    writer,
                    min_sequence_number,
                    first_row_id,
                } => {
                    output_manifests.push(
                        self.write_rewritten_manifest(
                            store_ctx,
                            writer,
                            sequence_number,
                            min_sequence_number,
                            snapshot_id,
                            first_row_id,
                            created_paths,
                        )
                        .await?,
                    );
                }
            }
        }
        Ok((output_manifests, stats))
    }

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        // TODO: Implement this function to validate the added data files
        Ok(())
    }

    pub async fn commit(self, update_kind: SnapshotUpdateKind) -> Result<ActionCommit, String> {
        Ok(self.prepare(update_kind).await?.into_action_commit())
    }

    pub(crate) async fn prepare(
        self,
        update_kind: SnapshotUpdateKind,
    ) -> Result<PreparedSnapshotCommit, String> {
        let store_ctx = self
            .store_ctx
            .clone()
            .ok_or_else(|| "store context not available".to_string())?;
        let mut created_paths = Vec::new();
        match self
            .build_action_commit(update_kind, &mut created_paths)
            .await
        {
            Ok(action_commit) => Ok(PreparedSnapshotCommit {
                action_commit,
                store_ctx,
                created_paths,
            }),
            Err(error) => {
                cleanup_created_paths(&store_ctx, &created_paths).await;
                Err(error)
            }
        }
    }

    async fn build_action_commit(
        self,
        update_kind: SnapshotUpdateKind,
        created_paths: &mut Vec<ObjectPath>,
    ) -> Result<ActionCommit, String> {
        let removed_data_file_paths = self
            .removed_data_file_paths
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        if removed_data_file_paths.iter().any(String::is_empty) {
            return Err("Iceberg removed data file path cannot be empty".to_string());
        }
        if update_kind.is_targeted_rewrite() {
            if removed_data_file_paths.is_empty() && self.added_data_files.is_empty() {
                return Err(
                    "Iceberg copy-on-write commit requires added or removed data files".to_string(),
                );
            }
            if !self.added_delete_files.is_empty() {
                return Err(
                    "Iceberg copy-on-write commit cannot add row-level delete files".to_string(),
                );
            }
        } else if !removed_data_file_paths.is_empty() {
            return Err(
                "Iceberg removed data files require a copy-on-write snapshot update".to_string(),
            );
        }
        let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
        let changes =
            SnapshotChanges::from_added_files(&self.added_data_files, &self.added_delete_files);
        let operation = update_kind.summary_operation(changes);
        let added_data_file_count = self.added_data_files.len();
        let added_records = self
            .added_data_files
            .iter()
            .map(|df| df.record_count)
            .sum::<u64>();
        let mut added_position_delete_files = 0usize;
        let mut added_position_deletes = 0u64;
        let mut added_equality_delete_files = 0usize;
        let mut added_equality_deletes = 0u64;
        for df in &self.added_delete_files {
            match df.content {
                crate::spec::DataContentType::PositionDeletes => {
                    added_position_delete_files += 1;
                    added_position_deletes += df.record_count;
                }
                crate::spec::DataContentType::EqualityDeletes => {
                    added_equality_delete_files += 1;
                    added_equality_deletes += df.record_count;
                }
                crate::spec::DataContentType::Data => {}
            }
        }
        let mut summary = crate::spec::snapshots::Summary::new(operation.clone());
        if added_data_file_count > 0 {
            summary = summary
                .with_property("added-data-files", added_data_file_count.to_string())
                .with_property("added-records", added_records.to_string());
        }
        if !self.added_delete_files.is_empty() {
            summary = summary.with_property(
                "added-delete-files",
                self.added_delete_files.len().to_string(),
            );
            if added_position_delete_files > 0 {
                summary = summary
                    .with_property(
                        "added-position-delete-files",
                        added_position_delete_files.to_string(),
                    )
                    .with_property("added-position-deletes", added_position_deletes.to_string());
            }
            if added_equality_delete_files > 0 {
                summary = summary
                    .with_property(
                        "added-equality-delete-files",
                        added_equality_delete_files.to_string(),
                    )
                    .with_property("added-equality-deletes", added_equality_deletes.to_string());
            }
        }

        // Build manifest metadata: prefer caller-provided metadata derived from table schema/spec
        // Fall back to deriving from the current transaction snapshot if not provided
        let metadata = match self.manifest_metadata.clone() {
            Some(meta) => meta,
            _ => {
                let schema_id = self.tx.snapshot().schema_id().unwrap_or_default();
                let schema = Schema::builder()
                    .with_schema_id(schema_id)
                    .with_fields(vec![])
                    .build()
                    .map_err(|e| format!("schema build error: {e}"))?;
                let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
                crate::spec::manifest::ManifestMetadata::new(
                    std::sync::Arc::new(schema.clone()),
                    schema_id,
                    partition_spec,
                    FormatVersion::V2,
                    ManifestContentType::Data,
                )
            }
        };
        let format_version = metadata.format_version;
        validate_delete_files_for_format(format_version, &self.added_delete_files)?;
        let data_manifest_inputs =
            data_manifest_inputs(&metadata, &self.partition_specs, &self.added_data_files)?;
        let delete_manifest_inputs =
            delete_manifest_inputs(&metadata, &self.partition_specs, &self.added_delete_files)?;

        let store_ctx = self
            .store_ctx
            .as_ref()
            .ok_or_else(|| "store context not available".to_string())?;

        // Generate new snapshot ID using UUID (not timestamp) and sequence number
        let new_snapshot_id = crate::utils::snapshot_id::generate_snapshot_id();
        let new_sequence_number = self.tx.next_sequence_number()?;

        let parent_snapshot = self.tx.snapshot();
        let parent_manifest_list_path_str = parent_snapshot.manifest_list();
        let mut parent_manifest_entries = Vec::new();

        if !self.is_bootstrap
            && update_kind.carries_parent_manifests()
            && !parent_manifest_list_path_str.is_empty()
        {
            let (store_ref, manifest_list_path) = store_ctx
                .resolve(parent_manifest_list_path_str)
                .map_err(|e| format!("{}", e))?;

            log::trace!(
                "snapshot producer: loading parent manifest list: {}",
                manifest_list_path
            );
            let manifest_list_data = store_ref
                .get(&manifest_list_path)
                .await
                .map_err(|e| format!("Failed to get parent manifest list: {}", e))?
                .bytes()
                .await
                .map_err(|e| format!("Failed to read parent manifest list bytes: {}", e))?;
            let parent_manifest_list =
                crate::spec::ManifestList::parse_with_version(&manifest_list_data, format_version)?;
            log::trace!(
                "snapshot producer: found parent manifest files: {}",
                parent_manifest_list.entries().len()
            );
            for mut manifest_file in parent_manifest_list.into_entries() {
                populate_retained_manifest_counts(store_ctx, &mut manifest_file).await?;
                parent_manifest_entries.push(manifest_file);
            }
        }

        let rewrite_stats = if update_kind.is_targeted_rewrite() {
            let (rewritten_manifests, stats) = self
                .rewrite_parent_manifests(
                    store_ctx,
                    parent_manifest_entries,
                    &removed_data_file_paths,
                    new_sequence_number,
                    new_snapshot_id,
                    created_paths,
                )
                .await?;
            parent_manifest_entries = rewritten_manifests;
            Some(stats)
        } else {
            None
        };

        if let Some(stats) = rewrite_stats {
            let added_files = i64::try_from(added_data_file_count)
                .map_err(|_| "Iceberg added data file count exceeds i64".to_string())?;
            let added_rows = i64::try_from(added_records)
                .map_err(|_| "Iceberg added record count exceeds i64".to_string())?;
            let total_data_files = stats
                .parent_live_files
                .checked_sub(stats.deleted_files)
                .and_then(|count| count.checked_add(added_files))
                .ok_or_else(|| "Iceberg total data file count overflow".to_string())?;
            let total_records = stats
                .parent_live_rows
                .checked_sub(stats.deleted_rows)
                .and_then(|count| count.checked_add(added_rows))
                .ok_or_else(|| "Iceberg total record count overflow".to_string())?;
            summary = summary
                .with_property("added-data-files", added_files.to_string())
                .with_property("deleted-data-files", stats.deleted_files.to_string())
                .with_property("added-records", added_rows.to_string())
                .with_property("deleted-records", stats.deleted_rows.to_string())
                .with_property("total-data-files", total_data_files.to_string())
                .with_property("total-records", total_records.to_string());
        }

        let new_added_rows: i64 = self
            .added_data_files
            .iter()
            .map(|df| df.record_count as i64)
            .sum();
        let mut row_lineage_next_row_id = self.row_lineage_start_row_id;
        let mut snapshot_added_rows = 0;

        if let Some(next_row_id) = &mut row_lineage_next_row_id {
            for entry in &mut parent_manifest_entries {
                if matches!(entry.content, ManifestContentType::Data)
                    && entry.first_row_id.is_none()
                {
                    entry.first_row_id = Some(*next_row_id);
                    let assigned_rows = entry.added_rows_count.unwrap_or(0)
                        + entry.existing_rows_count.unwrap_or(0);
                    *next_row_id += assigned_rows;
                    snapshot_added_rows += assigned_rows;
                }
            }
        }

        if self.row_lineage_start_row_id.is_some() {
            snapshot_added_rows += new_added_rows;
        }

        let mut new_manifest_files = Vec::new();
        for (data_metadata, added_data_files) in data_manifest_inputs {
            let manifest_first_row_id = row_lineage_next_row_id;
            let manifest_added_rows = added_data_files
                .iter()
                .map(|file| file.record_count as i64)
                .sum::<i64>();
            if let Some(next_row_id) = &mut row_lineage_next_row_id {
                *next_row_id += manifest_added_rows;
            }
            let mut writer = ManifestWriterBuilder::new(None, None, data_metadata.clone()).build();
            for df in &added_data_files {
                writer.add(df.clone());
            }
            let manifest = writer.finish();
            let manifest_bytes = manifest.to_avro_bytes_v2()?;

            let manifest_len = manifest_bytes.len() as i64;
            let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
            let manifest_path = object_store::path::Path::from(manifest_rel.as_str());
            store_ctx
                .prefixed
                .put(
                    &manifest_path,
                    object_store::PutPayload::from(Bytes::from(manifest_bytes)),
                )
                .await
                .map_err(|e| format!("{}", e))?;
            created_paths.push(manifest_path);

            let mut manifest_file_builder = crate::spec::manifest_list::ManifestFile::builder()
                .with_manifest_path(join_table_uri(
                    self.tx.table_uri(),
                    &manifest_rel,
                    &self.write_path_mode,
                ))
                .with_manifest_length(manifest_len)
                .with_partition_spec_id(data_metadata.partition_spec.spec_id())
                .with_content(ManifestContentType::Data)
                .with_sequence_number(new_sequence_number)
                .with_min_sequence_number(new_sequence_number)
                .with_added_snapshot_id(new_snapshot_id)
                .with_file_counts(added_data_files.len() as i32, 0, 0)
                .with_row_counts(manifest_added_rows, 0, 0);
            if let Some(first_row_id) = manifest_first_row_id {
                manifest_file_builder = manifest_file_builder.with_first_row_id(first_row_id);
            }
            new_manifest_files.push(manifest_file_builder.build()?);
        }

        for (delete_metadata, added_delete_files) in delete_manifest_inputs {
            let mut writer =
                ManifestWriterBuilder::new(None, None, delete_metadata.clone()).build();
            for df in &added_delete_files {
                writer.add(df.clone());
            }
            let manifest = writer.finish();
            let manifest_bytes = manifest.to_avro_bytes_v2()?;
            let manifest_len = manifest_bytes.len() as i64;
            let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
            let manifest_path = object_store::path::Path::from(manifest_rel.as_str());
            store_ctx
                .prefixed
                .put(
                    &manifest_path,
                    object_store::PutPayload::from(Bytes::from(manifest_bytes)),
                )
                .await
                .map_err(|e| format!("{}", e))?;
            created_paths.push(manifest_path);
            let added_delete_rows = added_delete_files
                .iter()
                .map(|df| df.record_count as i64)
                .sum::<i64>();
            new_manifest_files.push(
                crate::spec::manifest_list::ManifestFile::builder()
                    .with_manifest_path(join_table_uri(
                        self.tx.table_uri(),
                        &manifest_rel,
                        &self.write_path_mode,
                    ))
                    .with_manifest_length(manifest_len)
                    .with_partition_spec_id(delete_metadata.partition_spec.spec_id())
                    .with_content(ManifestContentType::Deletes)
                    .with_sequence_number(new_sequence_number)
                    .with_min_sequence_number(new_sequence_number)
                    .with_added_snapshot_id(new_snapshot_id)
                    .with_file_counts(added_delete_files.len() as i32, 0, 0)
                    .with_row_counts(added_delete_rows, 0, 0)
                    .build()?,
            );
        }

        summary = with_manifest_totals(
            summary,
            update_kind.delete_totals_base(parent_snapshot.summary(), self.is_bootstrap),
            parent_manifest_entries
                .iter()
                .chain(new_manifest_files.iter()),
            added_position_deletes,
            added_equality_deletes,
        );

        let mut list_writer = ManifestListWriter::new();
        let mut total_manifest_count = 0;

        for entry in parent_manifest_entries {
            list_writer.append(entry);
            total_manifest_count += 1;
        }

        log::trace!(
            "Creating new snapshot: id={} seq={} parent_id={}",
            new_snapshot_id,
            new_sequence_number,
            self.tx.snapshot().snapshot_id()
        );

        for manifest_file in new_manifest_files {
            list_writer.append(manifest_file);
            total_manifest_count += 1;
        }
        log::trace!(
            "snapshot producer: new manifest list will have files: {}",
            total_manifest_count
        );
        let list_bytes = list_writer.to_bytes(format_version)?;
        let list_rel = format!("metadata/snap-{}.avro", new_snapshot_id);
        let list_path = object_store::path::Path::from(list_rel.as_str());
        store_ctx
            .prefixed
            .put(
                &list_path,
                object_store::PutPayload::from(Bytes::from(list_bytes)),
            )
            .await
            .map_err(|e| format!("{}", e))?;
        created_paths.push(list_path);

        let manifest_list_uri =
            join_table_uri(self.tx.table_uri(), &list_rel, &self.write_path_mode);

        let schema_id = if let Some(meta) = &self.manifest_metadata {
            meta.schema_id
        } else {
            self.tx.snapshot().schema_id().unwrap_or_default()
        };

        let mut snapshot_builder = SnapshotBuilder::new()
            .with_snapshot_id(new_snapshot_id)
            .with_sequence_number(new_sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(manifest_list_uri)
            .with_summary(summary)
            .with_schema_id(schema_id);

        // Only set parent snapshot ID if not in bootstrap mode
        if !self.is_bootstrap {
            snapshot_builder =
                snapshot_builder.with_parent_snapshot_id(self.tx.snapshot().snapshot_id());
        }

        if let Some(start_row_id) = self.row_lineage_start_row_id {
            snapshot_builder = snapshot_builder
                .with_first_row_id(start_row_id)
                .with_added_rows(snapshot_added_rows);
        }

        let new_snapshot = snapshot_builder.build()?;

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot.clone(),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference {
                    snapshot_id: new_snapshot.snapshot_id(),
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ];

        // For bootstrap mode, expect no existing snapshot (None)
        // For normal mode, expect the current snapshot ID
        let expected_snapshot_id = if self.is_bootstrap {
            None
        } else {
            Some(self.tx.snapshot().snapshot_id())
        };

        let requirements = vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: expected_snapshot_id,
        }];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };

    use super::*;
    use crate::spec::manifest::ManifestMetadata;
    use crate::spec::manifest_list::ManifestFile;
    use crate::spec::types::values::{Literal, PrimitiveLiteral};
    use crate::spec::types::{NestedField, PrimitiveType, Type};
    use crate::spec::{DataContentType, DataFileFormat, Transform};

    #[derive(Debug)]
    struct ManifestListRejectingStore {
        memory_store: Arc<object_store::memory::InMemory>,
    }

    impl std::fmt::Display for ManifestListRejectingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ManifestListRejectingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ManifestListRejectingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if location.as_ref().contains("/metadata/snap-") {
                return Err(object_store::Error::Generic {
                    store: "fail-manifest-list",
                    source: Box::new(std::io::Error::other("injected manifest-list failure")),
                });
            }
            self.memory_store.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.memory_store.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.memory_store.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.memory_store.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.memory_store.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.memory_store.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.memory_store.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.memory_store.copy_opts(from, to, options).await
        }
    }

    fn nullable_manifest_list_bytes(manifest_path: &str, manifest_length: i64) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct NullableManifestCounts<'a> {
            manifest_path: &'a str,
            manifest_length: i64,
            partition_spec_id: i32,
            added_snapshot_id: i64,
            added_data_files_count: Option<i32>,
            existing_data_files_count: Option<i32>,
            deleted_data_files_count: Option<i32>,
            added_rows_count: Option<i64>,
            existing_rows_count: Option<i64>,
            deleted_rows_count: Option<i64>,
        }

        let schema = apache_avro::Schema::parse_str(
            r#"{
              "type": "record",
              "name": "manifest_file",
              "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "partition_spec_id", "type": "int"},
                {"name": "added_snapshot_id", "type": "long"},
                {"name": "added_data_files_count", "type": ["null", "int"], "default": null},
                {"name": "existing_data_files_count", "type": ["null", "int"], "default": null},
                {"name": "deleted_data_files_count", "type": ["null", "int"], "default": null},
                {"name": "added_rows_count", "type": ["null", "long"], "default": null},
                {"name": "existing_rows_count", "type": ["null", "long"], "default": null},
                {"name": "deleted_rows_count", "type": ["null", "long"], "default": null}
              ]
            }"#,
        )
        .expect("nullable manifest list schema");
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer
            .append_ser(NullableManifestCounts {
                manifest_path,
                manifest_length,
                partition_spec_id: 2,
                added_snapshot_id: 7,
                added_data_files_count: None,
                existing_data_files_count: None,
                deleted_data_files_count: None,
                added_rows_count: None,
                existing_rows_count: None,
                deleted_rows_count: None,
            })
            .expect("nullable manifest list entry");
        writer.into_inner().expect("nullable manifest list bytes")
    }

    fn manifest_file(content: ManifestContentType) -> ManifestFile {
        ManifestFile::builder()
            .with_manifest_path("metadata/manifest.avro")
            .with_content(content)
            .build()
            .expect("manifest file")
    }

    fn delete_file(path: &str, partition_spec_id: i32) -> DataFile {
        DataFile {
            content: DataContentType::PositionDeletes,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count: 1,
            file_size_in_bytes: 1,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id,
            referenced_data_file: Some("data.parquet".to_string()),
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn data_file(path: &str, record_count: u64) -> DataFile {
        let mut file = delete_file(path, 0);
        file.content = DataContentType::Data;
        file.record_count = record_count;
        file.referenced_data_file = None;
        file
    }

    #[test]
    fn targeted_rewrite_rejects_a_removal_path_missing_from_parent() {
        futures::executor::block_on(async {
            let table_url =
                url::Url::parse("file:///tmp/iceberg-missing-removal/").expect("table URL");
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let parent_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(10)
                .with_sequence_number(1)
                .with_manifest_list("metadata/parent-list.avro")
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let transaction = Transaction::new(table_url.to_string(), parent_snapshot, 1);
            let producer =
                SnapshotProducer::new(&transaction, vec![], Some(store_ctx.clone()), None);
            let mut created_paths = Vec::new();
            let error = producer
                .rewrite_parent_manifests(
                    &store_ctx,
                    vec![],
                    &HashSet::from(["data/missing.parquet".to_string()]),
                    2,
                    11,
                    &mut created_paths,
                )
                .await
                .expect_err("missing removal path must fail");
            assert!(error.contains("data/missing.parquet"));
            assert!(created_paths.is_empty());
        });
    }

    #[test]
    fn targeted_rewrite_marks_removed_files_and_reuses_unaffected_manifests() {
        futures::executor::block_on(async {
            let table_url =
                url::Url::parse("file:///tmp/iceberg-targeted-rewrite/").expect("table URL");
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = Schema::builder().build().expect("schema");
            let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
            let manifest_metadata = ManifestMetadata::new(
                Arc::new(schema.clone()),
                0,
                partition_spec.clone(),
                FormatVersion::V2,
                ManifestContentType::Data,
            );
            let parent_snapshot_id = 10;
            let parent_sequence_number = 1;

            let mut affected_writer = ManifestWriterBuilder::new(
                Some(parent_snapshot_id),
                None,
                manifest_metadata.clone(),
            )
            .build();
            affected_writer.add(data_file("data/remove.parquet", 2));
            affected_writer.add(data_file("data/survivor.parquet", 3));
            let affected_bytes = affected_writer
                .to_avro_bytes_v2()
                .expect("affected manifest");
            let mut affected_file = affected_writer.into_manifest_file(
                "metadata/affected.avro".to_string(),
                parent_sequence_number,
                parent_snapshot_id,
            );
            affected_file.manifest_length = affected_bytes.len() as i64;
            store_ctx
                .prefixed
                .put(
                    &ObjectPath::from("metadata/affected.avro"),
                    object_store::PutPayload::from(Bytes::from(affected_bytes)),
                )
                .await
                .expect("write affected manifest");

            let mut unaffected_writer = ManifestWriterBuilder::new(
                Some(parent_snapshot_id),
                None,
                manifest_metadata.clone(),
            )
            .build();
            unaffected_writer.add(data_file("data/unaffected.parquet", 5));
            let unaffected_bytes = unaffected_writer
                .to_avro_bytes_v2()
                .expect("unaffected manifest");
            let mut unaffected_file = unaffected_writer.into_manifest_file(
                "metadata/unaffected.avro".to_string(),
                parent_sequence_number,
                parent_snapshot_id,
            );
            unaffected_file.manifest_length = unaffected_bytes.len() as i64;
            store_ctx
                .prefixed
                .put(
                    &ObjectPath::from("metadata/unaffected.avro"),
                    object_store::PutPayload::from(Bytes::from(unaffected_bytes)),
                )
                .await
                .expect("write unaffected manifest");

            let mut parent_list = ManifestListWriter::new();
            parent_list.append(affected_file);
            parent_list.append(unaffected_file);
            let parent_list_bytes = parent_list
                .to_bytes(FormatVersion::V2)
                .expect("parent manifest list");
            store_ctx
                .prefixed
                .put(
                    &ObjectPath::from("metadata/parent-list.avro"),
                    object_store::PutPayload::from(Bytes::from(parent_list_bytes)),
                )
                .await
                .expect("write parent manifest list");

            let parent_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(parent_snapshot_id)
                .with_sequence_number(parent_sequence_number)
                .with_manifest_list("metadata/parent-list.avro")
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let transaction = Transaction::new(
                table_url.to_string(),
                parent_snapshot,
                parent_sequence_number,
            );
            let action_commit = SnapshotProducer::new(
                &transaction,
                vec![data_file("data/replacement.parquet", 1)],
                Some(store_ctx.clone()),
                Some(manifest_metadata),
            )
            .with_partition_specs(vec![partition_spec])
            .with_removed_data_file_paths(vec!["data/remove.parquet".to_string()])
            .commit(SnapshotUpdateKind::CopyOnWrite)
            .await
            .expect("targeted rewrite");

            let snapshot = action_commit
                .updates()
                .iter()
                .find_map(|update| match update {
                    TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                    _ => None,
                })
                .expect("new snapshot");
            assert_eq!(snapshot.summary.operation, Operation::Overwrite);
            let summary = &snapshot.summary.additional_properties;
            assert_eq!(
                summary.get("deleted-data-files").map(String::as_str),
                Some("1")
            );
            assert_eq!(
                summary.get("deleted-records").map(String::as_str),
                Some("2")
            );
            assert_eq!(
                summary.get("total-data-files").map(String::as_str),
                Some("3")
            );
            assert_eq!(summary.get("total-records").map(String::as_str), Some("9"));

            let manifest_list = crate::io::load_manifest_list(&store_ctx, snapshot.manifest_list())
                .await
                .expect("new manifest list");
            assert!(
                manifest_list
                    .entries()
                    .iter()
                    .any(|manifest| { manifest.manifest_path == "metadata/unaffected.avro" })
            );
            let mut statuses = HashMap::new();
            for manifest_file in manifest_list.entries() {
                let manifest = crate::io::load_manifest(&store_ctx, &manifest_file.manifest_path)
                    .await
                    .expect("manifest");
                for entry in manifest.entries() {
                    statuses.insert(entry.data_file.file_path.clone(), entry.status);
                }
            }
            assert_eq!(statuses["data/remove.parquet"], ManifestStatus::Deleted);
            assert_eq!(statuses["data/survivor.parquet"], ManifestStatus::Existing);
            assert_eq!(statuses["data/unaffected.parquet"], ManifestStatus::Added);
            assert_eq!(statuses["data/replacement.parquet"], ManifestStatus::Added);
        });
    }

    #[test]
    fn manifest_totals_omit_only_totals_with_unknown_active_counts() {
        let data_with_unknown_counts = manifest_file(ManifestContentType::Data);
        let deletes_with_unknown_files = manifest_file(ManifestContentType::Deletes);
        let summary = with_manifest_totals(
            crate::spec::snapshots::Summary::new(Operation::Append),
            DeleteRowTotalsBase::Parent(&crate::spec::snapshots::Summary::new(Operation::Append)),
            [&data_with_unknown_counts, &deletes_with_unknown_files],
            0,
            0,
        );

        assert!(
            !summary
                .additional_properties
                .contains_key("total-data-files")
        );
        assert!(!summary.additional_properties.contains_key("total-records"));
        assert!(
            !summary
                .additional_properties
                .contains_key("total-delete-files")
        );
    }

    #[test]
    fn full_overwrite_recomputes_snapshot_totals_from_new_manifests() {
        let parent_summary = crate::spec::snapshots::Summary::new(Operation::Append)
            .with_property("total-position-deletes", "7")
            .with_property("total-equality-deletes", "5");
        let replacement_data = ManifestFile::builder()
            .with_manifest_path("metadata/replacement.avro")
            .with_content(ManifestContentType::Data)
            .with_file_counts(2, 0, 0)
            .with_row_counts(10, 0, 0)
            .build()
            .expect("replacement manifest");

        let summary = with_manifest_totals(
            crate::spec::snapshots::Summary::new(Operation::Overwrite),
            SnapshotUpdateKind::FullOverwrite.delete_totals_base(&parent_summary, false),
            [&replacement_data],
            0,
            0,
        );

        let totals = &summary.additional_properties;
        assert_eq!(
            totals.get("total-data-files").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            totals.get("total-delete-files").map(String::as_str),
            Some("0")
        );
        assert_eq!(totals.get("total-records").map(String::as_str), Some("10"));
        assert_eq!(
            totals.get("total-position-deletes").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            totals.get("total-equality-deletes").map(String::as_str),
            Some("0")
        );
    }

    #[test]
    fn delete_manifest_inputs_group_files_by_historical_partition_spec() {
        let schema = Schema::builder().build().expect("schema");
        let current_spec = PartitionSpec::builder().with_spec_id(2).build();
        let historical_spec = PartitionSpec::builder().with_spec_id(1).build();
        let metadata = ManifestMetadata::new(
            Arc::new(schema),
            0,
            current_spec.clone(),
            FormatVersion::V2,
            ManifestContentType::Data,
        );
        let files = vec![
            delete_file("delete-1-a.parquet", 1),
            delete_file("delete-2.parquet", 2),
            delete_file("delete-1-b.parquet", 1),
        ];

        let groups = delete_manifest_inputs(&metadata, &[historical_spec], &files)
            .expect("partition specs should resolve");

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0.partition_spec.spec_id(), 1);
        assert_eq!(groups[0].0.content, ManifestContentType::Deletes);
        assert_eq!(groups[0].1.len(), 2);
        assert!(groups[0].1.iter().all(|file| file.partition_spec_id == 1));
        assert_eq!(groups[1].0.partition_spec.spec_id(), 2);
        assert_eq!(groups[1].0.content, ManifestContentType::Deletes);
        assert_eq!(groups[1].1.len(), 1);
        assert!(groups[1].1.iter().all(|file| file.partition_spec_id == 2));
    }

    #[test]
    fn delete_manifest_inputs_reject_missing_partition_spec() {
        let metadata = ManifestMetadata::new(
            Arc::new(Schema::builder().build().expect("schema")),
            0,
            PartitionSpec::builder().with_spec_id(2).build(),
            FormatVersion::V2,
            ManifestContentType::Data,
        );

        let error = delete_manifest_inputs(&metadata, &[], &[delete_file("delete.parquet", 1)])
            .expect_err("unknown partition spec must fail");

        assert!(error.contains("partition spec 1 is missing"));
    }

    #[test]
    fn snapshot_commit_rejects_position_deletes_after_format_v3_upgrade() {
        futures::executor::block_on(async {
            let table_url =
                url::Url::parse("file:///tmp/iceberg-v3-position-delete/").expect("table URL");
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = Schema::builder().build().expect("schema");
            let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
            let metadata = ManifestMetadata::new(
                Arc::new(schema),
                0,
                partition_spec.clone(),
                FormatVersion::V3,
                ManifestContentType::Data,
            );
            let parent_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(0)
                .with_sequence_number(0)
                .with_manifest_list(String::new())
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let transaction = Transaction::new(table_url.to_string(), parent_snapshot, 0);

            let result =
                SnapshotProducer::new(&transaction, vec![], Some(store_ctx), Some(metadata))
                    .with_bootstrap(true)
                    .with_added_delete_files(vec![delete_file("delete.parquet", 0)])
                    .with_partition_specs(vec![partition_spec])
                    .commit(SnapshotUpdateKind::RowDelta)
                    .await;

            let error = result
                .err()
                .expect("v3 snapshot commit must reject position delete files");
            assert!(error.contains("v3"));
            assert!(error.contains("position delete"));
        });
    }

    #[test]
    fn snapshot_producer_removes_manifests_when_manifest_list_write_fails() {
        futures::executor::block_on(async {
            let table_url =
                url::Url::parse("file:///tmp/iceberg-manifest-cleanup/").expect("table URL");
            let memory_store = Arc::new(object_store::memory::InMemory::new());
            let store: Arc<dyn ObjectStore> = Arc::new(ManifestListRejectingStore {
                memory_store: Arc::clone(&memory_store),
            });
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = Schema::builder().build().expect("schema");
            let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
            let metadata = ManifestMetadata::new(
                Arc::new(schema),
                0,
                partition_spec.clone(),
                FormatVersion::V2,
                ManifestContentType::Data,
            );
            let parent_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(0)
                .with_sequence_number(0)
                .with_manifest_list(String::new())
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let transaction = Transaction::new(table_url.to_string(), parent_snapshot, 0);
            let mut data_file = delete_file("data.parquet", 0);
            data_file.content = DataContentType::Data;
            data_file.referenced_data_file = None;

            let error = SnapshotProducer::new(
                &transaction,
                vec![data_file],
                Some(store_ctx),
                Some(metadata),
            )
            .with_bootstrap(true)
            .with_partition_specs(vec![partition_spec])
            .commit(SnapshotUpdateKind::FastAppend)
            .await
            .err()
            .expect("manifest-list write must fail");
            assert!(error.contains("injected manifest-list failure"));

            let remaining_paths = memory_store
                .list(None)
                .map_ok(|metadata| metadata.location.to_string())
                .try_collect::<Vec<_>>()
                .await
                .expect("list object store");
            assert!(
                remaining_paths.is_empty(),
                "failed snapshot left uncommitted objects: {remaining_paths:?}"
            );
        });
    }

    #[test]
    fn prepared_snapshot_cleanup_removes_uncommitted_manifests_and_list() {
        futures::executor::block_on(async {
            let table_url =
                url::Url::parse("file:///tmp/iceberg-prepared-cleanup/").expect("table URL");
            let memory_store = Arc::new(object_store::memory::InMemory::new());
            let store: Arc<dyn ObjectStore> = memory_store.clone();
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = Schema::builder().build().expect("schema");
            let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
            let metadata = ManifestMetadata::new(
                Arc::new(schema),
                0,
                partition_spec.clone(),
                FormatVersion::V2,
                ManifestContentType::Data,
            );
            let parent_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(0)
                .with_sequence_number(0)
                .with_manifest_list(String::new())
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let transaction = Transaction::new(table_url.to_string(), parent_snapshot, 0);
            let mut data_file = delete_file("data.parquet", 0);
            data_file.content = DataContentType::Data;
            data_file.referenced_data_file = None;

            let prepared_snapshot = SnapshotProducer::new(
                &transaction,
                vec![data_file],
                Some(store_ctx),
                Some(metadata),
            )
            .with_bootstrap(true)
            .with_partition_specs(vec![partition_spec])
            .prepare(SnapshotUpdateKind::FastAppend)
            .await
            .expect("prepare snapshot");
            assert!(
                memory_store
                    .list(None)
                    .try_next()
                    .await
                    .expect("list store")
                    .is_some()
            );

            prepared_snapshot.cleanup().await;

            assert!(
                memory_store
                    .list(None)
                    .try_next()
                    .await
                    .expect("list store")
                    .is_none()
            );
        });
    }

    #[test]
    fn snapshot_commit_uses_data_files_historical_partition_spec() {
        futures::executor::block_on(async {
            let table_url = url::Url::parse("file:///tmp/iceberg-concurrent-spec-evolution/")
                .expect("table URL");
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = Schema::builder().build().expect("schema");
            let historical_spec = PartitionSpec::builder().with_spec_id(1).build();
            let current_spec = PartitionSpec::builder().with_spec_id(2).build();
            let metadata = ManifestMetadata::new(
                Arc::new(schema),
                0,
                current_spec.clone(),
                FormatVersion::V2,
                ManifestContentType::Data,
            );
            let parent_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(0)
                .with_sequence_number(0)
                .with_manifest_list(String::new())
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let transaction = Transaction::new(table_url.to_string(), parent_snapshot, 0);
            let mut added_data_file = delete_file("data.parquet", historical_spec.spec_id());
            added_data_file.content = DataContentType::Data;
            added_data_file.referenced_data_file = None;

            let action_commit = SnapshotProducer::new(
                &transaction,
                vec![added_data_file],
                Some(store_ctx.clone()),
                Some(metadata),
            )
            .with_bootstrap(true)
            .with_partition_specs(vec![historical_spec, current_spec])
            .commit(SnapshotUpdateKind::RowDelta)
            .await
            .expect("snapshot commit");
            let snapshot = action_commit
                .updates()
                .iter()
                .find_map(|update| match update {
                    TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                    _ => None,
                })
                .expect("added snapshot");
            let manifest_list = crate::io::load_manifest_list(&store_ctx, snapshot.manifest_list())
                .await
                .expect("manifest list");
            let added_data_manifest = manifest_list
                .entries()
                .iter()
                .find(|manifest| manifest.content == ManifestContentType::Data)
                .expect("data manifest");

            assert_eq!(added_data_manifest.partition_spec_id, 1);
        });
    }

    #[test]
    fn snapshot_producer_writes_historical_delete_manifests_and_table_sequence() {
        futures::executor::block_on(async {
            let table_url = url::Url::parse("file:///tmp/iceberg-table/").expect("table URL");
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "part",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("schema");
            let historical_spec = PartitionSpec::builder()
                .with_spec_id(1)
                .add_field_with_id(1, 1000, "part", Transform::Identity)
                .build();
            let current_spec = PartitionSpec::builder().with_spec_id(2).build();
            let metadata = ManifestMetadata::new(
                Arc::new(schema),
                0,
                current_spec.clone(),
                FormatVersion::V2,
                ManifestContentType::Data,
            );
            let mut parent_manifest_writer =
                ManifestWriterBuilder::new(None, None, metadata.clone()).build();
            let mut parent_data_file = delete_file("data.parquet", 2);
            parent_data_file.content = DataContentType::Data;
            parent_data_file.referenced_data_file = None;
            parent_manifest_writer.add(parent_data_file);
            let parent_manifest_bytes = parent_manifest_writer
                .finish()
                .to_avro_bytes_v2()
                .expect("parent manifest bytes");
            store_ctx
                .prefixed
                .put(
                    &object_store::path::Path::from("metadata/parent-manifest.avro"),
                    object_store::PutPayload::from(Bytes::from(parent_manifest_bytes.clone())),
                )
                .await
                .expect("parent manifest");
            let parent_list_bytes = nullable_manifest_list_bytes(
                "metadata/parent-manifest.avro",
                parent_manifest_bytes.len() as i64,
            );
            store_ctx
                .prefixed
                .put(
                    &object_store::path::Path::from("metadata/parent-list.avro"),
                    object_store::PutPayload::from(Bytes::from(parent_list_bytes)),
                )
                .await
                .expect("parent manifest list");
            let parent_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(7)
                .with_sequence_number(3)
                .with_manifest_list("metadata/parent-list.avro".to_string())
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let transaction = Transaction::new(table_url.to_string(), parent_snapshot, 11);
            let mut historical_delete = delete_file("delete-1.parquet", 1);
            historical_delete.partition = vec![Some(Literal::Primitive(PrimitiveLiteral::Int(42)))];
            let action_commit = SnapshotProducer::new(
                &transaction,
                vec![],
                Some(store_ctx.clone()),
                Some(metadata),
            )
            .with_added_delete_files(vec![historical_delete, delete_file("delete-2.parquet", 2)])
            .with_partition_specs(vec![historical_spec, current_spec])
            .commit(SnapshotUpdateKind::RowDelta)
            .await
            .expect("row delta snapshot");
            let snapshot = action_commit
                .updates()
                .iter()
                .find_map(|update| match update {
                    TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                    _ => None,
                })
                .expect("added snapshot");

            assert_eq!(snapshot.sequence_number(), 12);
            assert_eq!(
                snapshot
                    .summary()
                    .additional_properties
                    .get("total-data-files"),
                Some(&"1".to_string())
            );
            assert_eq!(
                snapshot
                    .summary()
                    .additional_properties
                    .get("total-records"),
                Some(&"1".to_string())
            );
            let manifest_list = crate::io::load_manifest_list(&store_ctx, snapshot.manifest_list())
                .await
                .expect("manifest list");
            let delete_manifests = manifest_list
                .entries()
                .iter()
                .filter(|manifest| manifest.content == ManifestContentType::Deletes)
                .collect::<Vec<_>>();
            assert_eq!(delete_manifests.len(), 2);
            for manifest_file in delete_manifests {
                let manifest =
                    crate::io::load_manifest(&store_ctx, manifest_file.manifest_path.as_str())
                        .await
                        .expect("delete manifest");
                assert_eq!(
                    manifest.metadata().partition_spec.spec_id(),
                    manifest_file.partition_spec_id
                );
                assert_eq!(manifest.metadata().content, ManifestContentType::Deletes);
                assert!(manifest.entries().iter().all(|entry| {
                    entry.data_file.partition_spec_id == manifest_file.partition_spec_id
                }));
                if manifest_file.partition_spec_id == 1 {
                    assert_eq!(manifest.metadata().partition_spec.fields().len(), 1);
                    assert_eq!(
                        manifest.entries()[0].data_file.partition,
                        vec![Some(Literal::Primitive(PrimitiveLiteral::Int(42)))]
                    );
                }
            }
        });
    }
}
