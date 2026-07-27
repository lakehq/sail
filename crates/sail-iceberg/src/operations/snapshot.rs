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

use std::collections::HashSet;

use bytes::Bytes;
use object_store::ObjectStoreExt;

use super::{ActionCommit, Transaction};
use crate::io::StoreContext;
use crate::spec::manifest::{ManifestEntry, ManifestStatus, ManifestWriter, ManifestWriterBuilder};
use crate::spec::manifest_list::ManifestListWriter;
use crate::spec::{
    DataFile, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestFile, Operation,
    PartitionSpec, Schema, SnapshotBuilder, SnapshotReference, SnapshotRetention, TableRequirement,
    TableUpdate,
};
use crate::utils::join_table_uri;

pub trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> &'static str;

    fn deleted_data_file_paths_for_rewrite(&self) -> Option<&[String]> {
        None
    }
}

pub struct SnapshotProducer<'a> {
    pub tx: &'a Transaction,
    pub added_data_files: Vec<DataFile>,
    pub store_ctx: Option<StoreContext>,
    pub manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
    pub write_path_mode: crate::utils::WritePathMode,
    /// If true, create a snapshot with no parent (for bootstrap scenarios)
    pub is_bootstrap: bool,
    pub row_lineage_start_row_id: Option<i64>,
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
            store_ctx,
            manifest_metadata,
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

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        // TODO: Implement this function to validate the added data files
        Ok(())
    }

    async fn write_manifest(
        &self,
        store_ctx: &StoreContext,
        writer: ManifestWriter,
        sequence_number: i64,
        snapshot_id: i64,
        first_row_id: Option<i64>,
    ) -> Result<ManifestFile, String> {
        let manifest_bytes = writer.to_avro_bytes_v2()?;
        let manifest_len = i64::try_from(manifest_bytes.len())
            .map_err(|_| "manifest length exceeds i64".to_string())?;
        let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
        let mut manifest_file = writer.into_manifest_file(
            join_table_uri(self.tx.table_uri(), &manifest_rel, &self.write_path_mode),
            sequence_number,
            snapshot_id,
        );
        manifest_file.manifest_length = manifest_len;
        manifest_file.first_row_id = first_row_id;

        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from(manifest_rel.as_str()),
                object_store::PutPayload::from(Bytes::from(manifest_bytes)),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(manifest_file)
    }

    fn materialize_inherited_entry(
        mut entry: ManifestEntry,
        manifest_file: &ManifestFile,
        inherited_next_row_id: &mut Option<i64>,
    ) -> Result<ManifestEntry, String> {
        entry.snapshot_id = entry.snapshot_id.or(Some(manifest_file.added_snapshot_id));
        // V1 entries have no sequence columns and default to 0. In V2 and later,
        // only Added entries may inherit sequence numbers from manifest metadata.
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
                    .map_err(|_| "data file record count exceeds i64".to_string())?;
                *next_row_id = next_row_id
                    .checked_add(record_count)
                    .ok_or_else(|| "row lineage id overflow".to_string())?;
            }
        }
        Ok(entry)
    }

    async fn rewrite_parent_manifests(
        &self,
        store_ctx: &StoreContext,
        parent_manifests: Vec<ManifestFile>,
        deleted_data_file_paths: &HashSet<String>,
        sequence_number: i64,
        snapshot_id: i64,
    ) -> Result<(Vec<ManifestFile>, i64, i64, i64, i64), String> {
        enum PlannedManifest {
            Reuse(ManifestFile),
            Rewrite(ManifestWriter),
        }

        let mut planned_manifests = Vec::with_capacity(parent_manifests.len());
        let mut found_paths = HashSet::new();
        let mut parent_live_files = 0i64;
        let mut parent_live_rows = 0i64;

        for parent_manifest_file in parent_manifests {
            if !matches!(parent_manifest_file.content, ManifestContentType::Data) {
                planned_manifests.push((parent_manifest_file, None));
                continue;
            }

            let manifest = crate::io::load_manifest(store_ctx, &parent_manifest_file.manifest_path)
                .await
                .map_err(|e| format!("failed to load parent manifest: {e}"))?;
            let mut contains_deleted_path = false;
            for entry in manifest.entries().iter().filter(|entry| {
                matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                )
            }) {
                parent_live_files = parent_live_files
                    .checked_add(1)
                    .ok_or_else(|| "parent data file count overflow".to_string())?;
                let record_count = i64::try_from(entry.data_file.record_count)
                    .map_err(|_| "data file record count exceeds i64".to_string())?;
                parent_live_rows = parent_live_rows
                    .checked_add(record_count)
                    .ok_or_else(|| "parent record count overflow".to_string())?;
                if deleted_data_file_paths.contains(&entry.data_file.file_path) {
                    contains_deleted_path = true;
                    found_paths.insert(entry.data_file.file_path.clone());
                }
            }
            planned_manifests.push((
                parent_manifest_file,
                contains_deleted_path.then_some(manifest),
            ));
        }

        let mut missing_paths = deleted_data_file_paths
            .difference(&found_paths)
            .cloned()
            .collect::<Vec<_>>();
        if !missing_paths.is_empty() {
            missing_paths.sort();
            return Err(format!(
                "rewrite data files are not live in the parent snapshot: {}",
                missing_paths.join(", ")
            ));
        }

        let mut output_plan = Vec::with_capacity(planned_manifests.len());
        let mut deleted_files = 0i64;
        let mut deleted_rows = 0i64;
        for (parent_manifest_file, manifest) in planned_manifests {
            let Some(manifest) = manifest else {
                output_plan.push(PlannedManifest::Reuse(parent_manifest_file));
                continue;
            };

            let (entries, metadata) = manifest.into_parts();
            let mut writer = ManifestWriterBuilder::new(Some(snapshot_id), None, metadata).build();
            let mut inherited_next_row_id = parent_manifest_file.first_row_id;
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
                if deleted_data_file_paths.contains(&entry.data_file.file_path) {
                    deleted_files = deleted_files
                        .checked_add(1)
                        .ok_or_else(|| "deleted data file count overflow".to_string())?;
                    let record_count = i64::try_from(entry.data_file.record_count)
                        .map_err(|_| "data file record count exceeds i64".to_string())?;
                    deleted_rows = deleted_rows
                        .checked_add(record_count)
                        .ok_or_else(|| "deleted record count overflow".to_string())?;
                    writer.add_deleted_entry(entry)?;
                } else {
                    writer.add_existing_entry(entry)?;
                }
            }
            output_plan.push(PlannedManifest::Rewrite(writer));
        }

        let mut output_manifests = Vec::with_capacity(output_plan.len());
        for manifest in output_plan {
            match manifest {
                PlannedManifest::Reuse(manifest_file) => output_manifests.push(manifest_file),
                PlannedManifest::Rewrite(writer) => output_manifests.push(
                    self.write_manifest(store_ctx, writer, sequence_number, snapshot_id, None)
                        .await?,
                ),
            }
        }

        Ok((
            output_manifests,
            parent_live_files,
            parent_live_rows,
            deleted_files,
            deleted_rows,
        ))
    }

    pub async fn commit(self, op: impl SnapshotProduceOperation) -> Result<ActionCommit, String> {
        let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
        let is_overwrite = op.operation() == Operation::Overwrite.as_str();
        let deleted_data_file_paths = op
            .deleted_data_file_paths_for_rewrite()
            .map(|paths| paths.iter().cloned().collect::<HashSet<_>>());
        if deleted_data_file_paths
            .as_ref()
            .is_some_and(HashSet::is_empty)
        {
            return Err("rewrite requires at least one data file path to delete".to_string());
        }
        let is_rewrite = deleted_data_file_paths.is_some();

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

        let store_ctx = self
            .store_ctx
            .as_ref()
            .ok_or_else(|| "store context not available".to_string())?;

        // Generate new snapshot ID using UUID (not timestamp) and sequence number
        let new_snapshot_id = crate::utils::snapshot_id::generate_snapshot_id();
        let new_sequence_number = if self.is_bootstrap {
            1 // First snapshot starts at sequence 1
        } else {
            self.tx.snapshot().sequence_number() + 1
        };

        let parent_snapshot = self.tx.snapshot();
        let parent_manifest_list_path_str = parent_snapshot.manifest_list();
        let mut parent_manifest_entries = Vec::new();

        if !self.is_bootstrap
            && (!is_overwrite || is_rewrite)
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
            parent_manifest_entries.extend(parent_manifest_list.entries().iter().cloned());
        }

        let mut new_added_rows = 0i64;
        for data_file in &self.added_data_files {
            let record_count = i64::try_from(data_file.record_count)
                .map_err(|_| "data file record count exceeds i64".to_string())?;
            new_added_rows = new_added_rows
                .checked_add(record_count)
                .ok_or_else(|| "added record count overflow".to_string())?;
        }
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

        let new_manifest_first_row_id = row_lineage_next_row_id;
        if self.row_lineage_start_row_id.is_some() {
            snapshot_added_rows += new_added_rows;
        }

        let added_data_files = self.added_data_files.clone();
        let added_files = i64::try_from(added_data_files.len())
            .map_err(|_| "added data file count exceeds i64".to_string())?;
        let (
            parent_manifest_entries,
            parent_live_files,
            parent_live_rows,
            deleted_files,
            deleted_rows,
        ) = if let Some(paths) = deleted_data_file_paths.as_ref() {
            self.rewrite_parent_manifests(
                store_ctx,
                parent_manifest_entries,
                paths,
                new_sequence_number,
                new_snapshot_id,
            )
            .await?
        } else {
            (parent_manifest_entries, 0, 0, 0, 0)
        };

        let mut summary = if is_overwrite {
            crate::spec::snapshots::Summary::new(Operation::Overwrite)
        } else {
            crate::spec::snapshots::Summary::new(Operation::Append)
        };
        if is_rewrite {
            let total_data_files = parent_live_files
                .checked_sub(deleted_files)
                .and_then(|count| count.checked_add(added_files))
                .ok_or_else(|| "total data file count overflow".to_string())?;
            let total_records = parent_live_rows
                .checked_sub(deleted_rows)
                .and_then(|count| count.checked_add(new_added_rows))
                .ok_or_else(|| "total record count overflow".to_string())?;
            summary = summary
                .with_property("added-data-files", added_files)
                .with_property("deleted-data-files", deleted_files)
                .with_property("added-records", new_added_rows)
                .with_property("deleted-records", deleted_rows)
                .with_property("total-data-files", total_data_files)
                .with_property("total-records", total_records);
        }

        let mut list_writer = ManifestListWriter::new();
        let mut total_manifest_count = 0usize;
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

        if !is_rewrite || !added_data_files.is_empty() {
            let mut writer = ManifestWriterBuilder::new(None, None, metadata.clone()).build();
            for data_file in added_data_files {
                writer.add(data_file);
            }
            list_writer.append(
                self.write_manifest(
                    store_ctx,
                    writer,
                    new_sequence_number,
                    new_snapshot_id,
                    new_manifest_first_row_id,
                )
                .await?,
            );
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
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use super::*;
    use crate::datasource::IcebergTableProvider;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::io::{load_manifest, load_manifest_list};
    use crate::operations::RewriteFilesOperation;
    use crate::spec::manifest::{ManifestEntry, ManifestStatus};
    use crate::spec::types::{NestedField, PrimitiveType, Type};
    use crate::spec::{DataContentType, DataFileFormat, ManifestListWriter};
    use crate::utils::WritePathMode;

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }

    fn data_file(path: &str, record_count: u64, file_size_in_bytes: u64) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count,
            file_size_in_bytes,
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
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn parquet_bytes(schema: Arc<datafusion::arrow::datatypes::Schema>, ids: Vec<i64>) -> Vec<u8> {
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    struct SingleFileParent {
        store_ctx: StoreContext,
        manifest_metadata: crate::spec::manifest::ManifestMetadata,
        tx: Transaction,
        live_file: DataFile,
    }

    async fn single_file_parent(table_url: &str) -> SingleFileParent {
        let table_url = Url::parse(table_url).unwrap();
        let object_store = Arc::new(InMemory::new());
        let store_ctx = StoreContext::new(object_store, &table_url).unwrap();
        let schema = test_schema();
        let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
        let manifest_metadata = crate::spec::manifest::ManifestMetadata::new(
            Arc::new(schema.clone()),
            schema.schema_id(),
            partition_spec,
            FormatVersion::V2,
            ManifestContentType::Data,
        );
        let parent_snapshot_id = 10;
        let parent_sequence_number = 1;
        let live_file = data_file("data/live.parquet", 1, 100);
        let mut parent_writer =
            ManifestWriterBuilder::new(Some(parent_snapshot_id), None, manifest_metadata.clone())
                .build();
        parent_writer.add(live_file.clone());
        let parent_manifest_bytes = parent_writer.to_avro_bytes_v2().unwrap();
        let mut parent_manifest_file = parent_writer.into_manifest_file(
            "metadata/manifest-parent.avro".to_string(),
            parent_sequence_number,
            parent_snapshot_id,
        );
        parent_manifest_file.manifest_length = parent_manifest_bytes.len() as i64;
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("metadata/manifest-parent.avro"),
                object_store::PutPayload::from(Bytes::from(parent_manifest_bytes)),
            )
            .await
            .unwrap();

        let mut parent_list_writer = ManifestListWriter::new();
        parent_list_writer.append(parent_manifest_file);
        let parent_list_bytes = parent_list_writer.to_bytes(FormatVersion::V2).unwrap();
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("metadata/snap-parent.avro"),
                object_store::PutPayload::from(Bytes::from(parent_list_bytes)),
            )
            .await
            .unwrap();

        let parent_snapshot = SnapshotBuilder::new()
            .with_snapshot_id(parent_snapshot_id)
            .with_sequence_number(parent_sequence_number)
            .with_manifest_list("metadata/snap-parent.avro")
            .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
            .with_schema_id(schema.schema_id())
            .build()
            .unwrap();
        let tx = Transaction::new(table_url.to_string(), parent_snapshot);
        SingleFileParent {
            store_ctx,
            manifest_metadata,
            tx,
            live_file,
        }
    }

    #[test]
    fn inherited_row_ids_do_not_advance_for_preassigned_files() {
        let parent_manifest = ManifestFile::builder()
            .with_manifest_path("metadata/parent.avro")
            .with_sequence_number(1)
            .with_min_sequence_number(1)
            .with_added_snapshot_id(10)
            .with_file_counts(2, 0, 0)
            .with_row_counts(15, 0, 0)
            .with_first_row_id(100)
            .build()
            .unwrap();
        let mut assigned_file = data_file("data/assigned.parquet", 10, 100);
        assigned_file.first_row_id = Some(50);
        let assigned_entry = ManifestEntry::new(
            ManifestStatus::Added,
            Some(10),
            Some(1),
            Some(1),
            assigned_file,
        );
        let unassigned_entry = ManifestEntry::new(
            ManifestStatus::Added,
            Some(10),
            Some(1),
            Some(1),
            data_file("data/unassigned.parquet", 5, 100),
        );
        let mut inherited_next_row_id = parent_manifest.first_row_id;

        let assigned_entry = SnapshotProducer::<'static>::materialize_inherited_entry(
            assigned_entry,
            &parent_manifest,
            &mut inherited_next_row_id,
        )
        .unwrap();
        let unassigned_entry = SnapshotProducer::<'static>::materialize_inherited_entry(
            unassigned_entry,
            &parent_manifest,
            &mut inherited_next_row_id,
        )
        .unwrap();

        assert_eq!(assigned_entry.data_file.first_row_id, Some(50));
        assert_eq!(unassigned_entry.data_file.first_row_id, Some(100));
        assert_eq!(inherited_next_row_id, Some(105));
    }

    #[test]
    fn existing_entries_only_inherit_sequence_numbers_for_v1() {
        let parent_manifest = ManifestFile::builder()
            .with_manifest_path("metadata/parent.avro")
            .with_sequence_number(2)
            .with_min_sequence_number(1)
            .with_added_snapshot_id(20)
            .with_file_counts(0, 1, 0)
            .with_row_counts(0, 1, 0)
            .build()
            .unwrap();
        let entry = ManifestEntry::new(
            ManifestStatus::Existing,
            Some(10),
            None,
            None,
            data_file("data/existing.parquet", 1, 100),
        );

        let result = SnapshotProducer::<'static>::materialize_inherited_entry(
            entry,
            &parent_manifest,
            &mut None,
        );

        assert!(matches!(
            result,
            Err(message) if message.contains("sequence numbers")
        ));

        let mut v1_manifest = parent_manifest;
        v1_manifest.sequence_number = 0;
        let v1_entry = ManifestEntry::new(
            ManifestStatus::Existing,
            Some(10),
            None,
            None,
            data_file("data/v1-existing.parquet", 1, 100),
        );
        let v1_entry = SnapshotProducer::<'static>::materialize_inherited_entry(
            v1_entry,
            &v1_manifest,
            &mut None,
        )
        .unwrap();
        assert_eq!(v1_entry.sequence_number, Some(0));
        assert_eq!(v1_entry.file_sequence_number, Some(0));
    }

    #[tokio::test]
    async fn rewrite_files_marks_removed_files_and_preserves_survivor_rows() {
        let table_url = Url::parse("memory://rewrite-test/table/").unwrap();
        let object_store = Arc::new(InMemory::new());
        let store_ctx = StoreContext::new(object_store.clone(), &table_url).unwrap();
        let schema = test_schema();
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&schema).unwrap());
        let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
        let manifest_metadata = crate::spec::manifest::ManifestMetadata::new(
            Arc::new(schema.clone()),
            schema.schema_id(),
            partition_spec.clone(),
            FormatVersion::V2,
            ManifestContentType::Data,
        );

        let old_bytes = parquet_bytes(arrow_schema.clone(), vec![1]);
        let survivor_bytes = parquet_bytes(arrow_schema.clone(), vec![2]);
        let replacement_bytes = parquet_bytes(arrow_schema.clone(), vec![3]);
        let unaffected_bytes = parquet_bytes(arrow_schema, vec![4]);
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("data/old.parquet"),
                object_store::PutPayload::from(Bytes::from(old_bytes.clone())),
            )
            .await
            .unwrap();
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("data/survivor.parquet"),
                object_store::PutPayload::from(Bytes::from(survivor_bytes.clone())),
            )
            .await
            .unwrap();
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("data/replacement.parquet"),
                object_store::PutPayload::from(Bytes::from(replacement_bytes.clone())),
            )
            .await
            .unwrap();
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("data/unaffected.parquet"),
                object_store::PutPayload::from(Bytes::from(unaffected_bytes.clone())),
            )
            .await
            .unwrap();

        let old_file = data_file("data/old.parquet", 1, old_bytes.len() as u64);
        let survivor_file = data_file("data/survivor.parquet", 1, survivor_bytes.len() as u64);
        let replacement_file = data_file(
            "data/replacement.parquet",
            1,
            replacement_bytes.len() as u64,
        );
        let unaffected_file =
            data_file("data/unaffected.parquet", 1, unaffected_bytes.len() as u64);

        let parent_snapshot_id = 10;
        let parent_sequence_number = 1;
        let mut parent_writer =
            ManifestWriterBuilder::new(Some(parent_snapshot_id), None, manifest_metadata.clone())
                .build();
        parent_writer.add(old_file.clone());
        parent_writer.add(survivor_file.clone());
        let parent_manifest_bytes = parent_writer.to_avro_bytes_v2().unwrap();
        let mut parent_manifest_file = parent_writer.into_manifest_file(
            "metadata/manifest-parent.avro".to_string(),
            parent_sequence_number,
            parent_snapshot_id,
        );
        parent_manifest_file.manifest_length = parent_manifest_bytes.len() as i64;
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("metadata/manifest-parent.avro"),
                object_store::PutPayload::from(Bytes::from(parent_manifest_bytes)),
            )
            .await
            .unwrap();

        let mut unaffected_writer =
            ManifestWriterBuilder::new(Some(parent_snapshot_id), None, manifest_metadata.clone())
                .build();
        unaffected_writer.add(unaffected_file.clone());
        let unaffected_manifest_bytes = unaffected_writer.to_avro_bytes_v2().unwrap();
        let mut unaffected_manifest_file = unaffected_writer.into_manifest_file(
            "metadata/manifest-unaffected.avro".to_string(),
            parent_sequence_number,
            parent_snapshot_id,
        );
        unaffected_manifest_file.manifest_length = unaffected_manifest_bytes.len() as i64;
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("metadata/manifest-unaffected.avro"),
                object_store::PutPayload::from(Bytes::from(unaffected_manifest_bytes)),
            )
            .await
            .unwrap();

        let delete_manifest_metadata = crate::spec::manifest::ManifestMetadata::new(
            Arc::new(schema.clone()),
            schema.schema_id(),
            partition_spec.clone(),
            FormatVersion::V2,
            ManifestContentType::Deletes,
        );
        let delete_writer =
            ManifestWriterBuilder::new(Some(parent_snapshot_id), None, delete_manifest_metadata)
                .build();
        let delete_manifest_bytes = delete_writer.to_avro_bytes_v2().unwrap();
        let mut delete_manifest_file = delete_writer.into_manifest_file(
            "metadata/manifest-deletes.avro".to_string(),
            parent_sequence_number,
            parent_snapshot_id,
        );
        delete_manifest_file.manifest_length = delete_manifest_bytes.len() as i64;
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("metadata/manifest-deletes.avro"),
                object_store::PutPayload::from(Bytes::from(delete_manifest_bytes)),
            )
            .await
            .unwrap();

        let mut parent_list_writer = ManifestListWriter::new();
        parent_list_writer.append(parent_manifest_file);
        parent_list_writer.append(unaffected_manifest_file);
        parent_list_writer.append(delete_manifest_file);
        let parent_list_bytes = parent_list_writer.to_bytes(FormatVersion::V2).unwrap();
        store_ctx
            .prefixed
            .put(
                &object_store::path::Path::from("metadata/snap-parent.avro"),
                object_store::PutPayload::from(Bytes::from(parent_list_bytes)),
            )
            .await
            .unwrap();

        let parent_snapshot = SnapshotBuilder::new()
            .with_snapshot_id(parent_snapshot_id)
            .with_sequence_number(parent_sequence_number)
            .with_manifest_list("metadata/snap-parent.avro")
            .with_summary(
                crate::spec::snapshots::Summary::new(Operation::Append)
                    .with_property("total-data-files", 3)
                    .with_property("total-records", 3),
            )
            .with_schema_id(schema.schema_id())
            .build()
            .unwrap();
        let tx = Transaction::new(table_url.to_string(), parent_snapshot);

        let action_commit = SnapshotProducer::new(
            &tx,
            vec![replacement_file.clone()],
            Some(store_ctx.clone()),
            Some(manifest_metadata),
        )
        .with_write_path_mode(WritePathMode::Relative)
        .commit(RewriteFilesOperation::new(vec![old_file.file_path.clone()]))
        .await
        .unwrap();

        let new_snapshot = action_commit
            .updates()
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(new_snapshot.parent_snapshot_id(), Some(parent_snapshot_id));
        assert_eq!(new_snapshot.summary.operation, Operation::Overwrite);
        assert_eq!(
            new_snapshot
                .summary
                .additional_properties
                .get("added-data-files"),
            Some(&"1".to_string())
        );
        assert_eq!(
            new_snapshot
                .summary
                .additional_properties
                .get("deleted-data-files"),
            Some(&"1".to_string())
        );
        assert_eq!(
            new_snapshot
                .summary
                .additional_properties
                .get("total-data-files"),
            Some(&"3".to_string())
        );
        assert_eq!(
            new_snapshot
                .summary
                .additional_properties
                .get("total-records"),
            Some(&"3".to_string())
        );

        let manifest_list = load_manifest_list(&store_ctx, new_snapshot.manifest_list())
            .await
            .unwrap();
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|manifest| { manifest.manifest_path == "metadata/manifest-unaffected.avro" })
        );
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|manifest| { manifest.manifest_path == "metadata/manifest-deletes.avro" })
        );
        let mut entries_by_path = HashMap::<String, ManifestEntry>::new();
        for manifest_file in manifest_list.entries() {
            let manifest = load_manifest(&store_ctx, &manifest_file.manifest_path)
                .await
                .unwrap();
            for entry in manifest.entries() {
                entries_by_path.insert(entry.data_file.file_path.clone(), (**entry).clone());
            }
        }
        assert_eq!(
            entries_by_path["data/old.parquet"].status,
            ManifestStatus::Deleted
        );
        assert_eq!(
            entries_by_path["data/survivor.parquet"].status,
            ManifestStatus::Existing
        );
        assert_eq!(
            entries_by_path["data/replacement.parquet"].status,
            ManifestStatus::Added
        );
        assert_eq!(
            entries_by_path["data/replacement.parquet"].snapshot_id,
            None
        );
        assert_eq!(
            entries_by_path["data/unaffected.parquet"].status,
            ManifestStatus::Added
        );
        assert_eq!(
            entries_by_path["data/old.parquet"].snapshot_id,
            Some(new_snapshot.snapshot_id())
        );
        assert_eq!(
            entries_by_path["data/old.parquet"].sequence_number,
            Some(parent_sequence_number)
        );
        assert_eq!(
            entries_by_path["data/old.parquet"].file_sequence_number,
            Some(parent_sequence_number)
        );
        assert_eq!(
            entries_by_path["data/survivor.parquet"].snapshot_id,
            Some(parent_snapshot_id)
        );
        assert_eq!(
            entries_by_path["data/survivor.parquet"].sequence_number,
            Some(parent_sequence_number)
        );
        assert_eq!(
            entries_by_path["data/survivor.parquet"].file_sequence_number,
            Some(parent_sequence_number)
        );

        let context = SessionContext::new();
        context
            .runtime_env()
            .register_object_store(&Url::parse("memory://rewrite-test/").unwrap(), object_store);
        let provider =
            IcebergTableProvider::new(table_url, schema, new_snapshot, vec![partition_spec], 0)
                .unwrap();
        let plan = provider
            .scan(&context.state(), None, &[], None)
            .await
            .unwrap();
        let batches = datafusion::physical_plan::collect(plan, context.task_ctx())
            .await
            .unwrap();
        let mut ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn rewrite_files_rejects_paths_not_live_without_writing_orphan_manifests() {
        let fixture = single_file_parent("memory://rewrite-missing-test/table/").await;

        let result = SnapshotProducer::new(
            &fixture.tx,
            vec![],
            Some(fixture.store_ctx.clone()),
            Some(fixture.manifest_metadata),
        )
        .with_write_path_mode(WritePathMode::Relative)
        .commit(RewriteFilesOperation::new(vec![
            fixture.live_file.file_path,
            "data/missing.parquet".to_string(),
        ]))
        .await;

        assert!(matches!(
            result,
            Err(message) if message.contains("not live in the parent snapshot")
        ));
        let metadata_objects = fixture
            .store_ctx
            .prefixed
            .list(Some(&object_store::path::Path::from("metadata")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(metadata_objects.len(), 2);
    }

    #[tokio::test]
    async fn rewrite_files_supports_pure_deletes_without_an_empty_added_manifest() {
        let fixture = single_file_parent("memory://rewrite-pure-delete-test/table/").await;

        let action_commit = SnapshotProducer::new(
            &fixture.tx,
            vec![],
            Some(fixture.store_ctx.clone()),
            Some(fixture.manifest_metadata),
        )
        .with_write_path_mode(WritePathMode::Relative)
        .commit(RewriteFilesOperation::new(vec![
            fixture.live_file.file_path,
        ]))
        .await
        .unwrap();

        let new_snapshot = action_commit
            .updates()
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        for (key, expected) in [
            ("added-data-files", "0"),
            ("deleted-data-files", "1"),
            ("added-records", "0"),
            ("deleted-records", "1"),
            ("total-data-files", "0"),
            ("total-records", "0"),
        ] {
            assert_eq!(
                new_snapshot
                    .summary
                    .additional_properties
                    .get(key)
                    .map(String::as_str),
                Some(expected)
            );
        }

        let manifest_list = load_manifest_list(&fixture.store_ctx, new_snapshot.manifest_list())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 1);
        let manifest = load_manifest(
            &fixture.store_ctx,
            &manifest_list.entries()[0].manifest_path,
        )
        .await
        .unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(manifest.entries()[0].status, ManifestStatus::Deleted);
    }
}
