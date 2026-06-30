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

use bytes::Bytes;
use object_store::ObjectStoreExt;

use super::{ActionCommit, Transaction};
use crate::io::StoreContext;
use crate::spec::manifest::ManifestWriterBuilder;
use crate::spec::manifest_list::ManifestListWriter;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, Operation, PartitionSpec, Schema,
    SnapshotBuilder, SnapshotReference, SnapshotRetention, TableRequirement, TableUpdate,
    MAIN_BRANCH,
};
use crate::utils::join_table_uri;

pub trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> &'static str;
}

pub struct SnapshotProducer<'a> {
    pub tx: &'a Transaction,
    pub added_data_files: Vec<DataFile>,
    pub added_delete_files: Vec<DataFile>,
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
            added_delete_files: Vec::new(),
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

    pub fn with_added_delete_files(mut self, delete_files: Vec<DataFile>) -> Self {
        self.added_delete_files = delete_files;
        self
    }

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        // TODO: Implement this function to validate the added data files
        Ok(())
    }

    pub async fn commit(self, op: impl SnapshotProduceOperation) -> Result<ActionCommit, String> {
        let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
        let is_overwrite = op.operation() == Operation::Overwrite.as_str();
        let is_row_delta = !self.added_delete_files.is_empty();
        let operation = match op.operation() {
            "overwrite" => Operation::Overwrite,
            "delete" => Operation::Delete,
            _ => Operation::Append,
        };
        let mut summary = crate::spec::snapshots::Summary::new(operation.clone());
        if !self.added_data_files.is_empty() {
            summary = summary
                .with_property("added-data-files", self.added_data_files.len().to_string())
                .with_property(
                    "added-records",
                    self.added_data_files
                        .iter()
                        .map(|df| df.record_count)
                        .sum::<u64>()
                        .to_string(),
                );
        }
        if !self.added_delete_files.is_empty() {
            let added_position_deletes = self
                .added_delete_files
                .iter()
                .map(|df| df.record_count)
                .sum::<u64>();
            summary = summary
                .with_property(
                    "added-delete-files",
                    self.added_delete_files.len().to_string(),
                )
                .with_property(
                    "added-position-delete-files",
                    self.added_delete_files.len().to_string(),
                )
                .with_property("added-position-deletes", added_position_deletes.to_string())
                .with_property("deleted-records", added_position_deletes.to_string());
        }

        // Build manifest metadata: prefer caller-provided metadata derived from table schema/spec
        // Fall back to deriving from the current transaction snapshot if not provided
        let metadata = if let Some(meta) = self.manifest_metadata.clone() {
            meta
        } else {
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
            && (!is_overwrite || is_row_delta)
            && !parent_manifest_list_path_str.is_empty()
        {
            let (store_ref, manifest_list_path) = store_ctx
                .resolve(parent_manifest_list_path_str)
                .map_err(|e| format!("{}", e))?;

            log::trace!(
                "snapshot producer: loading parent manifest list: {}",
                &manifest_list_path
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

        let new_manifest_first_row_id = row_lineage_next_row_id;
        if self.row_lineage_start_row_id.is_some() {
            snapshot_added_rows += new_added_rows;
        }

        let mut new_manifest_files = Vec::new();
        let added_data_files = self.added_data_files.clone();
        if !added_data_files.is_empty() || self.added_delete_files.is_empty() {
            let mut writer = ManifestWriterBuilder::new(None, None, metadata.clone()).build();
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

            let mut manifest_file_builder = crate::spec::manifest_list::ManifestFile::builder()
                .with_manifest_path(join_table_uri(
                    self.tx.table_uri(),
                    &manifest_rel,
                    &self.write_path_mode,
                ))
                .with_manifest_length(manifest_len)
                .with_partition_spec_id(metadata.partition_spec.spec_id())
                .with_content(ManifestContentType::Data)
                .with_sequence_number(new_sequence_number)
                .with_min_sequence_number(new_sequence_number)
                .with_added_snapshot_id(new_snapshot_id)
                .with_file_counts(added_data_files.len() as i32, 0, 0)
                .with_row_counts(new_added_rows, 0, 0);
            if let Some(first_row_id) = new_manifest_first_row_id {
                manifest_file_builder = manifest_file_builder.with_first_row_id(first_row_id);
            }
            new_manifest_files.push(manifest_file_builder.build()?);
        }

        let added_delete_files = self.added_delete_files.clone();
        if !added_delete_files.is_empty() {
            let mut delete_metadata = metadata.clone();
            delete_metadata.content = ManifestContentType::Deletes;
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
