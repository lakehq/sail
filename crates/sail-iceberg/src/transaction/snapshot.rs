use bytes::Bytes;

use super::{ActionCommit, Transaction};
use crate::io::StoreContext;
use crate::spec::manifest::ManifestWriterBuilder;
use crate::spec::manifest_list::{ManifestListWriter, UNASSIGNED_SEQUENCE_NUMBER};
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
    pub store_ctx: Option<StoreContext>,
    pub manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
    pub write_path_mode: crate::utils::WritePathMode,
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
        }
    }

    pub fn with_write_path_mode(mut self, mode: crate::utils::WritePathMode) -> Self {
        self.write_path_mode = mode;
        self
    }

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        Ok(())
    }

    pub async fn commit(self, op: impl SnapshotProduceOperation) -> Result<ActionCommit, String> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let is_overwrite = op.operation() == Operation::Overwrite.as_str();
        let summary = if is_overwrite {
            crate::spec::snapshots::Summary::new(Operation::Overwrite)
        } else {
            crate::spec::snapshots::Summary::new(Operation::Append)
        };

        // Build a simple manifest in-memory using v2 minimal schema
        // For now, derive schema/spec from current snapshot if available; otherwise default empty
        let schema_id = self.tx.snapshot().schema_id().unwrap_or_default();
        let schema = Schema::builder()
            .with_schema_id(schema_id)
            .with_fields(vec![])
            .build()
            .map_err(|e| format!("schema build error: {e}"))?;
        let partition_spec = PartitionSpec::builder().with_spec_id(0).build();
        let metadata = crate::spec::manifest::ManifestMetadata::new(
            std::sync::Arc::new(schema.clone()),
            schema_id,
            partition_spec,
            FormatVersion::V2,
            ManifestContentType::Data,
        );
        let mut writer = ManifestWriterBuilder::new(None, None, metadata.clone()).build();
        for df in &self.added_data_files {
            writer.add(df.clone());
        }
        let manifest = writer.finish();
        let manifest_bytes = manifest.to_avro_bytes_v2()?;

        // Generate new snapshot ID and increment sequence number
        let new_snapshot_id = timestamp_ms;
        let new_sequence_number = self.tx.snapshot().sequence_number() + 1;

        let store_ctx = self
            .store_ctx
            .as_ref()
            .ok_or_else(|| "store context not available".to_string())?;

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

        // Build a manifest file entry for manifest list
        let added_rows: i64 = self
            .added_data_files
            .iter()
            .map(|df| df.record_count as i64)
            .sum();
        let manifest_file = crate::spec::manifest_list::ManifestFile::builder()
            .with_manifest_path(join_table_uri(
                self.tx.table_uri(),
                &manifest_rel,
                &self.write_path_mode,
            ))
            .with_manifest_length(manifest_len)
            .with_partition_spec_id(metadata.partition_spec.spec_id())
            .with_content(ManifestContentType::Data)
            .with_sequence_number(UNASSIGNED_SEQUENCE_NUMBER)
            .with_min_sequence_number(UNASSIGNED_SEQUENCE_NUMBER)
            .with_added_snapshot_id(new_snapshot_id)
            .with_file_counts(self.added_data_files.len() as i32, 0, 0)
            .with_row_counts(added_rows, 0, 0)
            .build()?;

        let mut list_writer = ManifestListWriter::new();
        let mut total_manifest_count = 0;

        // Load the parent manifest list and append its entries for append only.
        let parent_snapshot = self.tx.snapshot();
        let parent_manifest_list_path_str = parent_snapshot.manifest_list();

        if !is_overwrite && !parent_manifest_list_path_str.is_empty() {
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
            let parent_manifest_list = crate::spec::ManifestList::parse_with_version(
                &manifest_list_data,
                FormatVersion::V2,
            )?;
            log::trace!(
                "snapshot producer: found parent manifest files: {}",
                parent_manifest_list.entries().len()
            );
            for entry in parent_manifest_list.entries() {
                list_writer.append(entry.clone());
                total_manifest_count += 1;
            }
        }

        log::trace!(
            "Creating new snapshot: id={} seq={} parent_id={}",
            new_snapshot_id,
            new_sequence_number,
            self.tx.snapshot().snapshot_id()
        );

        list_writer.append(manifest_file);
        total_manifest_count += 1;
        log::trace!(
            "snapshot producer: new manifest list will have files: {}",
            total_manifest_count
        );
        let list_bytes = list_writer.to_bytes(FormatVersion::V2)?;
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

        let new_snapshot = SnapshotBuilder::new()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(self.tx.snapshot().snapshot_id())
            .with_sequence_number(new_sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(manifest_list_uri)
            .with_summary(summary)
            .with_schema_id(self.tx.snapshot().schema_id().unwrap_or_default())
            .build()?;

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

        let requirements = vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: Some(self.tx.snapshot().snapshot_id()),
        }];

        Ok(ActionCommit::new(updates, requirements))
    }
}
