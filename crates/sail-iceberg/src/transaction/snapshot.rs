use super::{ActionCommit, Transaction};
use crate::io::IcebergObjectStore;
use crate::spec::manifest::ManifestWriterBuilder;
use crate::spec::manifest_list::{ManifestListWriter, UNASSIGNED_SEQUENCE_NUMBER};
use crate::spec::{
    DataFile, Operation, SnapshotBuilder, SnapshotReference, SnapshotRetention, TableRequirement,
    TableUpdate, MAIN_BRANCH,
};
use crate::spec::{FormatVersion, ManifestContentType, PartitionSpec, Schema};
use bytes::Bytes;

pub trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> &'static str;
}

pub struct SnapshotProducer<'a> {
    pub tx: &'a Transaction,
    pub added_data_files: Vec<DataFile>,
    pub store: Option<IcebergObjectStore>,
    pub manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
}

impl<'a> SnapshotProducer<'a> {
    pub fn new(
        tx: &'a Transaction,
        added_data_files: Vec<DataFile>,
        store: Option<IcebergObjectStore>,
        manifest_metadata: Option<crate::spec::manifest::ManifestMetadata>,
    ) -> Self {
        Self {
            tx,
            added_data_files,
            store,
            manifest_metadata,
        }
    }

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        Ok(())
    }

    pub async fn commit(self, _op: impl SnapshotProduceOperation) -> Result<ActionCommit, String> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let summary = crate::spec::snapshots::Summary::new(Operation::Append);

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
        let manifest_len = manifest_bytes.len() as i64;

        // Write manifest to metadata path
        let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| "object store not available".to_string())?;
        let _manifest_path = store
            .put_rel(&manifest_rel, Bytes::from(manifest_bytes))
            .await?;

        // Build a manifest file entry for manifest list
        let added_rows: i64 = self
            .added_data_files
            .iter()
            .map(|df| df.record_count as i64)
            .sum();
        let manifest_file = crate::spec::manifest_list::ManifestFile::builder()
            .with_manifest_path(manifest_rel.clone())
            .with_manifest_length(manifest_len)
            .with_partition_spec_id(metadata.partition_spec.spec_id())
            .with_content(ManifestContentType::Data)
            .with_sequence_number(UNASSIGNED_SEQUENCE_NUMBER)
            .with_min_sequence_number(UNASSIGNED_SEQUENCE_NUMBER)
            .with_added_snapshot_id(self.tx.snapshot().snapshot_id())
            .with_file_counts(self.added_data_files.len() as i32, 0, 0)
            .with_row_counts(added_rows, 0, 0)
            .build()?;

        let mut list_writer = ManifestListWriter::new();
        list_writer.append(manifest_file);
        let list_bytes = list_writer.to_bytes(FormatVersion::V2)?;
        let list_rel = format!("metadata/snap-{}.avro", self.tx.snapshot().snapshot_id());
        let _list_path = store.put_rel(&list_rel, Bytes::from(list_bytes)).await?;

        let new_snapshot = SnapshotBuilder::new()
            .with_snapshot_id(self.tx.snapshot().snapshot_id())
            .with_parent_snapshot_id(self.tx.snapshot().parent_snapshot_id().unwrap_or_default())
            .with_sequence_number(self.tx.snapshot().sequence_number())
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(list_rel)
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
