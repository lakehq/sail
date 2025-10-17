use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use super::{
    ActionCommit, SnapshotProduceOperation, SnapshotProducer, Transaction, TransactionAction,
};
use crate::io::IcebergObjectStore;
use crate::spec::manifest::ManifestMetadata;
use crate::spec::DataFile;

pub struct FastAppendAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    store: Option<IcebergObjectStore>,
    manifest_metadata: Option<ManifestMetadata>,
}

impl Default for FastAppendAction {
    fn default() -> Self {
        Self::new()
    }
}

impl FastAppendAction {
    pub fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            store: None,
            manifest_metadata: None,
        }
    }

    pub fn add_file(&mut self, file: DataFile) {
        self.added_data_files.push(file);
    }

    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    pub fn with_store(mut self, store: IcebergObjectStore) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_manifest_metadata(mut self, metadata: ManifestMetadata) -> Self {
        self.manifest_metadata = Some(metadata);
        self
    }
}

#[async_trait]
impl TransactionAction for FastAppendAction {
    async fn commit(self: Arc<Self>, tx: &Transaction) -> Result<ActionCommit, String> {
        let snapshot_producer = SnapshotProducer::new(
            tx,
            self.added_data_files.clone(),
            self.store.clone(),
            self.manifest_metadata.clone(),
        );
        snapshot_producer.validate_added_data_files(&self.added_data_files)?;

        if self.check_duplicate {
            // TODO: validate duplicate files later
        }

        struct FastAppendOperation;
        impl SnapshotProduceOperation for FastAppendOperation {
            fn operation(&self) -> &'static str {
                "append"
            }
        }

        snapshot_producer.commit(FastAppendOperation).await
    }
}
