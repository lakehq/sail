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

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use super::{ActionCommit, SnapshotProducer, SnapshotUpdateKind, Transaction, TransactionAction};
use crate::io::StoreContext;
use crate::spec::manifest::ManifestMetadata;
use crate::spec::manifest_list::ManifestList;
use crate::spec::{DataFile, PartitionSpec};

pub struct FastAppendAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: BTreeMap<String, String>,
    added_data_files: Vec<DataFile>,
    parent_manifest_list: Option<ManifestList>,
    store_ctx: Option<StoreContext>,
    manifest_metadata: Option<ManifestMetadata>,
    partition_specs: Vec<PartitionSpec>,
    row_lineage_start_row_id: Option<i64>,
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
            snapshot_properties: BTreeMap::new(),
            added_data_files: Vec::new(),
            parent_manifest_list: None,
            store_ctx: None,
            manifest_metadata: None,
            partition_specs: Vec::new(),
            row_lineage_start_row_id: None,
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

    pub fn set_snapshot_properties(
        mut self,
        snapshot_properties: BTreeMap<String, String>,
    ) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    pub fn with_parent_manifest_list(mut self, list: Option<ManifestList>) -> Self {
        self.parent_manifest_list = list;
        self
    }

    pub fn with_store_context(mut self, store_ctx: StoreContext) -> Self {
        self.store_ctx = Some(store_ctx);
        self
    }

    pub fn with_manifest_metadata(mut self, metadata: ManifestMetadata) -> Self {
        self.manifest_metadata = Some(metadata);
        self
    }

    pub fn with_partition_specs(mut self, partition_specs: Vec<PartitionSpec>) -> Self {
        self.partition_specs = partition_specs;
        self
    }

    pub fn with_row_lineage_start_row_id(mut self, start_row_id: Option<i64>) -> Self {
        self.row_lineage_start_row_id = start_row_id;
        self
    }
}

#[async_trait]
impl TransactionAction for FastAppendAction {
    async fn commit(self: Arc<Self>, tx: &Transaction) -> Result<ActionCommit, String> {
        let snapshot_producer = SnapshotProducer::new(
            tx,
            self.added_data_files.clone(),
            self.store_ctx.clone(),
            self.manifest_metadata.clone(),
        )
        .with_snapshot_properties(self.snapshot_properties.clone())
        .with_partition_specs(self.partition_specs.clone())
        .with_row_lineage_start_row_id(self.row_lineage_start_row_id);
        snapshot_producer.validate_added_data_files(&self.added_data_files)?;

        if self.check_duplicate {
            // TODO: validate duplicate files later
        }

        snapshot_producer
            .commit(SnapshotUpdateKind::FastAppend)
            .await
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::spec::snapshots::summary::RESERVED_SNAPSHOT_PROPERTIES;
    use crate::spec::{Operation, SnapshotBuilder, Summary, TableUpdate};

    #[test]
    fn fast_append_routes_annotations_and_rejects_reserved_keys_before_io() {
        futures::executor::block_on(async {
            let snapshot = SnapshotBuilder::new()
                .with_snapshot_id(1)
                .with_manifest_list("")
                .with_summary(Summary::new(Operation::Append))
                .build()
                .expect("parent snapshot");
            let url = url::Url::parse("file:///snapshot-properties/").expect("table URL");
            let tx = Transaction::new(url.to_string(), snapshot, 0);
            for key in RESERVED_SNAPSHOT_PROPERTIES.into_iter().chain([""]) {
                let result = Arc::new(
                    FastAppendAction::new()
                        .set_snapshot_properties(BTreeMap::from([(key.into(), "override".into())])),
                )
                .commit(&tx)
                .await;
                let error = result
                    .err()
                    .expect("invalid annotation must fail without a store");
                assert!(error.contains(if key.is_empty() {
                    "cannot be empty"
                } else {
                    "is reserved"
                }));
            }
            let store = Arc::new(object_store::memory::InMemory::new());
            let store_ctx = StoreContext::new(store, &url).expect("store context");
            let action = Arc::new(
                FastAppendAction::new()
                    .set_snapshot_properties(BTreeMap::from([
                        ("run-id".into(), "first".into()),
                        ("empty".into(), "".into()),
                    ]))
                    .with_store_context(store_ctx),
            );
            let commit = action.commit(&tx).await.expect("annotated append");
            let snapshot = commit
                .updates()
                .iter()
                .find_map(|update| match update {
                    TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                    _ => None,
                })
                .expect("new snapshot");
            assert_eq!(snapshot.summary.operation, Operation::Append);
            let properties = &snapshot.summary.additional_properties;
            assert_eq!(properties.get("run-id").map(String::as_str), Some("first"));
            assert_eq!(properties.get("empty").map(String::as_str), Some(""));
            assert_eq!(
                properties.get("total-records").map(String::as_str),
                Some("0")
            );
            assert!(tx.snapshot().summary.additional_properties.is_empty());
        });
    }
}
