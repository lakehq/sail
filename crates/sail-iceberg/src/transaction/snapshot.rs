use super::{ActionCommit, Transaction};
use crate::spec::{
    DataFile, Operation, SnapshotBuilder, SnapshotReference, SnapshotRetention, TableRequirement,
    TableUpdate, MAIN_BRANCH,
};

pub trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> &'static str;
}

pub struct SnapshotProducer<'a> {
    pub tx: &'a Transaction,
    pub added_data_files: Vec<DataFile>,
}

impl<'a> SnapshotProducer<'a> {
    pub fn new(tx: &'a Transaction, added_data_files: Vec<DataFile>) -> Self {
        Self {
            tx,
            added_data_files,
        }
    }

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        Ok(())
    }

    pub async fn commit(self, _op: impl SnapshotProduceOperation) -> Result<ActionCommit, String> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let summary = crate::spec::snapshots::Summary::new(Operation::Append);
        let new_snapshot = SnapshotBuilder::new()
            .with_snapshot_id(self.tx.snapshot().snapshot_id())
            .with_parent_snapshot_id(self.tx.snapshot().parent_snapshot_id().unwrap_or_default())
            .with_sequence_number(self.tx.snapshot().sequence_number())
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(self.tx.snapshot().manifest_list())
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
