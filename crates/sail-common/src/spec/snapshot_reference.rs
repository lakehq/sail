use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SnapshotReferenceType {
    Branch,
    Tag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SnapshotReferenceCreateMode {
    Create,
    CreateIfNotExists,
    Replace,
    CreateOrReplace,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SnapshotReferenceOperation {
    Create {
        name: String,
        kind: SnapshotReferenceType,
        mode: SnapshotReferenceCreateMode,
        snapshot_id: Option<i64>,
        max_ref_age_ms: Option<i64>,
        min_snapshots_to_keep: Option<i32>,
        max_snapshot_age_ms: Option<i64>,
    },
    Drop {
        name: String,
        kind: SnapshotReferenceType,
        if_exists: bool,
    },
}
