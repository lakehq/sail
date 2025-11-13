use serde::{Deserialize, Serialize};

/// Options that control the behavior of Iceberg tables.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TableIcebergOptions {
    pub use_ref: Option<String>,
    pub snapshot_id: Option<i64>,
    pub timestamp_as_of: Option<String>,

    pub merge_schema: bool,
    pub overwrite_schema: bool,
}
