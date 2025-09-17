use serde::{Deserialize, Serialize};

/// Options that control the behavior of Delta Lake tables.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TableDeltaOptions {
    pub replace_where: Option<String>,
    pub merge_schema: bool,
    pub overwrite_schema: bool,
    pub target_file_size: u64,
    pub write_batch_size: usize,

    pub version_as_of: Option<i64>,
    pub timestamp_as_of: Option<String>,
}
