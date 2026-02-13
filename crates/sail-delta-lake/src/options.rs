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

use serde::{Deserialize, Serialize};

/// Options that control the behavior of Delta Lake tables.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDeltaOptions {
    pub replace_where: Option<String>,
    pub merge_schema: bool,
    pub overwrite_schema: bool,
    pub target_file_size: u64,
    pub write_batch_size: usize,

    pub version_as_of: Option<i64>,
    pub timestamp_as_of: Option<String>,

    /// Enable metadata-as-data read path (avoid loading file list on driver; use log replay + discovery).
    pub metadata_as_data_read: bool,

    /// Strategy for Delta log replay in metadata-as-data path.
    pub delta_log_replay_strategy: DeltaLogReplayStrategyOption,

    /// Max commit JSON file count to use hash-no-sort replay when strategy is `Auto`.
    #[serde(default = "default_delta_log_replay_hash_threshold")]
    pub delta_log_replay_hash_threshold: usize,

    /// Column mapping mode for new tables (dataframe API only)
    #[serde(default)]
    pub column_mapping_mode: ColumnMappingModeOption,
}

impl Default for TableDeltaOptions {
    fn default() -> Self {
        Self {
            replace_where: None,
            merge_schema: false,
            overwrite_schema: false,
            target_file_size: 0,
            write_batch_size: 0,
            version_as_of: None,
            timestamp_as_of: None,
            metadata_as_data_read: false,
            delta_log_replay_strategy: DeltaLogReplayStrategyOption::Auto,
            delta_log_replay_hash_threshold: default_delta_log_replay_hash_threshold(),
            column_mapping_mode: ColumnMappingModeOption::None,
        }
    }
}

fn default_delta_log_replay_hash_threshold() -> usize {
    100
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum DeltaLogReplayStrategyOption {
    #[default]
    Auto,
    Sort,
    HashNoSort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingModeOption {
    #[default]
    None,
    Name,
    Id,
}
