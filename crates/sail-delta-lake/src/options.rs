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
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TableDeltaOptions {
    pub replace_where: Option<String>,
    pub merge_schema: bool,
    pub overwrite_schema: bool,
    pub target_file_size: u64,
    pub write_batch_size: usize,

    pub version_as_of: Option<i64>,
    pub timestamp_as_of: Option<String>,

    /// Column mapping mode for new tables (dataframe API only)
    #[serde(default)]
    pub column_mapping_mode: ColumnMappingModeOption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingModeOption {
    #[default]
    None,
    Name,
    Id,
}
