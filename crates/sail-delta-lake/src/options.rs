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

use crate::spec::ColumnMappingMode;

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

impl ColumnMappingModeOption {
    pub fn parse(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "name" => Self::Name,
            "id" => Self::Id,
            _ => Self::None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Name => "name",
            Self::Id => "id",
        }
    }

    pub const fn to_kernel(self) -> ColumnMappingMode {
        match self {
            Self::Name => ColumnMappingMode::Name,
            Self::Id => ColumnMappingMode::Id,
            Self::None => ColumnMappingMode::None,
        }
    }

    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::Name | Self::Id)
    }
}

impl From<ColumnMappingMode> for ColumnMappingModeOption {
    fn from(value: ColumnMappingMode) -> Self {
        match value {
            ColumnMappingMode::Name => Self::Name,
            ColumnMappingMode::Id => Self::Id,
            ColumnMappingMode::None => Self::None,
        }
    }
}
