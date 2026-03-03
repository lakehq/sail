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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_features/mod.rs#L44-L119>
pub enum TableFeature {
    AppendOnly,
    Invariants,
    CheckConstraints,
    ChangeDataFeed,
    GeneratedColumns,
    IdentityColumns,
    ColumnMapping,
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L374-L466>
pub struct Protocol {
    min_reader_version: i32,
    min_writer_version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_features: Option<Vec<TableFeature>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<Vec<TableFeature>>,
}

impl Protocol {
    pub fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    pub fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    pub fn reader_features(&self) -> Option<&[TableFeature]> {
        self.reader_features.as_deref()
    }

    pub fn writer_features(&self) -> Option<&[TableFeature]> {
        self.writer_features.as_deref()
    }
}
