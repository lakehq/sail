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

use super::SnapshotProduceOperation;

/// A snapshot operation that replaces a specific set of live data files.
#[derive(Debug, Clone)]
pub struct RewriteFilesOperation {
    deleted_data_file_paths: Vec<String>,
}

impl RewriteFilesOperation {
    pub fn new(deleted_data_file_paths: impl IntoIterator<Item = String>) -> Self {
        Self {
            deleted_data_file_paths: deleted_data_file_paths.into_iter().collect(),
        }
    }

    pub fn deleted_data_file_paths(&self) -> &[String] {
        &self.deleted_data_file_paths
    }
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> &'static str {
        "overwrite"
    }

    fn deleted_data_file_paths_for_rewrite(&self) -> Option<&[String]> {
        Some(&self.deleted_data_file_paths)
    }
}

/// A snapshot operation that removes and optionally replaces specific live data files as a
/// logical row-level delete.
#[derive(Debug, Clone)]
pub struct DeleteFilesOperation {
    deleted_data_file_paths: Vec<String>,
}

impl DeleteFilesOperation {
    pub fn new(deleted_data_file_paths: impl IntoIterator<Item = String>) -> Self {
        Self {
            deleted_data_file_paths: deleted_data_file_paths.into_iter().collect(),
        }
    }

    pub fn deleted_data_file_paths(&self) -> &[String] {
        &self.deleted_data_file_paths
    }
}

impl SnapshotProduceOperation for DeleteFilesOperation {
    fn operation(&self) -> &'static str {
        "delete"
    }

    fn deleted_data_file_paths_for_rewrite(&self) -> Option<&[String]> {
        Some(&self.deleted_data_file_paths)
    }
}
