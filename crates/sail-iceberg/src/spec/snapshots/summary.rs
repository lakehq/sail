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

#[derive(Debug, Clone)]
pub struct SnapshotSummary;

pub(crate) const OPERATION: &str = "operation";
pub(crate) const REPLACE_PARTITIONS: &str = "replace-partitions";
pub(crate) const ADDED_DATA_FILES: &str = "added-data-files";
pub(crate) const DELETED_DATA_FILES: &str = "deleted-data-files";
pub(crate) const TOTAL_DATA_FILES: &str = "total-data-files";
pub(crate) const ADDED_RECORDS: &str = "added-records";
pub(crate) const DELETED_RECORDS: &str = "deleted-records";
pub(crate) const TOTAL_RECORDS: &str = "total-records";
pub(crate) const ADDED_DELETE_FILES: &str = "added-delete-files";
pub(crate) const REMOVED_DELETE_FILES: &str = "removed-delete-files";
pub(crate) const TOTAL_DELETE_FILES: &str = "total-delete-files";
pub(crate) const ADDED_POSITION_DELETE_FILES: &str = "added-position-delete-files";
pub(crate) const REMOVED_POSITION_DELETE_FILES: &str = "removed-position-delete-files";
pub(crate) const ADDED_POSITION_DELETES: &str = "added-position-deletes";
pub(crate) const REMOVED_POSITION_DELETES: &str = "removed-position-deletes";
pub(crate) const TOTAL_POSITION_DELETES: &str = "total-position-deletes";
pub(crate) const ADDED_EQUALITY_DELETE_FILES: &str = "added-equality-delete-files";
pub(crate) const REMOVED_EQUALITY_DELETE_FILES: &str = "removed-equality-delete-files";
pub(crate) const ADDED_EQUALITY_DELETES: &str = "added-equality-deletes";
pub(crate) const REMOVED_EQUALITY_DELETES: &str = "removed-equality-deletes";
pub(crate) const TOTAL_EQUALITY_DELETES: &str = "total-equality-deletes";
pub(crate) const ADDED_DVS: &str = "added-dvs";
pub(crate) const REMOVED_DVS: &str = "removed-dvs";
pub(crate) const TOTAL_FILES_SIZE: &str = "total-files-size";

/// Summary fields maintained by Sail rather than by individual writes.
pub(crate) const RESERVED_SNAPSHOT_PROPERTIES: [&str; 24] = [
    OPERATION,
    REPLACE_PARTITIONS,
    ADDED_DATA_FILES,
    DELETED_DATA_FILES,
    TOTAL_DATA_FILES,
    ADDED_RECORDS,
    DELETED_RECORDS,
    TOTAL_RECORDS,
    ADDED_DELETE_FILES,
    REMOVED_DELETE_FILES,
    TOTAL_DELETE_FILES,
    ADDED_POSITION_DELETE_FILES,
    REMOVED_POSITION_DELETE_FILES,
    ADDED_POSITION_DELETES,
    REMOVED_POSITION_DELETES,
    TOTAL_POSITION_DELETES,
    ADDED_EQUALITY_DELETE_FILES,
    REMOVED_EQUALITY_DELETE_FILES,
    ADDED_EQUALITY_DELETES,
    REMOVED_EQUALITY_DELETES,
    TOTAL_EQUALITY_DELETES,
    ADDED_DVS,
    REMOVED_DVS,
    TOTAL_FILES_SIZE,
];

pub(crate) fn validate_snapshot_properties(
    properties: &BTreeMap<String, String>,
) -> Result<(), String> {
    for key in properties.keys() {
        if key.is_empty() {
            return Err("Iceberg snapshot property key cannot be empty".to_string());
        }
        if RESERVED_SNAPSHOT_PROPERTIES.contains(&key.as_str()) {
            return Err(format!("Iceberg snapshot property '{key}' is reserved"));
        }
    }
    Ok(())
}
