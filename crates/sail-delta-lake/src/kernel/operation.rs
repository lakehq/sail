// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/protocol/mod.rs>
use std::collections::HashMap;

use delta_kernel::actions::Protocol;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::models::{CommitInfo, Metadata};
use crate::kernel::{DeltaResult, DeltaTableError};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MergePredicate {
    /// The type of merge operation performed
    pub action_type: String,
    /// The predicate used for the merge operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
}

/// The SaveMode used when performing a DeltaOperation.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum SaveMode {
    Append,
    Overwrite,
    ErrorIfExists,
    Ignore,
}

impl std::str::FromStr for SaveMode {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "append" => Ok(Self::Append),
            "overwrite" => Ok(Self::Overwrite),
            "error" | "error_if_exists" => Ok(Self::ErrorIfExists),
            "ignore" => Ok(Self::Ignore),
            _ => Err(DeltaTableError::generic(format!(
                "Invalid save mode provided: {s}, only these are supported: ['append', 'overwrite', 'error', 'ignore']"
            ))),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DeltaOperation {
    Create {
        mode: SaveMode,
        location: String,
        protocol: Protocol,
        metadata: Metadata,
    },
    Write {
        mode: SaveMode,
        #[serde(skip_serializing_if = "Option::is_none")]
        partition_by: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        predicate: Option<String>,
    },
    Delete {
        #[serde(skip_serializing_if = "Option::is_none")]
        predicate: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Merge {
        #[serde(skip_serializing_if = "Option::is_none")]
        predicate: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        merge_predicate: Option<String>,
        matched_predicates: Vec<MergePredicate>,
        not_matched_predicates: Vec<MergePredicate>,
        not_matched_by_source_predicates: Vec<MergePredicate>,
    },
    FileSystemCheck {},
    Restore {
        #[serde(skip_serializing_if = "Option::is_none")]
        version: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        datetime: Option<i64>,
    },
}

impl DeltaOperation {
    pub fn name(&self) -> &str {
        match self {
            Self::Create {
                mode: SaveMode::Overwrite,
                ..
            } => "CREATE OR REPLACE TABLE",
            Self::Create { .. } => "CREATE TABLE",
            Self::Write { .. } => "WRITE",
            Self::Delete { .. } => "DELETE",
            Self::Merge { .. } => "MERGE",
            Self::FileSystemCheck { .. } => "FSCK",
            Self::Restore { .. } => "RESTORE",
        }
    }

    pub fn operation_parameters(&self) -> DeltaResult<HashMap<String, Value>> {
        if let Some(Some(Some(map))) = serde_json::to_value(self)?
            .as_object()
            .map(|p| p.values().next().map(|q| q.as_object()))
        {
            Ok(map
                .iter()
                .filter(|item| !item.1.is_null())
                .map(|(k, v)| {
                    // Delta commitInfo.operationParameters expects values to be strings.
                    (
                        k.to_owned(),
                        Value::String(if v.is_string() {
                            String::from(v.as_str().unwrap_or_default())
                        } else {
                            v.to_string()
                        }),
                    )
                })
                .collect())
        } else {
            Err(DeltaTableError::generic(
                "Operation parameters serialized into unexpected shape",
            ))
        }
    }

    pub fn changes_data(&self) -> bool {
        !matches!(self, Self::FileSystemCheck {})
    }

    pub fn get_commit_info(&self) -> CommitInfo {
        CommitInfo {
            operation: Some(self.name().into()),
            operation_parameters: self.operation_parameters().ok(),
            // FIXME: use a proper engine name
            engine_info: Some(format!("sail-delta-lake:{}", env!("CARGO_PKG_VERSION"))),
            ..Default::default()
        }
    }

    /// Convert this operation into the JSON shape stored in Delta log `commitInfo`.
    ///
    /// Note: this does **not** add volatile fields like `timestamp` / `readVersion`; those are
    /// filled in by the transaction layer at commit time.
    pub fn to_commit_info_json(&self) -> DeltaResult<Value> {
        serde_json::to_value(self.get_commit_info())
            .map_err(|e| DeltaTableError::generic(format!("failed to serialize commit info: {e}")))
    }

    pub fn read_predicate(&self) -> Option<String> {
        match self {
            Self::Write { predicate, .. } => predicate.clone(),
            Self::Delete { predicate, .. } => predicate.clone(),
            Self::Merge { predicate, .. } => predicate.clone(),
            _ => None,
        }
    }

    pub fn read_whole_table(&self) -> bool {
        match self {
            // Predicate is none -> Merge operation had to join full source and target
            Self::Merge { predicate, .. } if predicate.is_none() => true,
            _ => false,
        }
    }
}
