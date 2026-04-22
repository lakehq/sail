// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright 2025-2026 LakeSail, Inc.
// Modified in 2026 by LakeSail, Inc.
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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::spec::actions::CommitInfo;
use crate::spec::{DeltaError as DeltaTableError, DeltaResult, Metadata, Protocol};

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/protocol/mod.rs#L211-L518>
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DeltaOperation {
    Create {
        mode: SaveMode,
        location: String,
        protocol: Box<Protocol>,
        metadata: Box<Metadata>,
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
    #[serde(rename_all = "camelCase")]
    SetTableProperties {
        properties: HashMap<String, String>,
    },
    #[serde(rename_all = "camelCase")]
    UnsetTableProperties {
        properties: Vec<String>,
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
            Self::SetTableProperties { .. } => "SET TBLPROPERTIES",
            Self::UnsetTableProperties { .. } => "UNSET TBLPROPERTIES",
        }
    }

    pub fn operation_parameters_string_map(&self) -> DeltaResult<HashMap<String, String>> {
        fn insert_json<T: Serialize>(
            map: &mut HashMap<String, String>,
            key: &str,
            value: &T,
        ) -> DeltaResult<()> {
            map.insert(
                key.to_string(),
                serde_json::to_string(value).map_err(DeltaTableError::generic_err)?,
            );
            Ok(())
        }

        fn insert_opt<T: ToString>(map: &mut HashMap<String, String>, key: &str, value: Option<T>) {
            if let Some(value) = value {
                map.insert(key.to_string(), value.to_string());
            }
        }

        let mut parameters = HashMap::new();
        match self {
            Self::Create {
                mode,
                location,
                protocol,
                metadata,
            } => {
                parameters.insert("mode".to_string(), format!("{mode:?}"));
                parameters.insert("location".to_string(), location.clone());
                insert_json(&mut parameters, "protocol", protocol.as_ref())?;
                insert_json(&mut parameters, "metadata", metadata.as_ref())?;
            }
            Self::Write {
                mode,
                partition_by,
                predicate,
            } => {
                parameters.insert("mode".to_string(), format!("{mode:?}"));
                if let Some(partition_by) = partition_by {
                    insert_json(&mut parameters, "partitionBy", partition_by)?;
                }
                insert_opt(&mut parameters, "predicate", predicate.clone());
            }
            Self::Delete { predicate } => {
                insert_opt(&mut parameters, "predicate", predicate.clone());
            }
            Self::Merge {
                predicate,
                merge_predicate,
                matched_predicates,
                not_matched_predicates,
                not_matched_by_source_predicates,
            } => {
                insert_opt(&mut parameters, "predicate", predicate.clone());
                insert_opt(&mut parameters, "mergePredicate", merge_predicate.clone());
                insert_json(&mut parameters, "matchedPredicates", matched_predicates)?;
                insert_json(
                    &mut parameters,
                    "notMatchedPredicates",
                    not_matched_predicates,
                )?;
                insert_json(
                    &mut parameters,
                    "notMatchedBySourcePredicates",
                    not_matched_by_source_predicates,
                )?;
            }
            Self::FileSystemCheck {} => {}
            Self::Restore { version, datetime } => {
                insert_opt(&mut parameters, "version", *version);
                insert_opt(&mut parameters, "datetime", *datetime);
            }
            Self::SetTableProperties { properties } => {
                insert_json(&mut parameters, "properties", properties)?;
            }
            Self::UnsetTableProperties { properties } => {
                insert_json(&mut parameters, "properties", properties)?;
            }
        }

        Ok(parameters)
    }

    pub fn operation_parameters(&self) -> DeltaResult<HashMap<String, Value>> {
        self.operation_parameters_string_map().map(|parameters| {
            parameters
                .into_iter()
                .map(|(key, value)| (key, Value::String(value)))
                .collect()
        })
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
