// https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
//
// Copyright 2023-2024 The Delta Kernel Rust Authors
// Portions Copyright 2025-2026 LakeSail, Inc.
// Ported and modified in 2026 by LakeSail, Inc.
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

use crate::spec::{
    add_struct_type, checkpoint_metadata_struct_type, domain_metadata_struct_type,
    metadata_struct_type, protocol_struct_type, remove_struct_type, sidecar_struct_type,
    transaction_struct_type, Add, CheckpointMetadata, DataType, DomainMetadata, Metadata, Protocol,
    Remove, Sidecar, StructField, StructType, Transaction,
};

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/checkpoint/mod.rs#L126-L135>
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointActionRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add: Option<Add>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove: Option<Remove>,
    #[serde(
        rename = "metaData",
        alias = "metadata",
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata: Option<Metadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
    #[serde(rename = "txn", skip_serializing_if = "Option::is_none")]
    pub txn: Option<Transaction>,
    #[serde(rename = "domainMetadata", skip_serializing_if = "Option::is_none")]
    pub domain_metadata: Option<DomainMetadata>,
    #[serde(rename = "checkpointMetadata", skip_serializing_if = "Option::is_none")]
    pub checkpoint_metadata: Option<CheckpointMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sidecar: Option<Sidecar>,
}

impl CheckpointActionRow {
    pub fn struct_type() -> StructType {
        StructType::new_unchecked([
            StructField::nullable("add", DataType::from(add_struct_type())),
            StructField::nullable("remove", DataType::from(remove_struct_type())),
            StructField::nullable("metaData", DataType::from(metadata_struct_type())),
            StructField::nullable("protocol", DataType::from(protocol_struct_type())),
            StructField::nullable("txn", DataType::from(transaction_struct_type())),
            StructField::nullable(
                "domainMetadata",
                DataType::from(domain_metadata_struct_type()),
            ),
            StructField::nullable(
                "checkpointMetadata",
                DataType::from(checkpoint_metadata_struct_type()),
            ),
            StructField::nullable("sidecar", DataType::from(sidecar_struct_type())),
        ])
    }
}

// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/last_checkpoint_hint.rs#L14-L46>
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCheckpointHint {
    pub version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_of_add_files: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_schema: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
}
