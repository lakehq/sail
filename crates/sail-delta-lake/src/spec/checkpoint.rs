// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: License: https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
// SPDX-FileCopyrightText: Copyright 2023-2024 The Delta Kernel Rust Authors
// SPDX-FileCopyrightText: Portions Copyright 2025-2026 LakeSail, Inc.
// SPDX-FileContributor: Ported and modified by LakeSail, Inc. in 2026

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::{
    Add, CheckpointMetadata, DataType, DomainMetadata, Metadata, Protocol, Remove, Sidecar,
    StructField, StructType, Transaction, add_struct_type, checkpoint_metadata_struct_type,
    domain_metadata_struct_type, metadata_struct_type, protocol_struct_type, remove_struct_type,
    sidecar_struct_type, transaction_struct_type,
};

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/checkpoint/mod.rs#L126-L135
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
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/last_checkpoint_hint.rs#L14-L46
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v2_checkpoint: Option<LastCheckpointV2>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCheckpointV2 {
    pub path: String,
    pub size_in_bytes: i64,
    pub modification_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub non_file_actions: Option<Vec<CheckpointActionRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sidecar_files: Option<Vec<Sidecar>>,
}
// SPDX-SnippetEnd
