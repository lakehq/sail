// SPDX-License-Identifier: Apache-2.0
// SPDX-FileContributor: The Apache Software Foundation (ASF)
// SPDX-FileComment: See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// SPDX-FileComment: Credit: https://github.com/apache/iceberg-rust/blob/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/encrypted_key.rs

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Metadata for an encrypted table key in Iceberg v3 table metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EncryptedKey {
    pub key_id: String,
    pub encrypted_key_metadata: Vec<u8>,
    pub encrypted_by_id: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}
