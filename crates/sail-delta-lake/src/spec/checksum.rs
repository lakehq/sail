// https://github.com/delta-io/delta/blob/master/PROTOCOL.md
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

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::spec::actions::DomainMetadata;
use crate::spec::{Add, Metadata, Protocol, Transaction};

// Version checksum for a committed Delta log version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionChecksum {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_id: Option<String>,
    pub table_size_bytes: i64,
    pub num_files: i64,
    pub num_metadata: i64,
    pub num_protocol: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_commit_timestamp_opt: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set_transactions: Option<Vec<Transaction>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_metadata: Option<Vec<DomainMetadata>>,
    pub metadata: Metadata,
    pub protocol: Protocol,
    // TODO: Fill these optional fields when we can compute them
    // faithfully from reconciled state instead of omitting them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size_histogram: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_files: Option<Vec<Add>>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::VersionChecksum;
    use crate::spec::{
        DataType, DeltaResult, Metadata, Protocol, StructField, StructType, TableFeature,
    };

    fn test_metadata() -> DeltaResult<Metadata> {
        Metadata::try_new(
            Some("test".to_string()),
            Some("checksum".to_string()),
            StructType::try_new([StructField::not_null("id", DataType::LONG)])?,
            Vec::new(),
            123,
            HashMap::from([("delta.appendOnly".to_string(), "true".to_string())]),
        )
    }

    #[test]
    fn version_checksum_json_roundtrip() -> DeltaResult<()> {
        let checksum = VersionChecksum {
            txn_id: Some("txn-123".to_string()),
            table_size_bytes: 42,
            num_files: 3,
            num_metadata: 1,
            num_protocol: 1,
            in_commit_timestamp_opt: Some(456),
            set_transactions: None,
            domain_metadata: None,
            metadata: test_metadata()?,
            protocol: Protocol::new(
                3,
                7,
                Some(vec![TableFeature::ColumnMapping]),
                Some(vec![TableFeature::AppendOnly, TableFeature::ColumnMapping]),
            ),
            file_size_histogram: None,
            all_files: None,
        };

        let json = serde_json::to_string(&checksum)?;
        let decoded: VersionChecksum = serde_json::from_str(&json)?;
        assert_eq!(decoded, checksum);
        Ok(())
    }
}
