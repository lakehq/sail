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

use serde::{Deserialize, Serialize};

use crate::spec::properties::TableProperties;
use crate::spec::{DeltaError as DeltaTableError, DeltaResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/table_features/mod.rs#L44-L119>
pub enum TableFeature {
    AppendOnly,
    Invariants,
    CheckConstraints,
    ChangeDataFeed,
    #[serde(rename = "allowColumnDefaults")]
    AllowColumnDefaults,
    GeneratedColumns,
    IdentityColumns,
    ColumnMapping,
    DeletionVectors,
    RowTracking,
    DomainMetadata,
    #[serde(rename = "v2Checkpoint")]
    V2Checkpoint,
    #[serde(rename = "inCommitTimestamp")]
    InCommitTimestamp,
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    // TODO: Implement reader/writer support for these newer protocol features.
    // For now we only register the official names so protocol parsing succeeds and
    // ProtocolChecker can reject unsupported tables explicitly.
    #[serde(rename = "icebergCompatV1")]
    IcebergCompatV1,
    #[serde(rename = "icebergCompatV2")]
    IcebergCompatV2,
    Clustering,
    VacuumProtocolCheck,
    VariantType,
    TypeWidening,
    CatalogManaged,
    #[serde(other)]
    Unknown,
}

impl TableFeature {
    pub fn as_str(&self) -> &str {
        match self {
            Self::AppendOnly => "appendOnly",
            Self::Invariants => "invariants",
            Self::CheckConstraints => "checkConstraints",
            Self::ChangeDataFeed => "changeDataFeed",
            Self::AllowColumnDefaults => "allowColumnDefaults",
            Self::GeneratedColumns => "generatedColumns",
            Self::IdentityColumns => "identityColumns",
            Self::ColumnMapping => "columnMapping",
            Self::DeletionVectors => "deletionVectors",
            Self::RowTracking => "rowTracking",
            Self::DomainMetadata => "domainMetadata",
            Self::V2Checkpoint => "v2Checkpoint",
            Self::InCommitTimestamp => "inCommitTimestamp",
            Self::TimestampWithoutTimezone => "timestampNtz",
            Self::IcebergCompatV1 => "icebergCompatV1",
            Self::IcebergCompatV2 => "icebergCompatV2",
            Self::Clustering => "clustering",
            Self::VacuumProtocolCheck => "vacuumProtocolCheck",
            Self::VariantType => "variantType",
            Self::TypeWidening => "typeWidening",
            Self::CatalogManaged => "catalogManaged",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse_str_name(value: &str) -> DeltaResult<Self> {
        match value {
            "appendOnly" => Ok(Self::AppendOnly),
            "invariants" => Ok(Self::Invariants),
            "checkConstraints" => Ok(Self::CheckConstraints),
            "changeDataFeed" => Ok(Self::ChangeDataFeed),
            "allowColumnDefaults" => Ok(Self::AllowColumnDefaults),
            "generatedColumns" => Ok(Self::GeneratedColumns),
            "identityColumns" => Ok(Self::IdentityColumns),
            "columnMapping" => Ok(Self::ColumnMapping),
            "deletionVectors" => Ok(Self::DeletionVectors),
            "rowTracking" => Ok(Self::RowTracking),
            "domainMetadata" => Ok(Self::DomainMetadata),
            "v2Checkpoint" => Ok(Self::V2Checkpoint),
            "inCommitTimestamp" => Ok(Self::InCommitTimestamp),
            "timestampNtz" => Ok(Self::TimestampWithoutTimezone),
            "icebergCompatV1" => Ok(Self::IcebergCompatV1),
            "icebergCompatV2" => Ok(Self::IcebergCompatV2),
            "clustering" => Ok(Self::Clustering),
            "vacuumProtocolCheck" => Ok(Self::VacuumProtocolCheck),
            "variantType" => Ok(Self::VariantType),
            "typeWidening" => Ok(Self::TypeWidening),
            "catalogManaged" => Ok(Self::CatalogManaged),
            _ => Err(DeltaTableError::generic(format!(
                "Unknown table feature: {value}"
            ))),
        }
    }

    /// Returns `true` if this feature requires reader support (readerVersion >= 3).
    pub fn is_reader_feature(&self) -> bool {
        matches!(
            self,
            Self::ColumnMapping
                | Self::DeletionVectors
                | Self::TimestampWithoutTimezone
                | Self::V2Checkpoint
                | Self::VacuumProtocolCheck
                | Self::CatalogManaged
                | Self::VariantType
                | Self::TypeWidening
        )
    }
}

#[cfg(test)]
mod tests {
    use super::TableFeature;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn table_feature_string_mappings_include_recent_protocol_features() {
        let cases = [
            (TableFeature::IcebergCompatV1, "icebergCompatV1"),
            (TableFeature::IcebergCompatV2, "icebergCompatV2"),
            (TableFeature::Clustering, "clustering"),
            (TableFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (TableFeature::VariantType, "variantType"),
            (TableFeature::TypeWidening, "typeWidening"),
            (TableFeature::CatalogManaged, "catalogManaged"),
        ];

        for (feature, expected_name) in cases {
            assert_eq!(feature.as_str(), expected_name);
            assert_eq!(
                TableFeature::parse_str_name(expected_name).unwrap(),
                feature
            );
        }
    }
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
    pub fn new(
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: Option<Vec<TableFeature>>,
        writer_features: Option<Vec<TableFeature>>,
    ) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
            reader_features: reader_features.filter(|features| !features.is_empty()),
            writer_features: writer_features.filter(|features| !features.is_empty()),
        }
    }

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

    /// Returns `true` if the table protocol explicitly declares the given reader feature.
    pub fn has_reader_feature(&self, feature: &TableFeature) -> bool {
        self.reader_features()
            .is_some_and(|features| features.contains(feature))
    }

    /// Returns `true` if the table protocol explicitly declares the given writer feature.
    pub fn has_writer_feature(&self, feature: &TableFeature) -> bool {
        self.writer_features()
            .is_some_and(|features| features.contains(feature))
    }

    pub fn supports_in_commit_timestamps(&self) -> bool {
        self.min_writer_version() >= 7 && self.has_writer_feature(&TableFeature::InCommitTimestamp)
    }

    pub fn is_in_commit_timestamps_enabled(&self, table_properties: &TableProperties) -> bool {
        self.supports_in_commit_timestamps() && table_properties.enable_in_commit_timestamps()
    }
}
