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
    GeneratedColumns,
    IdentityColumns,
    ColumnMapping,
    #[serde(rename = "inCommitTimestamp")]
    InCommitTimestamp,
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
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
            Self::GeneratedColumns => "generatedColumns",
            Self::IdentityColumns => "identityColumns",
            Self::ColumnMapping => "columnMapping",
            Self::InCommitTimestamp => "inCommitTimestamp",
            Self::TimestampWithoutTimezone => "timestampNtz",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse_str_name(value: &str) -> DeltaResult<Self> {
        match value {
            "appendOnly" => Ok(Self::AppendOnly),
            "invariants" => Ok(Self::Invariants),
            "checkConstraints" => Ok(Self::CheckConstraints),
            "changeDataFeed" => Ok(Self::ChangeDataFeed),
            "generatedColumns" => Ok(Self::GeneratedColumns),
            "identityColumns" => Ok(Self::IdentityColumns),
            "columnMapping" => Ok(Self::ColumnMapping),
            "inCommitTimestamp" => Ok(Self::InCommitTimestamp),
            "timestampNtz" => Ok(Self::TimestampWithoutTimezone),
            _ => Err(DeltaTableError::generic(format!(
                "Unknown table feature: {value}"
            ))),
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

    pub fn has_reader_feature(&self, feature: &TableFeature) -> bool {
        self.reader_features()
            .is_some_and(|features| features.contains(feature))
    }

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
