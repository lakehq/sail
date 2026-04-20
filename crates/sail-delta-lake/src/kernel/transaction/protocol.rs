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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/transaction/protocol.rs>

use std::collections::HashSet;
use std::sync::LazyLock;

use crate::kernel::DeltaOperation;
use crate::spec::{
    contains_timestampntz, Action, CommitConflictError, Protocol, Schema, TableFeature,
    TransactionError,
};
use crate::table::DeltaSnapshot;

static READER_V2: LazyLock<HashSet<TableFeature>> =
    LazyLock::new(|| HashSet::from_iter([TableFeature::ColumnMapping]));
static WRITER_V2: LazyLock<HashSet<TableFeature>> =
    LazyLock::new(|| HashSet::from_iter([TableFeature::AppendOnly, TableFeature::Invariants]));
static WRITER_V3: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
    ])
});
static WRITER_V4: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
        TableFeature::ChangeDataFeed,
        TableFeature::GeneratedColumns,
    ])
});
static WRITER_V5: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
        TableFeature::ChangeDataFeed,
        TableFeature::GeneratedColumns,
        TableFeature::ColumnMapping,
    ])
});
static WRITER_V6: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
        TableFeature::ChangeDataFeed,
        TableFeature::GeneratedColumns,
        TableFeature::ColumnMapping,
        TableFeature::IdentityColumns,
    ])
});

pub struct ProtocolChecker {
    reader_features: HashSet<TableFeature>,
    writer_features: HashSet<TableFeature>,
}

impl ProtocolChecker {
    /// Create a new protocol checker.
    pub fn new(
        reader_features: HashSet<TableFeature>,
        writer_features: HashSet<TableFeature>,
    ) -> Self {
        Self {
            reader_features,
            writer_features,
        }
    }
    #[expect(unused)]
    pub fn default_reader_version(&self) -> i32 {
        1
    }
    #[expect(unused)]
    pub fn default_writer_version(&self) -> i32 {
        2
    }

    /// Check append-only at the high level (operation level)
    #[expect(unused)]
    pub fn check_append_only(&self, snapshot: &DeltaSnapshot) -> Result<(), TransactionError> {
        if snapshot.table_properties().append_only() {
            return Err(TransactionError::DeltaTableAppendOnly);
        }
        Ok(())
    }

    fn required_reader_features(
        &self,
        protocol: &Protocol,
    ) -> Result<Option<HashSet<TableFeature>>, TransactionError> {
        match protocol.min_reader_version() {
            0 | 1 => Ok(None),
            2 => Ok(Some(READER_V2.clone())),
            3 => Ok(Some(
                protocol
                    .reader_features()
                    .unwrap_or(&[])
                    .iter()
                    .cloned()
                    .collect(),
            )),
            version => Err(TransactionError::CommitConflict(
                CommitConflictError::UnsupportedReaderVersion(version),
            )),
        }
    }

    fn required_writer_features(
        &self,
        protocol: &Protocol,
    ) -> Result<Option<HashSet<TableFeature>>, TransactionError> {
        // Delta protocol differs here:
        // - writer versions 2..=6 imply a fixed feature set from `minWriterVersion`
        // - writer version 7 uses the explicit `writerFeatures` declared by the table
        match protocol.min_writer_version() {
            0 | 1 => Ok(None),
            2 => Ok(Some(WRITER_V2.clone())),
            3 => Ok(Some(WRITER_V3.clone())),
            4 => Ok(Some(WRITER_V4.clone())),
            5 => Ok(Some(WRITER_V5.clone())),
            6 => Ok(Some(WRITER_V6.clone())),
            7 => Ok(Some(
                protocol
                    .writer_features()
                    .unwrap_or(&[])
                    .iter()
                    .cloned()
                    .collect(),
            )),
            version => Err(TransactionError::CommitConflict(
                CommitConflictError::UnsupportedWriterVersion(version),
            )),
        }
    }

    pub(crate) fn unsupported_reader_features(
        &self,
        protocol: &Protocol,
    ) -> Result<Vec<TableFeature>, TransactionError> {
        let Some(features) = self.required_reader_features(protocol)? else {
            return Ok(vec![]);
        };
        Ok(features
            .difference(&self.reader_features)
            .cloned()
            .collect::<Vec<_>>())
    }

    pub(crate) fn unsupported_writer_features(
        &self,
        protocol: &Protocol,
    ) -> Result<Vec<TableFeature>, TransactionError> {
        let Some(features) = self.required_writer_features(protocol)? else {
            return Ok(vec![]);
        };
        Ok(features
            .difference(&self.writer_features)
            .cloned()
            .collect::<Vec<_>>())
    }

    pub fn can_read_from_protocol(&self, protocol: &Protocol) -> Result<(), TransactionError> {
        let diff = self.unsupported_reader_features(protocol)?;
        if !diff.is_empty() {
            return Err(TransactionError::UnsupportedTableFeatures(diff));
        }
        Ok(())
    }

    pub fn check_can_write_timestamp_ntz_to_protocol(
        &self,
        protocol: &Protocol,
        schema: &Schema,
    ) -> Result<(), TransactionError> {
        let contains_timestampntz = contains_timestampntz(schema.fields());
        let required_features: Option<&[TableFeature]> = match protocol.min_writer_version() {
            0..=6 => None,
            _ => protocol.writer_features(),
        };

        if let Some(table_features) = required_features {
            if !table_features.contains(&TableFeature::TimestampWithoutTimezone)
                && contains_timestampntz
            {
                return Err(TransactionError::TableFeaturesRequired(
                    TableFeature::TimestampWithoutTimezone,
                ));
            }
        } else if contains_timestampntz {
            return Err(TransactionError::TableFeaturesRequired(
                TableFeature::TimestampWithoutTimezone,
            ));
        }
        Ok(())
    }

    pub fn can_write_to_protocol(&self, protocol: &Protocol) -> Result<(), TransactionError> {
        // NOTE: writers must always support all required reader features
        self.can_read_from_protocol(protocol)?;
        let diff = self.unsupported_writer_features(protocol)?;
        if !diff.is_empty() {
            return Err(TransactionError::UnsupportedTableFeatures(diff));
        }
        Ok(())
    }

    pub fn can_commit(
        &self,
        snapshot: &DeltaSnapshot,
        actions: &[Action],
        operation: &DeltaOperation,
    ) -> Result<(), TransactionError> {
        self.can_write_to_protocol(snapshot.protocol())?;

        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
        let append_only_enabled = if snapshot.protocol().min_writer_version() < 2 {
            false
        } else if snapshot.protocol().min_writer_version() < 7 {
            snapshot.table_properties().append_only()
        } else {
            snapshot
                .protocol()
                .has_writer_feature(&TableFeature::AppendOnly)
                && snapshot.table_properties().append_only()
        };
        if append_only_enabled {
            match operation {
                DeltaOperation::Restore { .. } | DeltaOperation::FileSystemCheck { .. } => {}
                _ => {
                    actions.iter().try_for_each(|action| match action {
                        Action::Remove(remove) if remove.data_change => {
                            Err(TransactionError::DeltaTableAppendOnly)
                        }
                        _ => Ok(()),
                    })?;
                }
            }
        }

        Ok(())
    }
}

/// The global protocol checker instance to validate table versions and features.
///
/// This instance is used by default in all transaction operations, since feature
/// support is not configurable but rather decided at compile time.
///
/// As we implement new features, we need to update this instance accordingly.
/// resulting version support is determined by the supported table feature set.
pub static INSTANCE: LazyLock<ProtocolChecker> = LazyLock::new(|| {
    let mut reader_features = HashSet::new();
    reader_features.insert(TableFeature::TimestampWithoutTimezone);
    reader_features.insert(TableFeature::ColumnMapping);
    reader_features.insert(TableFeature::V2Checkpoint);

    let mut writer_features = HashSet::new();
    // Keep this list aligned with end-to-end behavior, not just protocol parsing.
    // For writer versions 2..=6, claiming support here also means accepting older tables whose
    // `minWriterVersion` implies the feature set in WRITER_V2..WRITER_V6.
    writer_features.insert(TableFeature::AppendOnly);
    writer_features.insert(TableFeature::InCommitTimestamp);
    writer_features.insert(TableFeature::TimestampWithoutTimezone);
    // writer_features.insert(TableFeature::DomainMetadata);
    writer_features.insert(TableFeature::ColumnMapping);
    // writer_features.insert(TableFeature::ChangeDataFeed);
    // FIXME: implement delta.invariants
    writer_features.insert(TableFeature::Invariants);
    // writer_features.insert(TableFeature::CheckConstraints);
    // writer_features.insert(TableFeature::GeneratedColumns);
    // writer_features.insert(TableFeature::IdentityColumns);
    writer_features.insert(TableFeature::V2Checkpoint);

    ProtocolChecker::new(reader_features, writer_features)
});
