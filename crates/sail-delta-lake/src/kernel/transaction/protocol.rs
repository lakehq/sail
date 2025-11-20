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

use delta_kernel::table_features::TableFeature;

use super::{TableReference, TransactionError};
use crate::kernel::models::{contains_timestampntz, Action, Protocol, Schema};
use crate::kernel::snapshot::EagerSnapshot;
use crate::kernel::{DeltaOperation, TablePropertiesExt};
use crate::table::DeltaTableState;

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
    #[allow(unused)]
    pub fn default_reader_version(&self) -> i32 {
        1
    }
    #[allow(unused)]
    pub fn default_writer_version(&self) -> i32 {
        2
    }

    /// Check append-only at the high level (operation level)
    #[allow(unused)]
    pub fn check_append_only(&self, snapshot: &EagerSnapshot) -> Result<(), TransactionError> {
        if snapshot.table_properties().append_only() {
            return Err(TransactionError::DeltaTableAppendOnly);
        }
        Ok(())
    }

    /// Check can write_timestamp_ntz
    #[allow(unused)]
    pub fn check_can_write_timestamp_ntz(
        &self,
        snapshot: &DeltaTableState,
        schema: &Schema,
    ) -> Result<(), TransactionError> {
        let contains_timestampntz = contains_timestampntz(schema.fields());
        let required_features: Option<&[TableFeature]> =
            match snapshot.protocol().min_writer_version() {
                0..=6 => None,
                _ => snapshot.protocol().writer_features(),
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

    /// Check if delta-rs can read form the given delta table.
    pub fn can_read_from(&self, snapshot: &dyn TableReference) -> Result<(), TransactionError> {
        self.can_read_from_protocol(snapshot.protocol())
    }

    pub fn can_read_from_protocol(&self, protocol: &Protocol) -> Result<(), TransactionError> {
        let required_features: Option<HashSet<TableFeature>> = match protocol.min_reader_version() {
            0 | 1 => None,
            2 => Some(READER_V2.clone()),
            // _ => protocol.reader_features_set(),
            _ => Some(HashSet::new()),
        };
        if let Some(features) = required_features {
            let mut diff = features.difference(&self.reader_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedTableFeatures(
                    diff.cloned().collect(),
                ));
            }
        };
        Ok(())
    }

    /// Check if delta-rs can write to the given delta table.
    pub fn can_write_to(&self, snapshot: &dyn TableReference) -> Result<(), TransactionError> {
        // NOTE: writers must always support all required reader features
        self.can_read_from(snapshot)?;
        let min_writer_version = snapshot.protocol().min_writer_version();

        let required_features: Option<HashSet<TableFeature>> = match min_writer_version {
            0 | 1 => None,
            2 => Some(WRITER_V2.clone()),
            3 => Some(WRITER_V3.clone()),
            4 => Some(WRITER_V4.clone()),
            5 => Some(WRITER_V5.clone()),
            6 => Some(WRITER_V6.clone()),
            //  _ => snapshot.protocol().writer_features_set(),
            _ => Some(HashSet::new()),
        };

        if let Some(features) = required_features {
            let mut diff = features.difference(&self.writer_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedTableFeatures(
                    diff.cloned().collect(),
                ));
            }
        };
        Ok(())
    }

    pub fn can_commit(
        &self,
        snapshot: &dyn TableReference,
        actions: &[Action],
        operation: &DeltaOperation,
    ) -> Result<(), TransactionError> {
        self.can_write_to(snapshot)?;

        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
        let append_only_enabled = if snapshot.protocol().min_writer_version() < 2 {
            false
        } else if snapshot.protocol().min_writer_version() < 7 {
            snapshot.config().append_only()
        } else {
            snapshot
                .protocol()
                .writer_features()
                .ok_or(TransactionError::TableFeaturesRequired(
                    TableFeature::AppendOnly,
                ))?
                .contains(&TableFeature::AppendOnly)
                && snapshot.config().append_only()
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

    let mut writer_features = HashSet::new();
    writer_features.insert(TableFeature::AppendOnly);
    writer_features.insert(TableFeature::TimestampWithoutTimezone);
    {
        writer_features.insert(TableFeature::ChangeDataFeed);
        writer_features.insert(TableFeature::Invariants);
        writer_features.insert(TableFeature::CheckConstraints);
        writer_features.insert(TableFeature::GeneratedColumns);
    }
    writer_features.insert(TableFeature::ColumnMapping);
    // writer_features.insert(TableFeature::IdentityColumns);

    ProtocolChecker::new(reader_features, writer_features)
});
