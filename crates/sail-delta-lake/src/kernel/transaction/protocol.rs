// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/transaction/protocol.rs>

use std::collections::HashSet;
use std::sync::LazyLock;

use delta_kernel::table_features::{ReaderFeature, WriterFeature};
use deltalake::kernel::{contains_timestampntz, Action, Protocol, Schema};
use deltalake::protocol::DeltaOperation;
use deltalake::table::config::TablePropertiesExt as _;

use super::{TableReference, TransactionError};
use crate::kernel::snapshot::EagerSnapshot;
use crate::table::DeltaTableState;

static READER_V2: LazyLock<HashSet<ReaderFeature>> =
    LazyLock::new(|| HashSet::from_iter([ReaderFeature::ColumnMapping]));
static WRITER_V2: LazyLock<HashSet<WriterFeature>> =
    LazyLock::new(|| HashSet::from_iter([WriterFeature::AppendOnly, WriterFeature::Invariants]));
static WRITER_V3: LazyLock<HashSet<WriterFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        WriterFeature::AppendOnly,
        WriterFeature::Invariants,
        WriterFeature::CheckConstraints,
    ])
});
static WRITER_V4: LazyLock<HashSet<WriterFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        WriterFeature::AppendOnly,
        WriterFeature::Invariants,
        WriterFeature::CheckConstraints,
        WriterFeature::ChangeDataFeed,
        WriterFeature::GeneratedColumns,
    ])
});
static WRITER_V5: LazyLock<HashSet<WriterFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        WriterFeature::AppendOnly,
        WriterFeature::Invariants,
        WriterFeature::CheckConstraints,
        WriterFeature::ChangeDataFeed,
        WriterFeature::GeneratedColumns,
        WriterFeature::ColumnMapping,
    ])
});
static WRITER_V6: LazyLock<HashSet<WriterFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        WriterFeature::AppendOnly,
        WriterFeature::Invariants,
        WriterFeature::CheckConstraints,
        WriterFeature::ChangeDataFeed,
        WriterFeature::GeneratedColumns,
        WriterFeature::ColumnMapping,
        WriterFeature::IdentityColumns,
    ])
});

pub struct ProtocolChecker {
    reader_features: HashSet<ReaderFeature>,
    writer_features: HashSet<WriterFeature>,
}

impl ProtocolChecker {
    /// Create a new protocol checker.
    pub fn new(
        reader_features: HashSet<ReaderFeature>,
        writer_features: HashSet<WriterFeature>,
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
        let required_features: Option<&[WriterFeature]> =
            match snapshot.protocol().min_writer_version() {
                0..=6 => None,
                _ => snapshot.protocol().writer_features(),
            };

        if let Some(table_features) = required_features {
            if !table_features.contains(&WriterFeature::TimestampWithoutTimezone)
                && contains_timestampntz
            {
                return Err(TransactionError::WriterFeaturesRequired(
                    WriterFeature::TimestampWithoutTimezone,
                ));
            }
        } else if contains_timestampntz {
            return Err(TransactionError::WriterFeaturesRequired(
                WriterFeature::TimestampWithoutTimezone,
            ));
        }
        Ok(())
    }

    /// Check if delta-rs can read form the given delta table.
    pub fn can_read_from(&self, snapshot: &dyn TableReference) -> Result<(), TransactionError> {
        self.can_read_from_protocol(snapshot.protocol())
    }

    pub fn can_read_from_protocol(&self, protocol: &Protocol) -> Result<(), TransactionError> {
        let required_features: Option<HashSet<ReaderFeature>> = match protocol.min_reader_version()
        {
            0 | 1 => None,
            2 => Some(READER_V2.clone()),
            // _ => protocol.reader_features_set(),
            _ => Some(HashSet::new()), // FIXME: adopt kernel actions
        };
        if let Some(features) = required_features {
            let mut diff = features.difference(&self.reader_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedReaderFeatures(
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

        let required_features: Option<HashSet<WriterFeature>> = match min_writer_version {
            0 | 1 => None,
            2 => Some(WRITER_V2.clone()),
            3 => Some(WRITER_V3.clone()),
            4 => Some(WRITER_V4.clone()),
            5 => Some(WRITER_V5.clone()),
            6 => Some(WRITER_V6.clone()),
            //  _ => snapshot.protocol().writer_features_set(),
            _ => Some(HashSet::new()), // FIXME: adopt kernel actions
        };

        if let Some(features) = required_features {
            let mut diff = features.difference(&self.writer_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedWriterFeatures(
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
                .ok_or(TransactionError::WriterFeaturesRequired(
                    WriterFeature::AppendOnly,
                ))?
                .contains(&WriterFeature::AppendOnly)
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
    reader_features.insert(ReaderFeature::TimestampWithoutTimezone);
    // reader_features.insert(ReaderFeature::ColumnMapping);

    let mut writer_features = HashSet::new();
    writer_features.insert(WriterFeature::AppendOnly);
    writer_features.insert(WriterFeature::TimestampWithoutTimezone);
    {
        writer_features.insert(WriterFeature::ChangeDataFeed);
        writer_features.insert(WriterFeature::Invariants);
        writer_features.insert(WriterFeature::CheckConstraints);
        writer_features.insert(WriterFeature::GeneratedColumns);
    }
    // writer_features.insert(WriterFeature::ColumnMapping);
    // writer_features.insert(WriterFeature::IdentityColumns);

    ProtocolChecker::new(reader_features, writer_features)
});
