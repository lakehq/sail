use std::collections::BTreeMap;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{Result, exec_datafusion_err, internal_err};
use sail_common_datafusion::datasource::{
    OPERATION_COLUMN, RowLevelCommand, RowLevelOperationType, RowLevelWriteMode,
};
use serde::{Deserialize, Serialize};

/// Command identity is independent of the snapshot's storage update algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcebergOperation {
    Insert,
    Update,
    Delete,
    Merge,
}

impl IcebergOperation {
    pub(crate) fn from_row_level(command: RowLevelCommand) -> Option<Self> {
        match command {
            RowLevelCommand::Update => Some(Self::Update),
            RowLevelCommand::Delete => Some(Self::Delete),
            RowLevelCommand::Merge => Some(Self::Merge),
            RowLevelCommand::Truncate => None,
        }
    }
}

/// Logical actions, counted before data and position-delete projection.
/// None is unknown, not zero; combining partitions propagates unknown counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergActionSummary {
    operation: IcebergOperation,
    copied: Option<u64>,
    deleted: Option<u64>,
    updated: Option<u64>,
    inserted: Option<u64>,
    matched_updated: Option<u64>,
    matched_deleted: Option<u64>,
    target_only_updated: Option<u64>,
    target_only_deleted: Option<u64>,
}

/// Only the owned Spark action keys can enter the snapshot property boundary.
#[derive(Debug, Clone, Default)]
pub(crate) struct ActionSummaryProperties {
    operation: Option<IcebergOperation>,
    properties: BTreeMap<String, String>,
}

impl ActionSummaryProperties {
    pub(crate) fn operation(&self) -> Option<IcebergOperation> {
        self.operation
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.properties.iter()
    }
}

fn add_count(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>> {
    match (left, right) {
        (Some(left), Some(right)) => left
            .checked_add(right)
            .map(Some)
            .ok_or_else(|| exec_datafusion_err!("Iceberg action row count overflow")),
        _ => Ok(None),
    }
}

impl IcebergActionSummary {
    pub(crate) fn new(operation: IcebergOperation) -> Self {
        Self {
            operation,
            copied: Some(0),
            deleted: Some(0),
            updated: Some(0),
            inserted: Some(0),
            matched_updated: Some(0),
            matched_deleted: Some(0),
            target_only_updated: Some(0),
            target_only_deleted: Some(0),
        }
    }

    pub(crate) fn add_batch(
        &mut self,
        batch: &RecordBatch,
        mode: Option<RowLevelWriteMode>,
    ) -> Result<()> {
        if mode.is_none() {
            if self.operation != IcebergOperation::Insert {
                return internal_err!("Iceberg row-level summary requires operation tags");
            }
            self.inserted = add_count(self.inserted, Some(batch.num_rows() as u64))?;
            return Ok(());
        }
        let operations = batch.column_by_name(OPERATION_COLUMN).ok_or_else(|| {
            exec_datafusion_err!("Iceberg row-level summary is missing operation tags")
        })?;
        let operations = cast(operations, &DataType::Int64)?;
        let operations = operations
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| exec_datafusion_err!("Expected Iceberg operation integers"))?;
        for operation in operations.iter() {
            let operation = operation.ok_or_else(|| {
                exec_datafusion_err!("Iceberg row-level summary received a null operation")
            })?;
            let operation = RowLevelOperationType::try_from(operation).map_err(|value| {
                exec_datafusion_err!("Invalid Iceberg row-level operation: {value}")
            })?;
            self.add_operation(operation, mode == Some(RowLevelWriteMode::CopyOnWrite))?;
        }
        Ok(())
    }

    fn add_operation(
        &mut self,
        operation: RowLevelOperationType,
        copy_on_write: bool,
    ) -> Result<()> {
        use RowLevelOperationType::*;
        match operation {
            Copy if copy_on_write => self.copied = add_count(self.copied, Some(1))?,
            Insert => self.inserted = add_count(self.inserted, Some(1))?,
            Update | MatchedUpdate | NotMatchedBySourceUpdate => {
                self.updated = add_count(self.updated, Some(1))?;
                if operation == MatchedUpdate {
                    self.matched_updated = add_count(self.matched_updated, Some(1))?;
                } else if operation == NotMatchedBySourceUpdate {
                    self.target_only_updated = add_count(self.target_only_updated, Some(1))?;
                }
            }
            Delete | MatchedDelete | NotMatchedBySourceDelete => {
                self.deleted = add_count(self.deleted, Some(1))?;
                if operation == MatchedDelete {
                    self.matched_deleted = add_count(self.matched_deleted, Some(1))?;
                } else if operation == NotMatchedBySourceDelete {
                    self.target_only_deleted = add_count(self.target_only_deleted, Some(1))?;
                }
            }
            Copy | Noop | SourceMetric => {}
        }
        Ok(())
    }

    pub(crate) fn checked_add(&self, other: &Self) -> Result<Self> {
        if self.operation != other.operation {
            return internal_err!("Inconsistent Iceberg action summary operations");
        }
        Ok(Self {
            operation: self.operation,
            copied: add_count(self.copied, other.copied)?,
            deleted: add_count(self.deleted, other.deleted)?,
            updated: add_count(self.updated, other.updated)?,
            inserted: add_count(self.inserted, other.inserted)?,
            matched_updated: add_count(self.matched_updated, other.matched_updated)?,
            matched_deleted: add_count(self.matched_deleted, other.matched_deleted)?,
            target_only_updated: add_count(self.target_only_updated, other.target_only_updated)?,
            target_only_deleted: add_count(self.target_only_deleted, other.target_only_deleted)?,
        })
    }

    pub(crate) fn properties(&self) -> ActionSummaryProperties {
        let counts = match self.operation {
            IcebergOperation::Insert => vec![("spark.insert.num-inserted-rows", self.inserted)],
            IcebergOperation::Update => vec![
                ("spark.update.num-updated-rows", self.updated),
                ("spark.update.num-copied-rows", self.copied),
            ],
            IcebergOperation::Delete => vec![
                ("spark.delete.num-deleted-rows", self.deleted),
                ("spark.delete.num-copied-rows", self.copied),
            ],
            IcebergOperation::Merge => vec![
                ("spark.merge-into.num-target-rows-copied", self.copied),
                ("spark.merge-into.num-target-rows-deleted", self.deleted),
                ("spark.merge-into.num-target-rows-updated", self.updated),
                ("spark.merge-into.num-target-rows-inserted", self.inserted),
                (
                    "spark.merge-into.num-target-rows-matched-updated",
                    self.matched_updated,
                ),
                (
                    "spark.merge-into.num-target-rows-matched-deleted",
                    self.matched_deleted,
                ),
                (
                    "spark.merge-into.num-target-rows-not-matched-by-source-updated",
                    self.target_only_updated,
                ),
                (
                    "spark.merge-into.num-target-rows-not-matched-by-source-deleted",
                    self.target_only_deleted,
                ),
            ],
        };
        ActionSummaryProperties {
            operation: Some(self.operation),
            properties: counts
                .into_iter()
                .filter_map(|(key, value)| value.map(|value| (key.to_string(), value.to_string())))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_preserve_identity_without_known_counts() {
        let default = ActionSummaryProperties::default();
        assert_eq!(default.operation(), None);
        assert_eq!(default.iter().count(), 0);
        for operation in [
            IcebergOperation::Insert,
            IcebergOperation::Update,
            IcebergOperation::Delete,
            IcebergOperation::Merge,
        ] {
            let known = IcebergActionSummary::new(operation).properties();
            assert_eq!(known.operation(), Some(operation));
            assert!(known.iter().all(|(_, value)| value == "0"));
            let unknown = IcebergActionSummary {
                operation,
                copied: None,
                deleted: None,
                updated: None,
                inserted: None,
                matched_updated: None,
                matched_deleted: None,
                target_only_updated: None,
                target_only_deleted: None,
            }
            .properties();
            assert_eq!(unknown.operation(), Some(operation));
            assert_eq!(unknown.iter().count(), 0);
        }
    }

    #[test]
    fn summary_counts_logical_actions_and_propagates_unknown() -> Result<()> {
        let mut summary = IcebergActionSummary::new(IcebergOperation::Merge);
        for operation in [
            RowLevelOperationType::MatchedUpdate,
            RowLevelOperationType::MatchedDelete,
            RowLevelOperationType::NotMatchedBySourceUpdate,
            RowLevelOperationType::NotMatchedBySourceDelete,
            RowLevelOperationType::Insert,
            RowLevelOperationType::Copy,
            RowLevelOperationType::Noop,
            RowLevelOperationType::SourceMetric,
        ] {
            summary.add_operation(operation, true)?;
        }
        assert_eq!(summary.updated, Some(2));
        assert_eq!(summary.deleted, Some(2));
        assert_eq!(summary.copied, Some(1));
        assert_eq!(summary.properties().iter().count(), 8);
        let mut unknown = IcebergActionSummary::new(IcebergOperation::Merge);
        unknown.copied = None;
        let combined = summary.checked_add(&unknown)?;
        assert_eq!(combined.copied, None);
        assert_eq!(combined.properties().iter().count(), 7);
        assert!(
            summary
                .checked_add(&IcebergActionSummary::new(IcebergOperation::Update))
                .is_err()
        );
        summary.updated = Some(u64::MAX);
        assert!(summary.checked_add(&combined).is_err());
        Ok(())
    }
}
