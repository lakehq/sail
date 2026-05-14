use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::DeltaSnapshot;
use crate::schema::{
    ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME_KEY,
    ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME_KEY,
};
use crate::spec::{ColumnMappingMode, DeltaError, DeltaResult, TableFeature};

/// Proof that column mapping is active on a snapshot.
#[derive(Debug)]
pub struct ColumnMappingToken {
    pub mode: ColumnMappingMode,
}

/// Proof that deletion vectors are enabled for reads and writes.
#[derive(Debug)]
pub struct DeletionVectorToken;

/// Change Data Feed protocol support advertised by the current snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeDataFeedSupport {
    Unsupported,
    Legacy,
    WriterFeature,
}

/// Proof that Change Data Feed is active on the current snapshot.
///
/// CDF is special because protocol support and current-snapshot activation are distinct:
/// a table may support historical CDF reads even when the current table property is off.
#[derive(Debug)]
pub struct ChangeDataFeedToken;

/// Row tracking state derived from a snapshot.
#[derive(Debug)]
pub enum RowTrackingToken {
    Enabled(EnabledRowTrackingToken),
    SupportedOnly(SupportedRowTrackingToken),
    Suspended,
    Unsupported,
}

/// Row tracking is enabled and new row ids can be allocated.
#[derive(Debug)]
pub struct EnabledRowTrackingToken {
    pub next_row_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowTrackingMaterializedColumnNames {
    pub row_id: String,
    pub row_commit_version: String,
}

impl EnabledRowTrackingToken {
    pub fn assign_next_row_id(&mut self) -> i64 {
        let id = self.next_row_id;
        self.next_row_id = self.next_row_id.saturating_add(1);
        id
    }

    /// Reserve a contiguous `[base, base + num_records)` row-id range.
    pub fn reserve_row_ids(&mut self, num_records: i64) -> i64 {
        let base = self.next_row_id;
        self.next_row_id = self.next_row_id.saturating_add(num_records.max(0));
        base
    }
}

/// Row tracking is supported but the table is not fully enabled yet.
#[derive(Debug)]
pub struct SupportedRowTrackingToken {
    pub next_row_id: i64,
}

impl SupportedRowTrackingToken {
    pub fn assign_next_row_id(&mut self) -> i64 {
        let id = self.next_row_id;
        self.next_row_id = self.next_row_id.saturating_add(1);
        id
    }

    /// Reserve a contiguous `[base, base + num_records)` row-id range.
    pub fn reserve_row_ids(&mut self, num_records: i64) -> i64 {
        let base = self.next_row_id;
        self.next_row_id = self.next_row_id.saturating_add(num_records.max(0));
        base
    }
}

impl RowTrackingToken {
    /// Reserve a row-id range, or return `None` when the feature is unsupported or suspended.
    pub fn reserve_row_ids(&mut self, num_records: i64) -> Option<i64> {
        match self {
            RowTrackingToken::Enabled(t) => Some(t.reserve_row_ids(num_records)),
            RowTrackingToken::SupportedOnly(t) => Some(t.reserve_row_ids(num_records)),
            RowTrackingToken::Suspended | RowTrackingToken::Unsupported => None,
        }
    }

    /// Whether writers must stamp baseRowId / defaultRowCommitVersion on every add.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            RowTrackingToken::Enabled(_) | RowTrackingToken::SupportedOnly(_)
        )
    }

    /// Current high-water-mark (= next_row_id - 1) when row tracking is active.
    pub fn high_water_mark(&self) -> Option<i64> {
        match self {
            RowTrackingToken::Enabled(t) => active_high_water_mark(t.next_row_id),
            RowTrackingToken::SupportedOnly(t) => active_high_water_mark(t.next_row_id),
            _ => None,
        }
    }
}

fn active_high_water_mark(next_row_id: i64) -> Option<i64> {
    (next_row_id > 0).then_some(next_row_id - 1)
}

pub(crate) fn parse_row_tracking_high_water_mark(configuration: &str) -> DeltaResult<i64> {
    let configuration: Value = serde_json::from_str(configuration)?;
    let value = configuration
        .as_object()
        .and_then(|object| object.get("rowIdHighWaterMark"))
        .ok_or_else(|| {
            DeltaError::generic("delta.rowTracking domain metadata is missing rowIdHighWaterMark")
        })?;
    match value {
        Value::Number(number) => number.as_i64().ok_or_else(|| {
            DeltaError::generic("delta.rowTracking rowIdHighWaterMark must be representable as i64")
        }),
        Value::String(string) => string.parse::<i64>().map_err(|_| {
            DeltaError::generic("delta.rowTracking rowIdHighWaterMark must be an integer string")
        }),
        _ => Err(DeltaError::generic(
            "delta.rowTracking rowIdHighWaterMark must be a JSON number or string",
        )),
    }
}

pub fn enabled_row_tracking_materialized_column_names(
    snapshot: &DeltaSnapshot,
) -> DeltaResult<Option<RowTrackingMaterializedColumnNames>> {
    if !matches!(
        snapshot.get_row_tracking_state()?,
        RowTrackingToken::Enabled(_)
    ) {
        return Ok(None);
    }
    let config = snapshot.metadata().configuration();
    let row_id = config
        .get(ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME_KEY)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| {
            DeltaError::generic(format!(
                "{ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME_KEY} is required when delta.enableRowTracking = true"
            ))
        })?;
    let row_commit_version = config
        .get(ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME_KEY)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| {
            DeltaError::generic(format!(
                "{ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME_KEY} is required when delta.enableRowTracking = true"
            ))
        })?;
    Ok(Some(RowTrackingMaterializedColumnNames {
        row_id,
        row_commit_version,
    }))
}

pub(crate) fn require_reader_writer_feature(
    snapshot: &DeltaSnapshot,
    feature: &TableFeature,
    feature_name: &str,
) -> DeltaResult<()> {
    if snapshot.protocol().has_reader_feature(feature)
        && snapshot.protocol().has_writer_feature(feature)
    {
        Ok(())
    } else {
        Err(DeltaError::generic(format!(
            "table feature '{feature_name}' is not fully enabled on this table"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn high_water_mark_is_none_before_rows_are_assigned() {
        assert_eq!(
            RowTrackingToken::Enabled(EnabledRowTrackingToken { next_row_id: 0 }).high_water_mark(),
            None
        );
        assert_eq!(
            RowTrackingToken::SupportedOnly(SupportedRowTrackingToken { next_row_id: 0 })
                .high_water_mark(),
            None
        );
        assert_eq!(
            RowTrackingToken::Enabled(EnabledRowTrackingToken { next_row_id: 1 }).high_water_mark(),
            Some(0)
        );
    }

    #[test]
    fn parse_row_tracking_high_water_mark_accepts_numbers_and_strings() -> DeltaResult<()> {
        assert_eq!(
            parse_row_tracking_high_water_mark(r#"{"rowIdHighWaterMark":42}"#)?,
            42
        );
        assert_eq!(
            parse_row_tracking_high_water_mark(r#"{"rowIdHighWaterMark":"42"}"#)?,
            42
        );
        Ok(())
    }
}
