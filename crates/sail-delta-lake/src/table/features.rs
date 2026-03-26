use super::DeltaSnapshot;
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

impl EnabledRowTrackingToken {
    pub fn assign_next_row_id(&mut self) -> i64 {
        let id = self.next_row_id;
        self.next_row_id = self.next_row_id.saturating_add(1);
        id
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
