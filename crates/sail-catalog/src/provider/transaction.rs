use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::CatalogResult;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableCommitFormat {
    Delta,
    Iceberg,
    Unknown,
}

pub trait TableCommitPayload: Any + Debug + Send + Sync {
    fn format(&self) -> TableCommitFormat;

    fn as_any(&self) -> &dyn Any;
}

impl dyn TableCommitPayload {
    pub fn downcast_ref<T: TableCommitPayload + 'static>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCommitOutcome {
    pub committed_at_ms: Option<i64>,
    pub version: Option<i64>,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub metadata_location: Option<String>,
}

#[async_trait::async_trait]
pub trait TableCommitter: Send + Sync {
    // FIXME: This doesn't make sense. Simply place here as a POC for now.
    // Ideally we need something like `location`, `credentials`, "table_id", "is_staged", "commit_mode", etc.
    // Or maybe we need more stages like `prepare_write`.
    async fn staging_location(&self) -> CatalogResult<Option<String>> {
        Ok(None)
    }

    async fn commit(
        &self,
        payload: Arc<dyn TableCommitPayload>,
    ) -> CatalogResult<TableCommitOutcome>;

    async fn abort(&self) -> CatalogResult<()> {
        Ok(())
    }
}
