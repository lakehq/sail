use std::sync::Arc;

use sail_common_datafusion::catalog::TableHandle;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{CatalogError, CatalogResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableCommitFormat {
    Delta,
    Iceberg,
    Unknown,
}

impl TableCommitFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            TableCommitFormat::Delta => "delta",
            TableCommitFormat::Iceberg => "iceberg",
            TableCommitFormat::Unknown => "unknown",
        }
    }

    pub fn from_table_format_name(name: &str) -> Self {
        match name.to_ascii_lowercase().as_str() {
            "delta" => TableCommitFormat::Delta,
            "iceberg" => TableCommitFormat::Iceberg,
            _ => TableCommitFormat::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IcebergTableCommitPayload {
    pub requirements: Vec<Value>,
    pub updates: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCommitPayload {
    pub format: TableCommitFormat,
    pub payload: Value,
}

impl TableCommitPayload {
    pub fn new(format: TableCommitFormat, payload: Value) -> Self {
        Self { format, payload }
    }

    pub fn try_new<T: Serialize>(format: TableCommitFormat, payload: T) -> CatalogResult<Self> {
        let payload = serde_json::to_value(payload).map_err(|error| {
            CatalogError::InvalidArgument(format!(
                "failed to serialize {} table commit payload: {error}",
                format.as_str()
            ))
        })?;
        Ok(Self::new(format, payload))
    }

    pub fn deserialize<T: DeserializeOwned>(
        &self,
        expected_format: TableCommitFormat,
    ) -> CatalogResult<T> {
        if self.format != expected_format {
            return Err(CatalogError::InvalidArgument(format!(
                "expected {} table commit payload but found {}",
                expected_format.as_str(),
                self.format.as_str()
            )));
        }
        serde_json::from_value(self.payload.clone()).map_err(|error| {
            CatalogError::InvalidArgument(format!(
                "failed to deserialize {} table commit payload: {error}",
                expected_format.as_str()
            ))
        })
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

#[derive(Clone)]
pub struct TableCommit {
    table: TableHandle,
    committer: Arc<dyn TableCommitter>,
}

impl TableCommit {
    pub fn new(table: TableHandle, committer: Arc<dyn TableCommitter>) -> Self {
        Self { table, committer }
    }

    pub fn table(&self) -> &TableHandle {
        &self.table
    }

    pub fn format(&self) -> TableCommitFormat {
        self.committer.format()
    }

    pub async fn staging_location(&self) -> CatalogResult<Option<String>> {
        self.committer.staging_location().await
    }

    pub async fn commit(&self, payload: TableCommitPayload) -> CatalogResult<TableCommitOutcome> {
        self.committer.commit(payload).await
    }

    pub async fn abort(&self) -> CatalogResult<()> {
        self.committer.abort().await
    }
}

#[async_trait::async_trait]
pub trait TableCommitter: Send + Sync {
    fn format(&self) -> TableCommitFormat;

    // FIXME: This doesn't make sense. Simply place here as a POC for now.
    // Ideally we need something like `location`, `credentials`, "table_id", "is_staged", "commit_mode", etc.
    // Or maybe we need more stages like `prepare_write`.
    async fn staging_location(&self) -> CatalogResult<Option<String>> {
        Ok(None)
    }

    async fn commit(&self, payload: TableCommitPayload) -> CatalogResult<TableCommitOutcome>;

    async fn abort(&self) -> CatalogResult<()> {
        Ok(())
    }
}
