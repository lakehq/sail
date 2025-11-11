mod py_impl;

use async_trait::async_trait;
use datafusion::common::Result as DataFusionResult;
pub use py_impl::PythonMetaStore;

use crate::spec::{ColumnInfo, FileInfo, SchemaInfo, SnapshotInfo, TableInfo};

#[derive(Debug, Clone)]
pub struct DuckLakeTable {
    pub table_info: TableInfo,
    pub schema_info: SchemaInfo,
    pub columns: Vec<ColumnInfo>,
    pub partition_fields: Vec<crate::spec::PartitionFieldInfo>,
}

#[derive(Debug, Clone)]
pub struct DuckLakeSnapshot {
    pub snapshot: SnapshotInfo,
}

#[async_trait]
pub trait DuckLakeMetaStore: Send + Sync {
    async fn load_table(
        &self,
        table_name: &str,
        schema_name: Option<&str>,
    ) -> DataFusionResult<DuckLakeTable>;

    async fn current_snapshot(&self) -> DataFusionResult<DuckLakeSnapshot>;

    async fn snapshot_by_id(&self, snapshot_id: u64) -> DataFusionResult<DuckLakeSnapshot>;

    async fn list_data_files(
        &self,
        table_id: crate::spec::TableIndex,
        snapshot_id: Option<u64>,
    ) -> DataFusionResult<Vec<FileInfo>>;

    async fn list_delete_files(
        &self,
        _table_id: crate::spec::TableIndex,
        _snapshot_id: Option<u64>,
    ) -> DataFusionResult<Vec<crate::spec::DeleteFileInfo>> {
        Ok(vec![])
    }
}
