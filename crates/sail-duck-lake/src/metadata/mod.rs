mod py_impl;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::Result as DataFusionResult;
pub use py_impl::PythonMetaStore;

use crate::spec::{FieldIndex, FileInfo, PartitionFilter, SchemaInfo, SnapshotInfo, TableInfo};

#[derive(Debug, Clone)]
pub struct DuckLakeTable {
    pub table_info: TableInfo,
    pub schema_info: SchemaInfo,
    pub schema: ArrowSchemaRef,
    pub partition_fields: Vec<crate::spec::PartitionFieldInfo>,
}

#[derive(Debug, Clone)]
pub struct DuckLakeSnapshot {
    pub snapshot: SnapshotInfo,
}

#[derive(Debug, Clone)]
pub struct ListDataFilesRequest {
    pub table_id: crate::spec::TableIndex,
    pub snapshot_id: Option<u64>,
    pub partition_filters: Option<Vec<PartitionFilter>>,
    pub required_column_stats: Option<Vec<FieldIndex>>,
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

    // TODO: Add paginated or streaming metadata APIs for listing data files.
    async fn list_data_files(
        &self,
        request: ListDataFilesRequest,
    ) -> DataFusionResult<Vec<FileInfo>>;

    async fn list_delete_files(
        &self,
        _table_id: crate::spec::TableIndex,
        _snapshot_id: Option<u64>,
    ) -> DataFusionResult<Vec<crate::spec::DeleteFileInfo>> {
        // TODO: wire through delete files so reads can apply row-level deletes
        Ok(vec![])
    }
}
