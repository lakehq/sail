use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SendableRecordBatchStream;
use pyo3::prelude::PyAnyMethods;
use pyo3::type_object::PyTypeInfo;
use pyo3::{FromPyObject, PyResult, Python};
use serde::Deserialize;

use crate::datasource::arrow::build_arrow_field;
use crate::metadata::{
    file_info_schema, DuckLakeMetaStore, DuckLakeSnapshot, DuckLakeTable, ListDataFilesRequest,
};
use crate::python::Modules;
use crate::spec::{
    DataFileIndex, FieldIndex, FileInfo, MappingIndex, PartitionId, SchemaInfo, SnapshotInfo,
    TableIndex, TableInfo,
};

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct LoadTableResult {
    schema_info: PySchemaInfo,
    table_info: PyTableInfo,
    columns: Vec<PyColumnInfo>,
    partition_fields: Vec<PyPartitionField>,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PySchemaInfo {
    schema_id: u64,
    schema_uuid: String,
    begin_snapshot: Option<u64>,
    end_snapshot: Option<u64>,
    schema_name: String,
    path: String,
    path_is_relative: bool,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PyTableInfo {
    table_id: u64,
    table_uuid: String,
    begin_snapshot: Option<u64>,
    end_snapshot: Option<u64>,
    schema_id: u64,
    table_name: String,
    path: String,
    path_is_relative: bool,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PyColumnInfo {
    column_id: u64,
    #[allow(dead_code)]
    begin_snapshot: Option<u64>,
    #[allow(dead_code)]
    end_snapshot: Option<u64>,
    #[allow(dead_code)]
    table_id: u64,
    column_order: u64,
    column_name: String,
    column_type: String,
    initial_default: Option<String>,
    default_value: Option<String>,
    nulls_allowed: bool,
    parent_column: Option<u64>,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PyPartitionField {
    partition_key_index: u64,
    column_id: u64,
    transform: String,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PySnapshotInfo {
    snapshot_id: u64,
    snapshot_time: String,
    schema_version: u64,
    next_catalog_id: u64,
    next_file_id: u64,
    changes_made: Option<String>,
    author: Option<String>,
    commit_message: Option<String>,
    commit_extra_info: Option<String>,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PyFileInfo {
    data_file_id: u64,
    #[allow(dead_code)]
    table_id: u64,
    begin_snapshot: Option<u64>,
    end_snapshot: Option<u64>,
    file_order: u64,
    path: String,
    path_is_relative: bool,
    file_format: Option<String>,
    record_count: u64,
    file_size_bytes: u64,
    footer_size: Option<u64>,
    row_id_start: Option<u64>,
    partition_id: Option<u64>,
    encryption_key: String,
    partial_file_info: Option<String>,
    mapping_id: u64,
    column_stats: Vec<PyColumnStatsInfo>,
    partition_values: Vec<PyFilePartitionInfo>,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PyColumnStatsInfo {
    column_id: u64,
    column_size_bytes: Option<u64>,
    value_count: Option<u64>,
    null_count: Option<u64>,
    min_value: Option<String>,
    max_value: Option<String>,
    contains_nan: Option<bool>,
    extra_stats: Option<String>,
}

#[derive(Deserialize, FromPyObject)]
#[pyo3(from_item_all)]
struct PyFilePartitionInfo {
    partition_key_index: u64,
    partition_value: String,
}

pub struct PythonMetaStore {
    url: String,
}

impl PythonMetaStore {
    pub async fn new(url: &str) -> DataFusionResult<Self> {
        Ok(Self {
            url: url.to_string(),
        })
    }

    pub fn new_sync(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

fn parse_snapshot_time(s: &str) -> DataFusionResult<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z")
        .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%z"))
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse snapshot_time: {}: {}", s, e)))
}

impl TryFrom<PySchemaInfo> for SchemaInfo {
    type Error = DataFusionError;

    fn try_from(p: PySchemaInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            schema_id: crate::spec::SchemaIndex(p.schema_id),
            schema_uuid: uuid::Uuid::parse_str(&p.schema_uuid)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            begin_snapshot: p.begin_snapshot,
            end_snapshot: p.end_snapshot,
            schema_name: p.schema_name,
            path: p.path,
            path_is_relative: p.path_is_relative,
        })
    }
}

impl TryFrom<PyTableInfo> for TableInfo {
    type Error = DataFusionError;

    fn try_from(p: PyTableInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            table_id: TableIndex(p.table_id),
            table_uuid: uuid::Uuid::parse_str(&p.table_uuid)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            begin_snapshot: p.begin_snapshot,
            end_snapshot: p.end_snapshot,
            schema_id: crate::spec::SchemaIndex(p.schema_id),
            table_name: p.table_name,
            path: p.path,
            path_is_relative: p.path_is_relative,
            inlined_data_tables: vec![],
        })
    }
}

impl PyColumnInfo {
    fn into_arrow_field(self) -> DataFusionResult<Field> {
        build_arrow_field(
            &self.column_name,
            &self.column_type,
            self.nulls_allowed,
            FieldIndex(self.column_id),
            self.column_order,
            self.default_value.as_deref(),
            self.initial_default.as_deref(),
        )
    }
}

impl From<PyPartitionField> for crate::spec::PartitionFieldInfo {
    fn from(p: PyPartitionField) -> Self {
        Self {
            partition_key_index: p.partition_key_index,
            column_id: FieldIndex(p.column_id),
            transform: p.transform,
        }
    }
}

impl TryFrom<PySnapshotInfo> for SnapshotInfo {
    type Error = DataFusionError;

    fn try_from(p: PySnapshotInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            snapshot_id: p.snapshot_id,
            snapshot_time: parse_snapshot_time(&p.snapshot_time)?,
            schema_version: p.schema_version,
            next_catalog_id: p.next_catalog_id,
            next_file_id: p.next_file_id,
            changes_made: p.changes_made,
            author: p.author,
            commit_message: p.commit_message,
            commit_extra_info: p.commit_extra_info,
        })
    }
}

impl From<PyColumnStatsInfo> for crate::spec::ColumnStatsInfo {
    fn from(s: PyColumnStatsInfo) -> Self {
        Self {
            column_id: FieldIndex(s.column_id),
            column_size_bytes: s.column_size_bytes,
            value_count: s.value_count,
            null_count: s.null_count,
            min_value: s.min_value,
            max_value: s.max_value,
            contains_nan: s.contains_nan,
            extra_stats: s.extra_stats,
        }
    }
}

impl From<PyFilePartitionInfo> for crate::spec::FilePartitionInfo {
    fn from(p: PyFilePartitionInfo) -> Self {
        Self {
            partition_key_index: p.partition_key_index,
            partition_value: p.partition_value,
        }
    }
}

impl PyFileInfo {
    fn into_file_info(self, table_id: TableIndex) -> FileInfo {
        FileInfo {
            data_file_id: DataFileIndex(self.data_file_id),
            table_id,
            begin_snapshot: self.begin_snapshot,
            end_snapshot: self.end_snapshot,
            file_order: self.file_order,
            path: self.path,
            path_is_relative: self.path_is_relative,
            file_format: self.file_format,
            record_count: self.record_count,
            file_size_bytes: self.file_size_bytes,
            footer_size: self.footer_size,
            row_id_start: self.row_id_start,
            partition_id: self.partition_id.map(PartitionId),
            encryption_key: self.encryption_key,
            partial_file_info: self.partial_file_info,
            mapping_id: MappingIndex(self.mapping_id),
            column_stats: self.column_stats.into_iter().map(Into::into).collect(),
            partition_values: self.partition_values.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl DuckLakeMetaStore for PythonMetaStore {
    async fn load_table(
        &self,
        table_name: &str,
        schema_name: Option<&str>,
    ) -> DataFusionResult<DuckLakeTable> {
        let url = self.url.clone();
        let table_name = table_name.to_string();
        let schema_name = schema_name.map(|s| s.to_string());
        let parsed: LoadTableResult = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let call: PyResult<LoadTableResult> = (|| {
                    let m = crate::python::Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m.getattr("load_table")?.call1((
                        url.as_str(),
                        table_name.as_str(),
                        schema_name.as_deref(),
                    ))?;
                    obj.extract()
                })();
                match call {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        let is_value_error = e
                            .matches(py, pyo3::exceptions::PyValueError::type_object(py))
                            .unwrap_or(false);
                        if is_value_error {
                            let msg = e.to_string();
                            Err(DataFusionError::Plan(msg))
                        } else {
                            Err(DataFusionError::External(Box::new(e)))
                        }
                    }
                }
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;

        let schema_info: SchemaInfo = parsed.schema_info.try_into()?;
        let table_info: TableInfo = parsed.table_info.try_into()?;
        let fields: Vec<Field> = parsed
            .columns
            .into_iter()
            .filter(|c| c.parent_column.is_none())
            .map(PyColumnInfo::into_arrow_field)
            .collect::<DataFusionResult<_>>()?;
        let schema = Arc::new(Schema::new(fields));

        Ok(DuckLakeTable {
            table_info,
            schema_info,
            schema,
            partition_fields: parsed
                .partition_fields
                .into_iter()
                .map(Into::into)
                .collect(),
        })
    }

    async fn current_snapshot(&self) -> DataFusionResult<DuckLakeSnapshot> {
        let url = self.url.clone();
        let p: PySnapshotInfo = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let call: PyResult<PySnapshotInfo> = (|| {
                    let m = Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m.getattr("current_snapshot")?.call1((url.as_str(),))?;
                    obj.extract()
                })();
                match call {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        let is_value_error = e
                            .matches(py, pyo3::exceptions::PyValueError::type_object(py))
                            .unwrap_or(false);
                        if is_value_error {
                            let msg = e.to_string();
                            Err(DataFusionError::Plan(msg))
                        } else {
                            Err(DataFusionError::External(Box::new(e)))
                        }
                    }
                }
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;

        Ok(DuckLakeSnapshot {
            snapshot: p.try_into()?,
        })
    }

    async fn snapshot_by_id(&self, snapshot_id: u64) -> DataFusionResult<DuckLakeSnapshot> {
        let url = self.url.clone();
        let p: PySnapshotInfo = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let call: PyResult<PySnapshotInfo> = (|| {
                    let m = Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m
                        .getattr("snapshot_by_id")?
                        .call1((url.as_str(), snapshot_id))?;
                    obj.extract()
                })();
                match call {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        let is_value_error = e
                            .matches(py, pyo3::exceptions::PyValueError::type_object(py))
                            .unwrap_or(false);
                        if is_value_error {
                            Err(DataFusionError::Plan(e.to_string()))
                        } else {
                            Err(DataFusionError::External(Box::new(e)))
                        }
                    }
                }
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;

        Ok(DuckLakeSnapshot {
            snapshot: p.try_into()?,
        })
    }

    async fn list_data_files(
        &self,
        request: ListDataFilesRequest,
    ) -> DataFusionResult<Vec<FileInfo>> {
        // TODO: also surface associated ducklake_delete_file rows so readers can apply row-level deletes
        let table_id = request.table_id;
        let snapshot_id = request.snapshot_id;
        let url = self.url.clone();
        let py_partition_filters: Option<Vec<(u64, Vec<String>)>> =
            request.partition_filters.map(|filters| {
                filters
                    .into_iter()
                    .map(|f| (f.partition_key_index, f.values))
                    .collect()
            });
        let required_column_ids: Option<Vec<u64>> = request
            .required_column_stats
            .map(|cols| cols.into_iter().map(|field| field.0).collect());
        let rows: Vec<PyFileInfo> = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let call: PyResult<Vec<PyFileInfo>> = (|| {
                    let m = Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m.getattr("list_data_files")?.call1((
                        url.as_str(),
                        table_id.0,
                        snapshot_id,
                        py_partition_filters,
                        required_column_ids,
                    ))?;
                    obj.extract()
                })();
                match call {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        let is_value_error = e
                            .matches(py, pyo3::exceptions::PyValueError::type_object(py))
                            .unwrap_or(false);
                        if is_value_error {
                            Err(DataFusionError::Plan(e.to_string()))
                        } else {
                            Err(DataFusionError::External(Box::new(e)))
                        }
                    }
                }
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;

        let out = rows
            .into_iter()
            .map(|r| r.into_file_info(table_id))
            .collect();
        Ok(out)
    }

    fn scan_data_files(
        &self,
        request: ListDataFilesRequest,
        batch_size: usize,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // TODO: also surface associated ducklake_delete_file rows so readers can apply row-level deletes
        let table_id = request.table_id;
        let snapshot_id = request.snapshot_id;
        let url = self.url.clone();
        let py_partition_filters: Option<Vec<(u64, Vec<String>)>> =
            request.partition_filters.map(|filters| {
                filters
                    .into_iter()
                    .map(|f| (f.partition_key_index, f.values))
                    .collect()
            });
        let required_column_ids: Option<Vec<u64>> = request
            .required_column_stats
            .map(|cols| cols.into_iter().map(|field| field.0).collect());
        let schema = file_info_schema()?;

        super::file_info_stream::PyFileInfoStream::scan_data_files_arrow(
            schema,
            url,
            table_id.0,
            snapshot_id,
            py_partition_filters,
            required_column_ids,
            batch_size,
        )
    }
}
