use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use pyo3::prelude::PyAnyMethods;
use pyo3::type_object::PyTypeInfo;
use pyo3::{PyResult, Python};
use serde::Deserialize;

use crate::metadata::{DuckLakeMetaStore, DuckLakeSnapshot, DuckLakeTable};
use crate::python::Modules;
use crate::spec::{
    ColumnInfo, DataFileIndex, FieldIndex, FileInfo, MappingIndex, PartitionId, SchemaInfo,
    SnapshotInfo, TableIndex, TableInfo,
};

#[derive(Deserialize)]
struct LoadTableResult {
    schema_info: PySchemaInfo,
    table_info: PyTableInfo,
    columns: Vec<PyColumnInfo>,
}

#[derive(Deserialize)]
struct PySchemaInfo {
    schema_id: u64,
    schema_uuid: String,
    begin_snapshot: Option<u64>,
    end_snapshot: Option<u64>,
    schema_name: String,
    path: String,
    path_is_relative: bool,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct PyColumnInfo {
    column_id: u64,
    begin_snapshot: Option<u64>,
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
    #[serde(default)]
    column_stats: Vec<PyColumnStatsInfo>,
    #[serde(default)]
    partition_values: Vec<PyFilePartitionInfo>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
}

fn parse_snapshot_time(s: &str) -> DataFusionResult<chrono::DateTime<chrono::Utc>> {
    // Try with timezone like "+08" then without colon.
    chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z")
        .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%z"))
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse snapshot_time: {}: {}", s, e)))
}

fn py_serialize_to_json<'py>(
    py: Python<'py>,
    obj: pyo3::Bound<'py, pyo3::PyAny>,
) -> PyResult<String> {
    let json = pyo3::types::PyModule::import(py, "json")?;
    json.getattr("dumps")?.call1((obj,))?.extract()
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
        let json: String = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let call: PyResult<String> = (|| {
                    let m = crate::python::Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m.getattr("load_table")?.call1((
                        url.as_str(),
                        table_name.as_str(),
                        schema_name.as_deref(),
                    ))?;
                    py_serialize_to_json(py, obj)
                })();
                match call {
                    Ok(s) => Ok(s),
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

        let parsed: LoadTableResult =
            serde_json::from_str(&json).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let schema_info = SchemaInfo {
            schema_id: crate::spec::SchemaIndex(parsed.schema_info.schema_id),
            schema_uuid: uuid::Uuid::parse_str(&parsed.schema_info.schema_uuid)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            begin_snapshot: parsed.schema_info.begin_snapshot,
            end_snapshot: parsed.schema_info.end_snapshot,
            schema_name: parsed.schema_info.schema_name,
            path: parsed.schema_info.path,
            path_is_relative: parsed.schema_info.path_is_relative,
        };

        let table_id = TableIndex(parsed.table_info.table_id);
        let table_info = TableInfo {
            table_id,
            table_uuid: uuid::Uuid::parse_str(&parsed.table_info.table_uuid)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            begin_snapshot: parsed.table_info.begin_snapshot,
            end_snapshot: parsed.table_info.end_snapshot,
            schema_id: crate::spec::SchemaIndex(parsed.table_info.schema_id),
            table_name: parsed.table_info.table_name,
            path: parsed.table_info.path,
            path_is_relative: parsed.table_info.path_is_relative,
            columns: vec![],
            inlined_data_tables: vec![],
        };

        let columns: Vec<ColumnInfo> = parsed
            .columns
            .into_iter()
            .map(|c| ColumnInfo {
                column_id: FieldIndex(c.column_id),
                begin_snapshot: c.begin_snapshot,
                end_snapshot: c.end_snapshot,
                table_id,
                column_order: c.column_order,
                column_name: c.column_name,
                column_type: c.column_type,
                initial_default: c.initial_default,
                default_value: c.default_value,
                nulls_allowed: c.nulls_allowed,
                parent_column: c.parent_column.map(FieldIndex),
            })
            .collect();

        Ok(DuckLakeTable {
            table_info,
            schema_info,
            columns,
        })
    }

    async fn current_snapshot(&self) -> DataFusionResult<DuckLakeSnapshot> {
        let url = self.url.clone();
        let json: String = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let call: PyResult<String> = (|| {
                    let m = Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m.getattr("current_snapshot")?.call1((url.as_str(),))?;
                    py_serialize_to_json(py, obj)
                })();
                match call {
                    Ok(s) => Ok(s),
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

        let p: PySnapshotInfo =
            serde_json::from_str(&json).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(DuckLakeSnapshot {
            snapshot: SnapshotInfo {
                snapshot_id: p.snapshot_id,
                snapshot_time: parse_snapshot_time(&p.snapshot_time)?,
                schema_version: p.schema_version,
                next_catalog_id: p.next_catalog_id,
                next_file_id: p.next_file_id,
                changes_made: p.changes_made,
                author: p.author,
                commit_message: p.commit_message,
                commit_extra_info: p.commit_extra_info,
            },
        })
    }

    async fn snapshot_by_id(&self, snapshot_id: u64) -> DataFusionResult<DuckLakeSnapshot> {
        let url = self.url.clone();
        let json: String = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let call: PyResult<String> = (|| {
                    let m = Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m
                        .getattr("snapshot_by_id")?
                        .call1((url.as_str(), snapshot_id))?;
                    py_serialize_to_json(py, obj)
                })();
                match call {
                    Ok(s) => Ok(s),
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

        let p: PySnapshotInfo =
            serde_json::from_str(&json).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(DuckLakeSnapshot {
            snapshot: SnapshotInfo {
                snapshot_id: p.snapshot_id,
                snapshot_time: parse_snapshot_time(&p.snapshot_time)?,
                schema_version: p.schema_version,
                next_catalog_id: p.next_catalog_id,
                next_file_id: p.next_file_id,
                changes_made: p.changes_made,
                author: p.author,
                commit_message: p.commit_message,
                commit_extra_info: p.commit_extra_info,
            },
        })
    }

    async fn list_data_files(
        &self,
        table_id: TableIndex,
        snapshot_id: Option<u64>,
    ) -> DataFusionResult<Vec<FileInfo>> {
        let url = self.url.clone();
        let json: String = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let call: PyResult<String> = (|| {
                    let m = Modules::DUCKLAKE_METADATA.load(py)?;
                    let obj = m.getattr("list_data_files")?.call1((
                        url.as_str(),
                        table_id.0,
                        snapshot_id,
                    ))?;
                    py_serialize_to_json(py, obj)
                })();
                match call {
                    Ok(s) => Ok(s),
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

        let rows: Vec<PyFileInfo> =
            serde_json::from_str(&json).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let out = rows
            .into_iter()
            .map(|r| FileInfo {
                data_file_id: DataFileIndex(r.data_file_id),
                table_id,
                begin_snapshot: r.begin_snapshot,
                end_snapshot: r.end_snapshot,
                file_order: r.file_order,
                path: r.path,
                path_is_relative: r.path_is_relative,
                file_format: r.file_format,
                record_count: r.record_count,
                file_size_bytes: r.file_size_bytes,
                footer_size: r.footer_size,
                row_id_start: r.row_id_start,
                partition_id: r.partition_id.map(PartitionId),
                encryption_key: r.encryption_key,
                partial_file_info: r.partial_file_info,
                mapping_id: MappingIndex(r.mapping_id),
                column_stats: r
                    .column_stats
                    .into_iter()
                    .map(|s| crate::spec::ColumnStatsInfo {
                        column_id: FieldIndex(s.column_id),
                        column_size_bytes: s.column_size_bytes,
                        value_count: s.value_count,
                        null_count: s.null_count,
                        min_value: s.min_value,
                        max_value: s.max_value,
                        contains_nan: s.contains_nan,
                        extra_stats: s.extra_stats,
                    })
                    .collect(),
                partition_values: r
                    .partition_values
                    .into_iter()
                    .map(|p| crate::spec::FilePartitionInfo {
                        partition_key_index: p.partition_key_index,
                        partition_value: p.partition_value,
                    })
                    .collect(),
            })
            .collect();
        Ok(out)
    }
}
