use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};

use crate::metadata::{DuckLakeMetaStore, DuckLakeSnapshot, DuckLakeTable};
use crate::schema::*;
use crate::spec::{
    ColumnInfo, DataFileIndex, FieldIndex, FileInfo, MappingIndex, PartitionId, SchemaIndex,
    SchemaInfo, SnapshotInfo, TableIndex, TableInfo,
};

type SqlitePool = Pool<ConnectionManager<SqliteConnection>>;

pub struct DieselMetaStore {
    pool: SqlitePool,
}

impl DieselMetaStore {
    pub async fn new_sqlite(url: &str) -> Result<Self, DataFusionError> {
        let url = url.strip_prefix("sqlite://").unwrap_or(url);
        let manager = ConnectionManager::<SqliteConnection>::new(url);
        let pool = Pool::builder()
            .build(manager)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl DuckLakeMetaStore for DieselMetaStore {
    async fn load_table(
        &self,
        table_name: &str,
        schema_name: Option<&str>,
    ) -> DataFusionResult<DuckLakeTable> {
        let schema_name = schema_name.unwrap_or("main");
        let pool = self.pool.clone();
        let table_name = table_name.to_string();
        let schema_name_owned = schema_name.to_string();

        tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let schema_row: (i64, String, Option<i64>, Option<i64>, String, String, bool) =
                ducklake_schema::table
                    .filter(ducklake_schema::schema_name.eq(&schema_name_owned))
                    .filter(ducklake_schema::end_snapshot.is_null())
                    .select((
                        ducklake_schema::schema_id,
                        ducklake_schema::schema_uuid,
                        ducklake_schema::begin_snapshot,
                        ducklake_schema::end_snapshot,
                        ducklake_schema::schema_name,
                        ducklake_schema::path,
                        ducklake_schema::path_is_relative,
                    ))
                    .first(&mut conn)
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "Schema not found: {}: {}",
                            schema_name_owned, e
                        ))
                    })?;

            let schema_id = SchemaIndex(schema_row.0 as u64);
            let schema_info = SchemaInfo {
                schema_id,
                schema_uuid: uuid::Uuid::parse_str(&schema_row.1)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
                begin_snapshot: schema_row.2.map(|v| v as u64),
                end_snapshot: schema_row.3.map(|v| v as u64),
                schema_name: schema_row.4,
                path: schema_row.5,
                path_is_relative: schema_row.6,
            };

            let table_row: (
                i64,
                String,
                Option<i64>,
                Option<i64>,
                i64,
                String,
                String,
                bool,
            ) = ducklake_table::table
                .filter(ducklake_table::table_name.eq(&table_name))
                .filter(ducklake_table::schema_id.eq(schema_id.0 as i64))
                .filter(ducklake_table::end_snapshot.is_null())
                .select((
                    ducklake_table::table_id,
                    ducklake_table::table_uuid,
                    ducklake_table::begin_snapshot,
                    ducklake_table::end_snapshot,
                    ducklake_table::schema_id,
                    ducklake_table::table_name,
                    ducklake_table::path,
                    ducklake_table::path_is_relative,
                ))
                .first(&mut conn)
                .map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Table not found: {}.{}: {}",
                        schema_name_owned, table_name, e
                    ))
                })?;

            let table_id = TableIndex(table_row.0 as u64);
            let table_info = TableInfo {
                table_id,
                table_uuid: uuid::Uuid::parse_str(&table_row.1)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
                begin_snapshot: table_row.2.map(|v| v as u64),
                end_snapshot: table_row.3.map(|v| v as u64),
                schema_id,
                table_name: table_row.5,
                path: table_row.6,
                path_is_relative: table_row.7,
                columns: vec![],
                inlined_data_tables: vec![],
            };

            let column_rows: Vec<(
                i64,
                Option<i64>,
                Option<i64>,
                i64,
                i64,
                String,
                String,
                Option<String>,
                Option<String>,
                bool,
                Option<i64>,
            )> = ducklake_column::table
                .filter(ducklake_column::table_id.eq(table_id.0 as i64))
                .filter(ducklake_column::end_snapshot.is_null())
                .order(ducklake_column::column_order.asc())
                .select((
                    ducklake_column::column_id,
                    ducklake_column::begin_snapshot,
                    ducklake_column::end_snapshot,
                    ducklake_column::table_id,
                    ducklake_column::column_order,
                    ducklake_column::column_name,
                    ducklake_column::column_type,
                    ducklake_column::initial_default,
                    ducklake_column::default_value,
                    ducklake_column::nulls_allowed,
                    ducklake_column::parent_column,
                ))
                .load(&mut conn)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let columns = column_rows
                .into_iter()
                .map(|row| ColumnInfo {
                    column_id: FieldIndex(row.0 as u64),
                    begin_snapshot: row.1.map(|v| v as u64),
                    end_snapshot: row.2.map(|v| v as u64),
                    table_id,
                    column_order: row.4 as u64,
                    column_name: row.5,
                    column_type: row.6,
                    initial_default: row.7,
                    default_value: row.8,
                    nulls_allowed: row.9,
                    parent_column: row.10.map(|v| FieldIndex(v as u64)),
                })
                .collect();

            Ok(DuckLakeTable {
                table_info,
                schema_info,
                columns,
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    }

    async fn current_snapshot(&self) -> DataFusionResult<DuckLakeSnapshot> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let row: (
                i64,
                chrono::NaiveDateTime,
                i64,
                i64,
                i64,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
            ) = ducklake_snapshot::table
                .order(ducklake_snapshot::snapshot_id.desc())
                .select((
                    ducklake_snapshot::snapshot_id,
                    ducklake_snapshot::snapshot_time,
                    ducklake_snapshot::schema_version,
                    ducklake_snapshot::next_catalog_id,
                    ducklake_snapshot::next_file_id,
                    ducklake_snapshot::changes_made,
                    ducklake_snapshot::author,
                    ducklake_snapshot::commit_message,
                    ducklake_snapshot::commit_extra_info,
                ))
                .first(&mut conn)
                .map_err(|e| {
                    DataFusionError::Plan(format!("No snapshots found in metadata: {}", e))
                })?;

            Ok(DuckLakeSnapshot {
                snapshot: SnapshotInfo {
                    snapshot_id: row.0 as u64,
                    snapshot_time: chrono::DateTime::from_naive_utc_and_offset(row.1, chrono::Utc),
                    schema_version: row.2 as u64,
                    next_catalog_id: row.3 as u64,
                    next_file_id: row.4 as u64,
                    changes_made: row.5,
                    author: row.6,
                    commit_message: row.7,
                    commit_extra_info: row.8,
                },
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    }

    async fn snapshot_by_id(&self, snapshot_id: u64) -> DataFusionResult<DuckLakeSnapshot> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let row: (
                i64,
                chrono::NaiveDateTime,
                i64,
                i64,
                i64,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
            ) = ducklake_snapshot::table
                .filter(ducklake_snapshot::snapshot_id.eq(snapshot_id as i64))
                .select((
                    ducklake_snapshot::snapshot_id,
                    ducklake_snapshot::snapshot_time,
                    ducklake_snapshot::schema_version,
                    ducklake_snapshot::next_catalog_id,
                    ducklake_snapshot::next_file_id,
                    ducklake_snapshot::changes_made,
                    ducklake_snapshot::author,
                    ducklake_snapshot::commit_message,
                    ducklake_snapshot::commit_extra_info,
                ))
                .first(&mut conn)
                .map_err(|e| {
                    DataFusionError::Plan(format!("Snapshot not found: {}: {}", snapshot_id, e))
                })?;

            Ok(DuckLakeSnapshot {
                snapshot: SnapshotInfo {
                    snapshot_id: row.0 as u64,
                    snapshot_time: chrono::DateTime::from_naive_utc_and_offset(row.1, chrono::Utc),
                    schema_version: row.2 as u64,
                    next_catalog_id: row.3 as u64,
                    next_file_id: row.4 as u64,
                    changes_made: row.5,
                    author: row.6,
                    commit_message: row.7,
                    commit_extra_info: row.8,
                },
            })
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    }

    async fn list_data_files(
        &self,
        table_id: TableIndex,
        snapshot_id: Option<u64>,
    ) -> DataFusionResult<Vec<FileInfo>> {
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || {
            let mut conn = pool
                .get()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let rows: Vec<(
                i64,
                i64,
                Option<i64>,
                Option<i64>,
                i64,
                String,
                bool,
                String,
                i64,
                i64,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<String>,
                Option<String>,
                i64,
            )> = if let Some(snap_id) = snapshot_id {
                ducklake_data_file::table
                    .filter(ducklake_data_file::table_id.eq(table_id.0 as i64))
                    .filter(ducklake_data_file::begin_snapshot.le(snap_id as i64))
                    .filter(
                        ducklake_data_file::end_snapshot
                            .is_null()
                            .or(ducklake_data_file::end_snapshot.gt(snap_id as i64)),
                    )
                    .order(ducklake_data_file::file_order.asc())
                    .select((
                        ducklake_data_file::data_file_id,
                        ducklake_data_file::table_id,
                        ducklake_data_file::begin_snapshot,
                        ducklake_data_file::end_snapshot,
                        ducklake_data_file::file_order,
                        ducklake_data_file::path,
                        ducklake_data_file::path_is_relative,
                        ducklake_data_file::file_format,
                        ducklake_data_file::record_count,
                        ducklake_data_file::file_size_bytes,
                        ducklake_data_file::footer_size,
                        ducklake_data_file::row_id_start,
                        ducklake_data_file::partition_id,
                        ducklake_data_file::encryption_key,
                        ducklake_data_file::partial_file_info,
                        ducklake_data_file::mapping_id,
                    ))
                    .load(&mut conn)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                ducklake_data_file::table
                    .filter(ducklake_data_file::table_id.eq(table_id.0 as i64))
                    .filter(ducklake_data_file::end_snapshot.is_null())
                    .order(ducklake_data_file::file_order.asc())
                    .select((
                        ducklake_data_file::data_file_id,
                        ducklake_data_file::table_id,
                        ducklake_data_file::begin_snapshot,
                        ducklake_data_file::end_snapshot,
                        ducklake_data_file::file_order,
                        ducklake_data_file::path,
                        ducklake_data_file::path_is_relative,
                        ducklake_data_file::file_format,
                        ducklake_data_file::record_count,
                        ducklake_data_file::file_size_bytes,
                        ducklake_data_file::footer_size,
                        ducklake_data_file::row_id_start,
                        ducklake_data_file::partition_id,
                        ducklake_data_file::encryption_key,
                        ducklake_data_file::partial_file_info,
                        ducklake_data_file::mapping_id,
                    ))
                    .load(&mut conn)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            let files = rows
                .into_iter()
                .map(|row| FileInfo {
                    data_file_id: DataFileIndex(row.0 as u64),
                    table_id,
                    begin_snapshot: row.2.map(|v| v as u64),
                    end_snapshot: row.3.map(|v| v as u64),
                    file_order: row.4 as u64,
                    path: row.5,
                    path_is_relative: row.6,
                    file_format: Some(row.7),
                    record_count: row.8 as u64,
                    file_size_bytes: row.9 as u64,
                    footer_size: row.10.map(|v| v as u64),
                    row_id_start: row.11.map(|v| v as u64),
                    partition_id: row.12.map(|v| PartitionId(v as u64)),
                    encryption_key: row.13.unwrap_or_default(),
                    partial_file_info: row.14,
                    mapping_id: MappingIndex(row.15 as u64),
                    column_stats: vec![],
                    partition_values: vec![],
                })
                .collect();

            Ok(files)
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    }
}
