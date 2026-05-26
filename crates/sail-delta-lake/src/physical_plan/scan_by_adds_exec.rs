// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, Int64Builder, StructArray,
};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::ColumnStatistics;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result, Statistics};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::stream::{self, StreamExt, TryStreamExt};
use sail_common_datafusion::array::record_batch::cast_record_batch_relaxed_tz;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;
use url::Url;

use crate::datasource::scan::{FileScanParams, TableStatsMode};
use crate::datasource::{
    build_file_scan_config, df_logical_schema, is_metadata_struct_field, metadata_struct_fields,
    DeltaScanConfig, METADATA_COLUMN_NAME,
};
use crate::deletion_vector::DeletionVectorBitmap;
use crate::physical_plan::{
    decode_adds_from_batch, enabled_row_tracking_materialized_column_names, meta_adds, COL_ACTION,
};
use crate::schema::{arrow_field_physical_name, get_physical_schema};
use crate::session_extension::{load_table_uncached, DeltaTableCache};
use crate::spec::StructType;
use crate::storage::LogStoreRef;
use crate::table::{DeltaSnapshot, RowTrackingMaterializedColumnNames};

// TODO(dynamic-file-scheduling): Replace fixed file-count chunking with byte-aware chunking
// and optional work-stealing so executors pull remaining file work dynamically under skew.
const ADD_SCAN_CHUNK_FILES: usize = 1024;

struct BulkScanParams<'a> {
    snapshot: &'a DeltaSnapshot,
    log_store: &'a LogStoreRef,
    session_state: &'a dyn datafusion::catalog::Session,
    adds: &'a [crate::spec::Add],
    file_schema: SchemaRef,
    logical_names: &'a [String],
    partition_columns: &'a [String],
    materialized_columns: Option<&'a RowTrackingMaterializedColumnNames>,
    disable_row_filtering: bool,
}

struct ScanByAddsStreamState {
    input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    table_url: Url,
    table_version: i64,
    output_schema: SchemaRef,
    scan_config: DeltaScanConfig,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    pushdown_filter: Option<Arc<dyn PhysicalExpr>>,

    // Lazy init
    table_opened: bool,
    snapshot: Option<Arc<crate::table::DeltaSnapshot>>,
    log_store: Option<crate::storage::LogStoreRef>,
    session_state: Option<datafusion::execution::SessionState>,
    file_schema: Option<SchemaRef>,
    partition_columns: Option<Vec<String>>,
    logical_names: Option<Vec<String>>,
    metadata_column_name: Option<String>,

    // control
    partition_scan: Option<bool>,
    emitted_partition_empty: bool,
    pending_adds: Vec<crate::spec::Add>,
    current_scan: Option<SendableRecordBatchStream>,
    input_done: bool,
}

impl ScanByAddsStreamState {
    #[expect(clippy::too_many_arguments)]
    fn new(
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        table_url: Url,
        table_version: i64,
        output_schema: SchemaRef,
        scan_config: DeltaScanConfig,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            input,
            context,
            table_url,
            table_version,
            output_schema,
            scan_config,
            projection,
            limit,
            pushdown_filter,
            table_opened: false,
            snapshot: None,
            log_store: None,
            session_state: None,
            file_schema: None,
            partition_columns: None,
            logical_names: None,
            metadata_column_name: None,
            partition_scan: None,
            emitted_partition_empty: false,
            pending_adds: Vec::new(),
            current_scan: None,
            input_done: false,
        }
    }

    async fn ensure_table(&mut self) -> Result<()> {
        if self.table_opened {
            return Ok(());
        }
        // Prefer a session-scoped cache. This avoids leaking state across sessions / RuntimeEnvs.
        // If the cache extension is not installed, fall back to no caching.
        let cached = match self.context.as_ref().extension::<DeltaTableCache>() {
            Ok(cache) => {
                cache
                    .get(self.context.as_ref(), &self.table_url, self.table_version)
                    .await?
            }
            Err(_) => {
                load_table_uncached(
                    self.context.runtime_env(),
                    &self.table_url,
                    self.table_version,
                )
                .await?
            }
        };

        let snapshot_state = cached.snapshot.clone();
        snapshot_state
            .ensure_data_read_supported()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partition_columns = snapshot_state.metadata().partition_columns().clone();
        let session_state = SessionStateBuilder::new()
            .with_runtime_env(self.context.runtime_env().clone())
            .build();

        let mut scan_config = self.scan_config.clone();
        if scan_config.schema.is_none() {
            let schema = snapshot_state
                .input_schema()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            scan_config.schema = Some(schema);
        }

        let logical_schema = df_logical_schema(
            snapshot_state.as_ref(),
            &scan_config.file_column_name,
            &scan_config.row_index_column_name,
            &scan_config.commit_version_column_name,
            &scan_config.commit_timestamp_column_name,
            scan_config.schema.clone(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let logical_names = logical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let metadata_column_name = logical_schema
            .fields()
            .iter()
            .find(|field| is_metadata_struct_field(field))
            .map(|field| field.name().clone());

        let table_partition_cols = snapshot_state.metadata().partition_columns();
        let kmode = snapshot_state.effective_column_mapping_mode();
        let kschema_arc = snapshot_state.schema();
        let logical_kernel = StructType::try_from(kschema_arc)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let physical_arrow = get_physical_schema(&logical_kernel, kmode);
        let physical_partition_cols: std::collections::HashSet<String> = table_partition_cols
            .iter()
            .map(|col| {
                kschema_arc
                    .field_with_name(col)
                    .map(|f| arrow_field_physical_name(f, kmode).to_string())
                    .unwrap_or_else(|_| col.clone())
            })
            .collect();

        let file_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
            physical_arrow
                .fields()
                .iter()
                .filter(|f| !physical_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        self.log_store = Some(cached.log_store.clone());
        self.snapshot = Some(snapshot_state);
        self.session_state = Some(session_state);
        self.file_schema = Some(file_schema);
        self.partition_columns = Some(partition_columns);
        self.logical_names = Some(logical_names);
        self.metadata_column_name = metadata_column_name;
        self.scan_config = scan_config;
        self.table_opened = true;
        Ok(())
    }

    fn update_partition_scan_from_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let v = if let Some(scan_col) = batch.column_by_name("partition_scan") {
            let scan_array = scan_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("partition_scan column is not a BooleanArray".into())
                })?;
            scan_array.value(0)
        } else {
            false
        };
        self.partition_scan = Some(self.partition_scan.unwrap_or(true) && v);
        Ok(())
    }

    async fn build_next_scan(&mut self) -> Result<()> {
        if self.pending_adds.is_empty() {
            return Ok(());
        }
        self.ensure_table().await?;

        let snapshot = self
            .snapshot
            .as_deref()
            .ok_or_else(|| DataFusionError::Internal("missing snapshot".into()))?;
        let log_store = self
            .log_store
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing log_store".into()))?;
        let session_state = self
            .session_state
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing session_state".into()))?;
        let file_schema = self
            .file_schema
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing file_schema".into()))?
            .clone();
        let partition_columns = self
            .partition_columns
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing partition_columns".into()))?
            .clone();
        let logical_names = self
            .logical_names
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("missing logical_names".into()))?
            .clone();

        let wants_metadata = self.output_schema_has_row_tracking_metadata();
        let metadata_column_name = if wants_metadata {
            Some(self.row_tracking_metadata_column_name()?.to_string())
        } else {
            None
        };
        let materialized_columns = if wants_metadata {
            row_tracking_materialized_columns(snapshot)?
        } else {
            None
        };
        let metadata_file_schema = if wants_metadata {
            file_schema_with_materialized_columns(
                Arc::clone(&file_schema),
                materialized_columns.as_ref(),
            )
        } else {
            Arc::clone(&file_schema)
        };

        // Split adds: files that need file-local row positions go through per-file scans;
        // the rest use the bulk parallel scan.
        let adds = std::mem::take(&mut self.pending_adds);
        let mut all_streams: Vec<SendableRecordBatchStream> = Vec::new();
        let row_index_column = self
            .scan_config
            .row_index_column_name
            .clone()
            .filter(|name| self.output_schema.field_with_name(name).is_ok());
        if let Some(row_index_column) = row_index_column {
            let bitmaps: Vec<Option<DeletionVectorBitmap>> =
                if adds.iter().any(|add| add.deletion_vector.is_some()) {
                    let table_url = self.table_url.clone();
                    let object_store = self
                        .context
                        .runtime_env()
                        .object_store_registry
                        .get_store(&table_url)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let dv_futures = adds.iter().map(|add| {
                        let store = Arc::clone(&object_store);
                        let url = table_url.clone();
                        let dv_descriptor = add.deletion_vector.clone();
                        async move {
                            let Some(descriptor) = dv_descriptor else {
                                return Ok(None);
                            };
                            let bitmap = crate::deletion_vector::read_deletion_vector(
                                store.as_ref(),
                                &url,
                                &descriptor,
                            )
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                            Ok::<_, DataFusionError>(Some(bitmap))
                        }
                    });
                    futures::future::try_join_all(dv_futures).await?
                } else {
                    (0..adds.len()).map(|_| None).collect()
                };

            for (add, maybe_bitmap) in adds.iter().zip(bitmaps) {
                let file_streams = self.build_bulk_scan(BulkScanParams {
                    snapshot,
                    log_store,
                    session_state,
                    adds: std::slice::from_ref(add),
                    file_schema: metadata_file_schema.clone(),
                    logical_names: &logical_names,
                    partition_columns: &partition_columns,
                    materialized_columns: materialized_columns.as_ref(),
                    disable_row_filtering: maybe_bitmap.is_some(),
                })?;

                let maybe_bitmap = maybe_bitmap.map(Arc::new);
                for mut inner in file_streams {
                    if wants_metadata {
                        inner = row_tracking_metadata_stream(
                            inner,
                            add.base_row_id,
                            add.default_row_commit_version,
                            materialized_columns.clone(),
                            metadata_column_name
                                .as_deref()
                                .unwrap_or(METADATA_COLUMN_NAME),
                        );
                    }
                    let stream = append_row_index_stream(inner, row_index_column.clone())?;
                    if let Some(bitmap) = &maybe_bitmap {
                        all_streams.push(dv_filter_stream(stream, Arc::clone(bitmap)));
                    } else {
                        all_streams.push(stream);
                    }
                }
            }
            let output_schema = Arc::clone(&self.output_schema);
            let combined = stream::iter(all_streams)
                .map(Ok::<_, DataFusionError>)
                .try_flatten()
                .and_then(move |batch| {
                    let output_schema = Arc::clone(&output_schema);
                    async move {
                        let casted = cast_record_batch_relaxed_tz(&batch, &output_schema)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        Ok(casted)
                    }
                });
            self.current_scan = Some(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                combined,
            )));
            return Ok(());
        }
        let (per_file_adds, plain_adds): (Vec<_>, Vec<_>) = adds
            .into_iter()
            .partition(|a| a.deletion_vector.is_some() || wants_metadata);

        if !plain_adds.is_empty() {
            let streams = self.build_bulk_scan(BulkScanParams {
                snapshot,
                log_store,
                session_state,
                adds: &plain_adds,
                file_schema: file_schema.clone(),
                logical_names: &logical_names,
                partition_columns: &partition_columns,
                materialized_columns: None,
                disable_row_filtering: false,
            })?;
            all_streams.extend(streams);
        }

        if !per_file_adds.is_empty() {
            let table_url = self.table_url.clone();
            let object_store = self
                .context
                .runtime_env()
                .object_store_registry
                .get_store(&table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let bitmap_futures = per_file_adds.iter().map(|add| {
                let store = Arc::clone(&object_store);
                let url = table_url.clone();
                let dv_descriptor = add.deletion_vector.clone();
                async move {
                    match dv_descriptor {
                        Some(descriptor) => {
                            let bitmap = crate::deletion_vector::read_deletion_vector(
                                store.as_ref(),
                                &url,
                                &descriptor,
                            )
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                            Ok::<_, DataFusionError>(Some(bitmap))
                        }
                        None => Ok(None),
                    }
                }
            });
            let bitmaps = futures::future::try_join_all(bitmap_futures).await?;

            for (add, bitmap) in per_file_adds.iter().zip(bitmaps) {
                let file_streams = self.build_bulk_scan(BulkScanParams {
                    snapshot,
                    log_store,
                    session_state,
                    adds: std::slice::from_ref(add),
                    file_schema: metadata_file_schema.clone(),
                    logical_names: &logical_names,
                    partition_columns: &partition_columns,
                    materialized_columns: materialized_columns.as_ref(),
                    disable_row_filtering: bitmap.is_some(),
                })?;

                let bitmap = bitmap.map(Arc::new);
                for mut inner in file_streams {
                    if wants_metadata {
                        inner = row_tracking_metadata_stream(
                            inner,
                            add.base_row_id,
                            add.default_row_commit_version,
                            materialized_columns.clone(),
                            metadata_column_name
                                .as_deref()
                                .unwrap_or(METADATA_COLUMN_NAME),
                        );
                    }
                    if let Some(bitmap) = bitmap.as_ref() {
                        inner = dv_filter_stream(inner, Arc::clone(bitmap));
                    }
                    all_streams.push(inner);
                }
            }
        }

        let output_schema = Arc::clone(&self.output_schema);
        let combined = stream::iter(all_streams)
            .map(Ok::<_, DataFusionError>)
            .try_flatten()
            .and_then(move |batch| {
                let output_schema = Arc::clone(&output_schema);
                async move {
                    let casted = cast_record_batch_relaxed_tz(&batch, &output_schema)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Ok(casted)
                }
            });
        self.current_scan = Some(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            combined,
        )));
        Ok(())
    }

    /// Build a bulk Parquet scan for a set of Add actions (no DV filtering).
    fn output_schema_has_row_tracking_metadata(&self) -> bool {
        self.output_schema.fields().iter().any(|field| {
            is_metadata_struct_field(field)
                || self
                    .metadata_column_name
                    .as_ref()
                    .is_some_and(|name| field.name() == name)
        })
    }

    fn row_tracking_metadata_column_name(&self) -> Result<&str> {
        if let Some(field) = self
            .output_schema
            .fields()
            .iter()
            .find(|field| is_metadata_struct_field(field))
        {
            return Ok(field.name());
        }
        self.metadata_column_name
            .as_deref()
            .ok_or_else(|| DataFusionError::Internal("missing row tracking metadata column".into()))
    }

    /// `logical_names` with the `_metadata` entry removed.
    fn inner_logical_names(&self, logical_names: &[String]) -> Vec<String> {
        let metadata_column_name = self.metadata_column_name.as_deref();
        logical_names
            .iter()
            .filter(|name| Some(name.as_str()) != metadata_column_name)
            .cloned()
            .collect()
    }

    fn inner_logical_names_for_metadata_scan(
        &self,
        logical_names: &[String],
        partition_columns: &[String],
        materialized_columns: Option<&RowTrackingMaterializedColumnNames>,
    ) -> Vec<String> {
        let mut names = self.inner_logical_names(logical_names);
        if let Some(columns) = materialized_columns {
            let insert_at = names
                .iter()
                .position(|name| {
                    partition_columns.iter().any(|p| p == name)
                        || self.scan_config.file_column_name.as_ref() == Some(name)
                        || self.scan_config.commit_version_column_name.as_ref() == Some(name)
                        || self.scan_config.commit_timestamp_column_name.as_ref() == Some(name)
                })
                .unwrap_or(names.len());
            names.insert(insert_at, columns.row_id.clone());
            names.insert(insert_at + 1, columns.row_commit_version.clone());
        }
        names
    }

    fn inner_projection_for_metadata_scan(
        &self,
        logical_names: &[String],
        inner_logical_names: &[String],
        materialized_columns: Option<&RowTrackingMaterializedColumnNames>,
    ) -> Result<Option<Vec<usize>>> {
        metadata_scan_projection(
            self.projection.as_deref(),
            logical_names,
            inner_logical_names,
            self.metadata_column_name.as_deref(),
            materialized_columns,
        )
    }

    fn build_bulk_scan(
        &self,
        params: BulkScanParams<'_>,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let wants_metadata = self.output_schema_has_row_tracking_metadata();
        let inner_logical_names_vec;
        let inner_logical_names: &[String] = if wants_metadata {
            inner_logical_names_vec = self.inner_logical_names_for_metadata_scan(
                params.logical_names,
                params.partition_columns,
                params.materialized_columns,
            );
            &inner_logical_names_vec
        } else {
            params.logical_names
        };
        let inner_projection = if wants_metadata {
            self.inner_projection_for_metadata_scan(
                params.logical_names,
                inner_logical_names,
                params.materialized_columns,
            )?
        } else {
            self.projection.clone()
        };

        let mut scan_config = self.scan_config.clone();
        let row_index_idx = scan_config
            .row_index_column_name
            .as_ref()
            .and_then(|name| inner_logical_names.iter().position(|x| x == name));
        scan_config.row_index_column_name = None;

        let file_projection = match (inner_projection.as_ref(), row_index_idx) {
            (Some(projection), Some(row_index_idx)) => Some(
                projection
                    .iter()
                    .copied()
                    .filter(|idx| *idx != row_index_idx)
                    .collect::<Vec<_>>(),
            ),
            (Some(projection), None) => Some(projection.clone()),
            (None, _) => None,
        };
        let file_logical_names = match row_index_idx {
            Some(row_index_idx) => inner_logical_names
                .iter()
                .enumerate()
                .filter_map(|(idx, name)| (idx != row_index_idx).then_some(name.clone()))
                .collect::<Vec<_>>(),
            None => inner_logical_names.to_vec(),
        };
        let pushdown_filter = if wants_metadata || params.disable_row_filtering {
            None
        } else {
            self.pushdown_filter.clone()
        };

        let file_scan_config = build_file_scan_config(
            params.snapshot,
            params.log_store,
            params.adds,
            &scan_config,
            FileScanParams {
                pruning_mask: None,
                projection: file_projection.as_ref(),
                limit: self.limit,
                pushdown_filter,
                sort_order: None,
                table_stats_mode: TableStatsMode::AddsOnly,
            },
            params.session_state,
            params.file_schema,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let partitions = file_scan_config.file_groups.len().max(1);
        let scan_exec =
            datafusion::datasource::source::DataSourceExec::from_data_source(file_scan_config);
        let scan_exec = rename_projected_physical_plan(
            scan_exec,
            &file_logical_names,
            file_projection.as_ref(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut streams = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            streams.push(scan_exec.execute(partition, Arc::clone(&self.context))?);
        }
        Ok(streams)
    }

    async fn decode_adds_from_meta_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<Vec<crate::spec::Add>> {
        self.ensure_table().await?;
        let partition_columns = self
            .partition_columns
            .clone()
            .unwrap_or_else(|| meta_adds::infer_partition_columns_from_schema(&batch.schema()));
        meta_adds::decode_adds_from_meta_batch(batch, Some(&partition_columns))
    }
}

fn push_projection_index_once(projection: &mut Vec<usize>, index: usize) {
    if !projection.contains(&index) {
        projection.push(index);
    }
}

fn metadata_scan_projection(
    projection: Option<&[usize]>,
    logical_names: &[String],
    inner_logical_names: &[String],
    metadata_column_name: Option<&str>,
    materialized_columns: Option<&RowTrackingMaterializedColumnNames>,
) -> Result<Option<Vec<usize>>> {
    let Some(projection) = projection else {
        return Ok(None);
    };

    let mut inner_projection =
        Vec::with_capacity(projection.len() + usize::from(materialized_columns.is_some()) * 2);
    for index in projection {
        let name = logical_names.get(*index).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "projection index {index} is out of bounds for Delta logical schema"
            ))
        })?;
        if Some(name.as_str()) == metadata_column_name {
            continue;
        }
        let inner_index = inner_logical_names
            .iter()
            .position(|inner_name| inner_name == name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "projected Delta column '{name}' is missing from metadata scan schema"
                ))
            })?;
        push_projection_index_once(&mut inner_projection, inner_index);
    }

    if let Some(columns) = materialized_columns {
        for name in [&columns.row_id, &columns.row_commit_version] {
            let inner_index = inner_logical_names
                .iter()
                .position(|inner_name| inner_name == name)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Row Tracking materialized column '{name}' is missing from metadata scan schema"
                    ))
                })?;
            push_projection_index_once(&mut inner_projection, inner_index);
        }
    }

    Ok(Some(inner_projection))
}

/// Append a file-local row-index column to a single-file scan stream.
///
/// Callers must only use this for streams that read one physical file, so the counter
/// starts at zero for each file and matches Delta deletion-vector row positions.
fn append_row_index_stream(
    inner: SendableRecordBatchStream,
    column_name: String,
) -> Result<SendableRecordBatchStream> {
    let mut fields = inner.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        column_name,
        ArrowDataType::Int64,
        false,
    )));
    let output_schema = Arc::new(Schema::new(fields));
    let stream_schema = Arc::clone(&output_schema);

    let stream = stream::try_unfold((inner, 0_i64), move |(mut inner, mut row_offset)| {
        let schema = Arc::clone(&stream_schema);
        async move {
            match inner.try_next().await? {
                Some(batch) => {
                    let num_rows_i64 = i64::try_from(batch.num_rows()).map_err(|_| {
                        DataFusionError::Execution(
                            "record batch row count exceeds i64::MAX".to_string(),
                        )
                    })?;
                    let row_indices =
                        Int64Array::from_iter_values(row_offset..row_offset + num_rows_i64);
                    row_offset += num_rows_i64;

                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(row_indices) as ArrayRef);
                    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
                    Ok(Some((batch, (inner, row_offset))))
                }
                None => Ok(None),
            }
        }
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

/// Wrap a record-batch stream so that rows whose file-level indices appear in the
/// deletion vector bitmap are excluded.
///
/// The wrapper maintains a running `row_offset` across successive batches received
/// from the inner stream. For each batch it builds a boolean selection mask by
/// checking every global row index against the bitmap, then applies
/// `filter_record_batch` to drop deleted rows.  Batches where all rows are deleted
/// are silently skipped instead of emitting empty batches.
fn dv_filter_stream(
    inner: SendableRecordBatchStream,
    bitmap: Arc<DeletionVectorBitmap>,
) -> SendableRecordBatchStream {
    let schema = inner.schema();
    let filtered = stream::try_unfold(
        (inner, bitmap, 0u64),
        |(mut inner, bitmap, mut row_offset)| async move {
            loop {
                match inner.try_next().await? {
                    None => return Ok(None),
                    Some(batch) => {
                        let num_rows = batch.num_rows();
                        if num_rows == 0 {
                            continue;
                        }

                        // Build selection mask: true = keep, false = deleted.
                        let keep: BooleanBuffer = (0..num_rows)
                            .map(|i| !bitmap.contains(row_offset + i as u64))
                            .collect();
                        row_offset += num_rows as u64;

                        let keep_count = keep.count_set_bits();
                        if keep_count == 0 {
                            // Entire batch is deleted.
                            continue;
                        }
                        if keep_count == num_rows {
                            // Nothing deleted in this batch.
                            return Ok(Some((batch, (inner, bitmap, row_offset))));
                        }

                        let predicate = BooleanArray::new(keep, None);
                        let filtered_batch =
                            datafusion::arrow::compute::filter_record_batch(&batch, &predicate)?;
                        return Ok(Some((filtered_batch, (inner, bitmap, row_offset))));
                    }
                }
            }
        },
    );
    Box::pin(RecordBatchStreamAdapter::new(schema, filtered))
}

fn row_tracking_materialized_columns(
    snapshot: &DeltaSnapshot,
) -> Result<Option<RowTrackingMaterializedColumnNames>> {
    enabled_row_tracking_materialized_column_names(snapshot)
}

fn file_schema_with_materialized_columns(
    file_schema: SchemaRef,
    materialized_columns: Option<&RowTrackingMaterializedColumnNames>,
) -> SchemaRef {
    let Some(columns) = materialized_columns else {
        return file_schema;
    };
    let mut fields = file_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for name in [&columns.row_id, &columns.row_commit_version] {
        if !fields.iter().any(|field| field.name() == name) {
            fields.push(Field::new(name.clone(), ArrowDataType::Int64, true));
        }
    }
    Arc::new(Schema::new(fields))
}

fn generated_row_id_array(
    base_row_id: Option<i64>,
    row_offset: i64,
    num_rows: usize,
) -> Int64Array {
    match base_row_id {
        Some(base) => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                builder.append_value(base.saturating_add(row_offset).saturating_add(i as i64));
            }
            builder.finish()
        }
        None => Int64Array::new_null(num_rows),
    }
}

fn constant_i64_array(value: Option<i64>, num_rows: usize) -> Int64Array {
    match value {
        Some(value) => Int64Array::from(vec![value; num_rows]),
        None => Int64Array::new_null(num_rows),
    }
}

fn coalesce_materialized_i64(
    batch: &RecordBatch,
    materialized_name: Option<&str>,
    fallback: &Int64Array,
) -> Result<ArrayRef> {
    let Some(materialized_name) = materialized_name else {
        return Ok(Arc::new(fallback.clone()));
    };
    let Some(array) = batch.column_by_name(materialized_name) else {
        return Ok(Arc::new(fallback.clone()));
    };
    let materialized = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Row Tracking materialized column '{materialized_name}' must be Int64"
        ))
    })?;
    let mut builder = Int64Builder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if materialized.is_valid(row) {
            builder.append_value(materialized.value(row));
        } else if fallback.is_valid(row) {
            builder.append_value(fallback.value(row));
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Append a `_metadata` struct column with Delta Row Tracking metadata.
///
/// `row_id` and `row_commit_version` prefer materialized hidden Parquet columns when present,
/// and otherwise fall back to the Add-action defaults plus the physical file row index.
fn row_tracking_metadata_stream(
    inner: SendableRecordBatchStream,
    base_row_id: Option<i64>,
    default_row_commit_version: Option<i64>,
    materialized_columns: Option<RowTrackingMaterializedColumnNames>,
    metadata_column_name: &str,
) -> SendableRecordBatchStream {
    let inner_schema = inner.schema();
    let struct_fields = metadata_struct_fields();
    let metadata_field = Field::new(
        metadata_column_name,
        ArrowDataType::Struct(struct_fields.clone()),
        true,
    );
    let mut out_fields: Vec<Field> = inner_schema
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    out_fields.push(metadata_field);
    let out_schema = Arc::new(Schema::new(out_fields));
    let out_schema_stream = Arc::clone(&out_schema);

    let stream = stream::try_unfold((inner, 0i64), move |(mut inner, mut row_offset)| {
        let struct_fields = struct_fields.clone();
        let out_schema = Arc::clone(&out_schema_stream);
        let materialized_columns = materialized_columns.clone();
        async move {
            match inner.try_next().await? {
                None => Ok(None),
                Some(batch) => {
                    let num_rows = batch.num_rows();

                    let default_row_id = generated_row_id_array(base_row_id, row_offset, num_rows);
                    let base_row_id_array = constant_i64_array(base_row_id, num_rows);
                    let default_commit_version_array =
                        constant_i64_array(default_row_commit_version, num_rows);
                    let row_id_array = coalesce_materialized_i64(
                        &batch,
                        materialized_columns
                            .as_ref()
                            .map(|columns| columns.row_id.as_str()),
                        &default_row_id,
                    )?;
                    let commit_version_array = coalesce_materialized_i64(
                        &batch,
                        materialized_columns
                            .as_ref()
                            .map(|columns| columns.row_commit_version.as_str()),
                        &default_commit_version_array,
                    )?;
                    let struct_array = StructArray::new(
                        struct_fields,
                        vec![
                            row_id_array,
                            Arc::new(base_row_id_array) as ArrayRef,
                            Arc::new(default_commit_version_array) as ArrayRef,
                            commit_version_array,
                        ],
                        None,
                    );

                    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
                    columns.push(Arc::new(struct_array) as ArrayRef);
                    let out_batch = RecordBatch::try_new(out_schema, columns)?;

                    row_offset += num_rows as i64;
                    Ok(Some((out_batch, (inner, row_offset))))
                }
            }
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(out_schema, stream))
}

/// Physical execution node that scans Delta data files based on Add actions from upstream.
///
/// This node bridges the metadata layer (Add actions) with the data layer (Parquet scans).
/// It consumes a stream of encoded Add actions and produces a stream of data records by
/// scanning the referenced files.
#[derive(Debug, Clone)]
pub struct DeltaScanByAddsExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    version: i64,
    table_schema: SchemaRef,
    output_schema: SchemaRef,
    scan_config: DeltaScanConfig,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    statistics: Statistics,
    cache: Arc<PlanProperties>,
}

impl DeltaScanByAddsExec {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        version: i64,
        table_schema: SchemaRef,
        output_schema: SchemaRef,
        scan_config: DeltaScanConfig,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let statistics = Statistics::new_unknown(output_schema.as_ref());
        let cache = Self::compute_properties(
            output_schema.clone(),
            input.output_partitioning().partition_count(),
        );
        Self {
            input,
            table_url,
            version,
            table_schema,
            output_schema,
            scan_config,
            projection,
            limit,
            pushdown_filter,
            statistics,
            cache,
        }
    }

    pub fn with_table_statistics(mut self, table_statistics: Option<Statistics>) -> Self {
        self.statistics = table_statistics
            .as_ref()
            .map(|s| map_statistics_to_schema(s, &self.table_schema, &self.output_schema))
            .unwrap_or_else(|| Statistics::new_unknown(self.output_schema.as_ref()));
        self
    }

    pub fn with_output_statistics(mut self, output_statistics: Option<Statistics>) -> Self {
        self.statistics = output_statistics
            .as_ref()
            .map(|statistics| {
                if statistics.column_statistics.len() == self.output_schema.fields().len() {
                    sanitize_statistics_to_schema(statistics.clone(), &self.output_schema)
                } else {
                    map_statistics_to_schema(statistics, &self.table_schema, &self.output_schema)
                }
            })
            .unwrap_or_else(|| Statistics::new_unknown(self.output_schema.as_ref()));
        self
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn scan_config(&self) -> &DeltaScanConfig {
        &self.scan_config
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn pushdown_filter(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.pushdown_filter.as_ref()
    }

    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }

    fn compute_properties(schema: SchemaRef, partition_count: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count.max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }
}

#[async_trait]
impl ExecutionPlan for DeltaScanByAddsExec {
    fn name(&self) -> &'static str {
        "DeltaScanByAddsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaScanByAddsExec requires exactly one child");
        }
        let mut cloned = (*self).clone();
        cloned.input = children[0].clone();
        cloned.cache = Self::compute_properties(
            cloned.output_schema.clone(),
            cloned.input.output_partitioning().partition_count(),
        );
        Ok(Arc::new(cloned))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let table_url = self.table_url.clone();
        let table_version = self.version;
        let output_schema = self.schema();
        let scan_config = self.scan_config.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let pushdown_filter = self.pushdown_filter.clone();
        let state = ScanByAddsStreamState::new(
            input_stream,
            context,
            table_url,
            table_version,
            Arc::clone(&output_schema),
            scan_config,
            projection,
            limit,
            pushdown_filter,
        );

        let s = stream::try_unfold(state, |mut st| async move {
            loop {
                // Drain current scan stream first.
                if let Some(scan) = &mut st.current_scan {
                    match scan.try_next().await? {
                        Some(batch) => return Ok(Some((batch, st))),
                        None => {
                            st.current_scan = None;
                            continue;
                        }
                    }
                }

                // Partition-only scans: emit a single empty batch then stop.
                if st.partition_scan == Some(true) && !st.emitted_partition_empty {
                    st.emitted_partition_empty = true;
                    return Ok(Some((RecordBatch::new_empty(st.output_schema.clone()), st)));
                }
                if st.partition_scan == Some(true) && st.emitted_partition_empty {
                    return Ok(None);
                }

                // If we have enough pending adds (or input is done), start a scan.
                if !st.pending_adds.is_empty()
                    && (st.pending_adds.len() >= ADD_SCAN_CHUNK_FILES || st.input_done)
                {
                    st.build_next_scan().await?;
                    continue;
                }

                // Otherwise, pull more adds from upstream.
                match st.input.try_next().await? {
                    Some(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        st.update_partition_scan_from_batch(&batch)?;
                        if st.partition_scan == Some(true) {
                            continue;
                        }

                        if batch.column_by_name(COL_ACTION).is_some() {
                            st.pending_adds.extend(decode_adds_from_batch(&batch)?);
                        } else {
                            // Arrow-native metadata rows path (preferred for query).
                            let adds = st.decode_adds_from_meta_batch(&batch).await?;
                            st.pending_adds.extend(adds);
                        }
                        continue;
                    }
                    None => {
                        st.input_done = true;
                        // If input is done and we still have pending adds, start the final scan.
                        if !st.pending_adds.is_empty() {
                            st.build_next_scan().await?;
                            continue;
                        }
                        // No adds at all: emit a single empty batch.
                        if !st.emitted_partition_empty {
                            st.emitted_partition_empty = true;
                            return Ok(Some((
                                RecordBatch::new_empty(st.output_schema.clone()),
                                st,
                            )));
                        }
                        return Ok(None);
                    }
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, s)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_none() {
            Ok(self.statistics.clone())
        } else {
            Ok(Statistics::new_unknown(self.schema().as_ref()))
        }
    }
}

pub(crate) fn map_statistics_to_schema(
    statistics: &Statistics,
    source_schema: &SchemaRef,
    target_schema: &SchemaRef,
) -> Statistics {
    let column_statistics = target_schema
        .fields()
        .iter()
        .map(|field| {
            let mut column_statistics = source_schema
                .index_of(field.name())
                .ok()
                .and_then(|idx| statistics.column_statistics.get(idx).cloned())
                .unwrap_or_else(ColumnStatistics::new_unknown);
            sanitize_column_statistics_for_field(
                &mut column_statistics,
                field.name(),
                field.data_type(),
            );
            column_statistics
        })
        .collect();

    Statistics {
        num_rows: statistics.num_rows,
        total_byte_size: statistics.total_byte_size,
        column_statistics,
    }
}

fn sanitize_statistics_to_schema(mut statistics: Statistics, schema: &SchemaRef) -> Statistics {
    if statistics.column_statistics.len() != schema.fields().len() {
        return Statistics::new_unknown(schema.as_ref());
    }

    for (field, column_stats) in schema
        .fields()
        .iter()
        .zip(&mut statistics.column_statistics)
    {
        sanitize_column_statistics_for_field(column_stats, field.name(), field.data_type());
    }

    statistics
}

fn sanitize_column_statistics_for_field(
    column_stats: &mut ColumnStatistics,
    _column_name: &str,
    data_type: &datafusion::arrow::datatypes::DataType,
) {
    column_stats.min_value = sanitize_bound_for_type(&column_stats.min_value, data_type);
    column_stats.max_value = sanitize_bound_for_type(&column_stats.max_value, data_type);
}

fn sanitize_bound_for_type(
    bound: &datafusion::common::stats::Precision<datafusion::common::ScalarValue>,
    data_type: &datafusion::arrow::datatypes::DataType,
) -> datafusion::common::stats::Precision<datafusion::common::ScalarValue> {
    let sanitize_value = |value: &datafusion::common::ScalarValue| {
        if value.is_null() {
            return None;
        }
        if value.data_type() == *data_type {
            return Some(value.clone());
        }
        value
            .cast_to(data_type)
            .ok()
            .filter(|casted| !casted.is_null())
    };

    match bound {
        datafusion::common::stats::Precision::Exact(value) => sanitize_value(value)
            .map(datafusion::common::stats::Precision::Exact)
            .unwrap_or(datafusion::common::stats::Precision::Absent),
        datafusion::common::stats::Precision::Inexact(value) => sanitize_value(value)
            .map(datafusion::common::stats::Precision::Inexact)
            .unwrap_or(datafusion::common::stats::Precision::Absent),
        datafusion::common::stats::Precision::Absent => {
            datafusion::common::stats::Precision::Absent
        }
    }
}

impl DisplayAs for DeltaScanByAddsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaScanByAddsExec(table_path={}, version={}, projection={:?}, limit={:?}, pushdown={})",
                    self.table_url,
                    self.version,
                    self.projection,
                    self.limit,
                    self.pushdown_filter.is_some()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DeltaScanByAddsExec: table_path={}, version={}, projection={:?}, limit={:?}, pushdown={}",
                    self.table_url,
                    self.version,
                    self.projection,
                    self.limit,
                    self.pushdown_filter.is_some()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use url::Url;

    use super::{map_statistics_to_schema, metadata_scan_projection, DeltaScanByAddsExec};
    use crate::table::RowTrackingMaterializedColumnNames;

    #[test]
    fn metadata_scan_projection_keeps_user_projection_and_materialized_columns() -> Result<()> {
        let logical_names = ["id", "value", "_metadata", "part"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let inner_logical_names = [
            "id",
            "value",
            "_row-id-col-test",
            "_row-commit-version-col-test",
            "part",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let materialized_columns = RowTrackingMaterializedColumnNames {
            row_id: "_row-id-col-test".to_string(),
            row_commit_version: "_row-commit-version-col-test".to_string(),
        };

        let projection = metadata_scan_projection(
            Some(&[2]),
            &logical_names,
            &inner_logical_names,
            Some("_metadata"),
            Some(&materialized_columns),
        )?;
        assert_eq!(projection, Some(vec![2, 3]));

        let projection = metadata_scan_projection(
            Some(&[0, 2]),
            &logical_names,
            &inner_logical_names,
            Some("_metadata"),
            Some(&materialized_columns),
        )?;
        assert_eq!(projection, Some(vec![0, 2, 3]));
        Ok(())
    }

    #[test]
    fn test_map_statistics_to_schema_by_name() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
            Field::new("_virtual", DataType::Utf8, true),
        ]));

        let source_stats = Statistics {
            num_rows: Precision::Exact(42),
            total_byte_size: Precision::Exact(4096),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(9))),
                    min_value: Precision::Exact(ScalarValue::Null),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Exact(7),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(99))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Exact(11),
                    byte_size: Precision::Absent,
                },
            ],
        };

        let mapped = map_statistics_to_schema(&source_stats, &source_schema, &target_schema);
        assert_eq!(mapped.num_rows, Precision::Exact(42));
        assert_eq!(mapped.total_byte_size, Precision::Exact(4096));
        assert_eq!(mapped.column_statistics.len(), 3);

        // `b` lands first in target schema.
        assert_eq!(mapped.column_statistics[0].null_count, Precision::Exact(2));
        assert_eq!(
            mapped.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(10)))
        );

        // `a` lands second in target schema.
        assert_eq!(mapped.column_statistics[1].null_count, Precision::Exact(1));
        assert_eq!(mapped.column_statistics[1].min_value, Precision::Absent);
        assert_eq!(
            mapped.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(9)))
        );

        // Unknown column gets unknown stats.
        assert_eq!(mapped.column_statistics[2], ColumnStatistics::new_unknown());
    }

    #[test]
    fn test_scan_by_adds_exposes_known_statistics() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "action",
            DataType::Utf8,
            true,
        )]));

        let input = Arc::new(EmptyExec::new(input_schema));
        let table_stats = Statistics {
            num_rows: Precision::Exact(123),
            total_byte_size: Precision::Exact(2048),
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics {
                    null_count: Precision::Exact(4),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(88))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Exact(12),
                    byte_size: Precision::Absent,
                },
            ],
        };

        let table_url = Url::parse("file:///tmp/table").ok();
        assert!(table_url.is_some());
        let table_url = match table_url {
            Some(url) => url,
            None => return,
        };

        let scan = DeltaScanByAddsExec::new(
            input,
            table_url,
            1,
            table_schema,
            output_schema,
            crate::datasource::DeltaScanConfig::default(),
            None,
            None,
            None,
        )
        .with_table_statistics(Some(table_stats));

        let stats = scan.partition_statistics(None).ok();
        assert!(stats.is_some());
        let stats = match stats {
            Some(s) => s,
            None => return,
        };
        assert_eq!(stats.num_rows, Precision::Exact(123));
        assert_eq!(stats.column_statistics.len(), 1);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Exact(4));
    }

    #[test]
    fn test_scan_by_adds_accepts_output_statistics_directly() -> Result<()> {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "action",
            DataType::Utf8,
            true,
        )]));
        let input = Arc::new(EmptyExec::new(input_schema));
        let table_url =
            Url::parse("file:///tmp/table").map_err(|e| DataFusionError::External(Box::new(e)))?;

        let output_stats = Statistics {
            num_rows: Precision::Exact(88),
            total_byte_size: Precision::Exact(1024),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(6),
                max_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(2))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Exact(5),
                byte_size: Precision::Absent,
            }],
        };

        let scan = DeltaScanByAddsExec::new(
            input,
            table_url,
            1,
            table_schema,
            output_schema,
            crate::datasource::DeltaScanConfig::default(),
            None,
            None,
            None,
        )
        .with_output_statistics(Some(output_stats));

        let stats = scan.partition_statistics(None)?;
        assert_eq!(stats.num_rows, Precision::Exact(88));
        assert_eq!(stats.column_statistics.len(), 1);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Exact(6));
        Ok(())
    }
}
