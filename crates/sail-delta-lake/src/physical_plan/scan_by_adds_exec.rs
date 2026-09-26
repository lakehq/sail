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

use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::statistics::StatisticsArgs;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, apply_expression_roots, execute_stream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, ScalarValue, Statistics, internal_err};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::stream::{self, StreamExt, TryStreamExt};
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use url::Url;

use crate::datasource::scan::{
    FileScanParams, TableStatsMode, file_scan_projection_for_schema, map_statistics_to_schema,
    sanitize_statistics_for_schema,
};
use crate::datasource::{DeltaScanConfig, PATH_COLUMN, build_file_scan_config};
use crate::delta_log::LogStoreRef;
use crate::physical_plan::{COL_ACTION, DeltaDecodePath, decode_adds_from_batch, meta_adds};
use crate::schema::{arrow_field_physical_name, get_physical_schema, restore_logical_record_batch};
use crate::session_extension::{DeltaTableCache, load_table_uncached, load_table_with_config};
use crate::snapshot::{CatalogManagedCommitSet, DeltaSnapshotConfig, GroupedCountMetadataRow};
use crate::spec::StructType;
use crate::table::DeltaSnapshot;

const ADD_SCAN_CHUNK_FILES: usize = 1024;
const ADD_SCAN_CHUNK_BYTES: u64 = 128 * 1024 * 1024;
const FILE_OPEN_COST_BYTES: u64 = 4 * 1024 * 1024;
type ScanMetrics = Arc<Mutex<Vec<ExecutionPlanMetricsSet>>>;

struct ScanByAddsStreamState {
    input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    table_url: Url,
    table_version: i64,
    output_schema: SchemaRef,
    scan_schema: SchemaRef,
    scan_config: DeltaScanConfig,
    lakehouse_table: Option<LakehouseExecutionContext>,
    catalog_managed_commits: Option<CatalogManagedCommitSet>,
    remaining_rows: Option<usize>,
    pushdown_filter: Option<Arc<dyn PhysicalExpr>>,

    scan_parallelism: usize,
    scan_metrics: ScanMetrics,

    // Lazy init
    table_opened: bool,
    snapshot: Option<Arc<crate::table::DeltaSnapshot>>,
    log_store: Option<crate::delta_log::LogStoreRef>,
    session_state: Option<datafusion::execution::SessionState>,
    file_schema: Option<SchemaRef>,

    // control
    partition_scan: Option<bool>,
    emitted_partition_empty: bool,
    pending_adds: Vec<crate::spec::Add>,
    current_scan: Option<SendableRecordBatchStream>,
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
        lakehouse_table: Option<LakehouseExecutionContext>,
        catalog_managed_commits: Option<CatalogManagedCommitSet>,
        limit: Option<usize>,
        pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
        scan_parallelism: usize,
        scan_metrics: ScanMetrics,
    ) -> Self {
        let scan_schema = match &scan_config.metadata_aggregate {
            Some(aggregate) => Arc::new(Schema::new(
                output_schema
                    .fields()
                    .iter()
                    .zip(&aggregate.group_columns)
                    .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
                    .collect::<Vec<_>>(),
            )),
            None => Arc::clone(&output_schema),
        };
        Self {
            input,
            context,
            table_url,
            table_version,
            output_schema,
            scan_schema,
            scan_config,
            lakehouse_table,
            catalog_managed_commits,
            remaining_rows: limit,
            pushdown_filter,
            scan_parallelism,
            scan_metrics,
            table_opened: false,
            snapshot: None,
            log_store: None,
            session_state: None,
            file_schema: None,
            partition_scan: None,
            emitted_partition_empty: false,
            pending_adds: Vec::new(),
            current_scan: None,
        }
    }

    async fn ensure_table(&mut self) -> Result<()> {
        if self.table_opened {
            return Ok(());
        }
        let cached = if let Some(catalog_managed_commits) = self.catalog_managed_commits.clone() {
            load_table_with_config(
                self.context.as_ref(),
                &self.table_url,
                self.table_version,
                DeltaSnapshotConfig {
                    require_files: false,
                    catalog_managed_commits: Some(catalog_managed_commits),
                    ..Default::default()
                },
            )
            .await?
        } else {
            // Prefer a session-scoped cache. This avoids leaking state across sessions / RuntimeEnvs.
            // If the cache extension is not installed, fall back to no caching.
            let lakehouse_table = self.lakehouse_table.as_ref();
            match self.context.as_ref().extension::<DeltaTableCache>() {
                Ok(cache) => {
                    cache
                        .get(
                            self.context.as_ref(),
                            &self.table_url,
                            self.table_version,
                            lakehouse_table,
                        )
                        .await?
                }
                Err(_) => {
                    load_table_uncached(
                        self.context.as_ref(),
                        &self.table_url,
                        self.table_version,
                        lakehouse_table,
                    )
                    .await?
                }
            }
        };

        let snapshot_state = cached.snapshot.clone();
        snapshot_state
            .ensure_data_read_supported()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let session_state = SessionStateBuilder::new()
            .with_config(self.context.session_config().clone())
            .with_runtime_env(self.context.runtime_env().clone())
            .build();

        let mut scan_config = self.scan_config.clone();
        if scan_config.schema.is_none() {
            let schema = Arc::new(snapshot_state.schema().clone());
            scan_config.schema = Some(schema);
        }

        let table_partition_cols = snapshot_state.metadata().partition_columns();
        let kmode = snapshot_state.effective_column_mapping_mode();
        let kschema_arc = snapshot_state.schema();
        let logical_kernel = StructType::try_from(kschema_arc)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let physical_arrow = get_physical_schema(&logical_kernel, kmode)?;
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
        let column_mapping_mode = snapshot.effective_column_mapping_mode();
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

        let mut bytes = 0u64;
        let chunk_len = self
            .pending_adds
            .iter()
            .take(ADD_SCAN_CHUNK_FILES)
            .take_while(|add| {
                let take = bytes < ADD_SCAN_CHUNK_BYTES;
                bytes = bytes
                    .saturating_add(add.size.max(0) as u64)
                    .saturating_add(FILE_OPEN_COST_BYTES);
                take
            })
            .count();
        let mut adds = self.pending_adds.drain(..chunk_len).collect::<Vec<_>>();
        let mut metadata_batches = Vec::new();
        if let Some(aggregate) = &self.scan_config.metadata_aggregate
            && let Some(metadata) = snapshot.summarize_metadata_files(
                &adds,
                &aggregate.group_columns,
                ADD_SCAN_CHUNK_FILES,
            )
        {
            metadata_batches = metadata_record_batches(
                &metadata.rows,
                &self.output_schema,
                self.context.session_config().batch_size(),
            )?;
            let mut residual = metadata.residual_file_indices.into_iter().peekable();
            adds = adds
                .into_iter()
                .enumerate()
                .filter_map(|(index, add)| {
                    if residual.peek() == Some(&index) {
                        residual.next();
                        Some(add)
                    } else {
                        None
                    }
                })
                .collect();
        }
        let scan = if adds.is_empty() {
            None
        } else {
            Some(self.build_bulk_scan(snapshot, log_store, session_state, &adds, file_schema)?)
        };

        let scan_schema = Arc::clone(&self.scan_schema);
        let output_schema = Arc::clone(&self.output_schema);
        let weighted = self.scan_config.metadata_aggregate.is_some();
        let scanned = stream::iter(scan).flatten().and_then(move |batch| {
            let scan_schema = Arc::clone(&scan_schema);
            let output_schema = Arc::clone(&output_schema);
            async move {
                let batch =
                    restore_logical_record_batch(&batch, &scan_schema, column_mapping_mode)?;
                if weighted {
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(Int64Array::from_value(1, batch.num_rows())));
                    Ok(RecordBatch::try_new(output_schema, columns)?)
                } else {
                    Ok(batch)
                }
            }
        });
        let combined = stream::iter(metadata_batches.into_iter().map(Ok)).chain(scanned);
        self.current_scan = Some(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            combined,
        )));
        Ok(())
    }

    fn build_bulk_scan(
        &self,
        snapshot: &DeltaSnapshot,
        log_store: &LogStoreRef,
        session_state: &dyn datafusion::catalog::Session,
        adds: &[crate::spec::Add],
        file_schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream> {
        let scan_config = &self.scan_config;
        let file_output_schema = &self.scan_schema;
        let file_projection = file_scan_projection_for_schema(
            snapshot,
            scan_config,
            &file_schema,
            file_output_schema,
        )?;
        let file_logical_names = file_output_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();

        let mut file_scan_config = build_file_scan_config(
            snapshot,
            log_store,
            adds,
            scan_config,
            FileScanParams {
                projection: Some(&file_projection),
                // Limit must be applied after DV filtering, otherwise deleted rows consume the
                // physical-file limit and valid rows can be missed.
                limit: None,
                pushdown_filter: self.pushdown_filter.clone(),
                sort_order: None,
                table_stats_mode: TableStatsMode::AddsOnly,
            },
            session_state,
            file_schema,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if let Some(config) = file_scan_config.file_source.repartitioned(
            self.scan_parallelism,
            session_state
                .config()
                .options()
                .optimizer
                .repartition_file_min_size,
            None,
            &file_scan_config,
        )? {
            file_scan_config = config;
        } else {
            // Small files are not byte-split. Partition values remain attached
            // to each file, so they can share a bounded number of scan tasks.
            let files = std::mem::take(&mut file_scan_config.file_groups)
                .into_iter()
                .flat_map(|group| group.into_inner())
                .collect();
            file_scan_config.file_groups =
                datafusion::datasource::physical_plan::FileGroup::new(files)
                    .split_files(self.scan_parallelism);
            if file_scan_config.file_groups.is_empty() {
                file_scan_config.file_groups.push(
                    datafusion::datasource::physical_plan::FileGroup::new(vec![]),
                );
            }
        }
        self.scan_metrics
            .lock()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .push(file_scan_config.file_source.metrics().clone());
        let scan_exec =
            datafusion::datasource::source::DataSourceExec::from_data_source(file_scan_config);
        let scan_exec = rename_physical_plan(scan_exec, &file_logical_names)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        execute_stream(scan_exec, Arc::clone(&self.context))
    }

    async fn decode_adds_from_meta_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<Vec<crate::spec::Add>> {
        self.ensure_table().await?;
        let partition_value_columns = self
            .snapshot
            .as_deref()
            .ok_or_else(|| DataFusionError::Internal("missing snapshot".into()))?
            .physical_partition_columns();
        meta_adds::decode_adds_from_meta_batch_with_partition_value_columns(
            batch,
            Some(&partition_value_columns),
        )
    }
}

/// Materialize metadata groups without scaling buffers by their row-count weights.
fn metadata_record_batches(
    rows: &[GroupedCountMetadataRow],
    schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
    let mut batches = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let mut end = start;
        let mut bytes = 0usize;
        while end < rows.len() && end - start < batch_size.max(1) {
            let row_bytes = rows[end]
                .group_values
                .iter()
                .map(ScalarValue::size)
                .sum::<usize>()
                .saturating_add(std::mem::size_of::<i64>());
            if end > start && bytes.saturating_add(row_bytes) > MAX_BATCH_BYTES {
                break;
            }
            bytes = bytes.saturating_add(row_bytes);
            end += 1;
        }
        let chunk = &rows[start..end];
        let mut columns = (0..schema.fields().len() - 1)
            .map(|index| {
                ScalarValue::iter_to_array(chunk.iter().map(|row| row.group_values[index].clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        columns.push(Arc::new(Int64Array::from_iter_values(
            chunk.iter().map(|row| row.count),
        )));
        batches.push(RecordBatch::try_new(Arc::clone(schema), columns)?);
        start = end;
    }
    Ok(batches)
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
    lakehouse_table: Option<LakehouseExecutionContext>,
    catalog_managed_commits: Option<CatalogManagedCommitSet>,
    statistics: Statistics,
    scan_metrics: ScanMetrics,
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
        lakehouse_table: Option<LakehouseExecutionContext>,
        catalog_managed_commits: Option<CatalogManagedCommitSet>,
    ) -> Self {
        let statistics = Statistics::new_unknown(output_schema.as_ref());
        let cache = Self::compute_properties(input.as_ref(), output_schema.clone(), &scan_config);
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
            lakehouse_table,
            catalog_managed_commits,
            statistics,
            scan_metrics: Arc::new(Mutex::new(Vec::new())),
            cache,
        }
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

    pub fn catalog_table(&self) -> Option<&[String]> {
        self.lakehouse_table
            .as_ref()
            .map(LakehouseExecutionContext::catalog_table)
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }

    pub fn catalog_managed_commits(&self) -> Option<&CatalogManagedCommitSet> {
        self.catalog_managed_commits.as_ref()
    }

    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }

    fn compute_properties(
        input: &dyn ExecutionPlan,
        schema: SchemaRef,
        scan_config: &DeltaScanConfig,
    ) -> Arc<PlanProperties> {
        // Each Add stays in its metadata partition, including all native file scan splits.
        let partitioning = if let Some(path_column) = &scan_config.file_column_name
            && let Ok(path_index) = schema.index_of(path_column)
            && let Partitioning::Hash(expressions, count) = input.output_partitioning()
            && let Ok(metadata_path) = DeltaDecodePath::expression(PATH_COLUMN, &input.schema())
            && expressions.as_slice() == [metadata_path]
        {
            Partitioning::Hash(vec![Arc::new(Column::new(path_column, path_index))], *count)
        } else {
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count().max(1))
        };
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

#[async_trait]
impl ExecutionPlan for DeltaScanByAddsExec {
    fn name(&self) -> &'static str {
        "DeltaScanByAddsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let distribution = if self.scan_config.hash_partition_files {
            match DeltaDecodePath::expression(PATH_COLUMN, &self.input.schema()) {
                Ok(path) => Distribution::KeyPartitioned(vec![path]),
                Err(_) => Distribution::SinglePartition,
            }
        } else {
            Distribution::UnspecifiedDistribution
        };
        vec![distribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.pushdown_filter.iter(), f)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
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
            cloned.input.as_ref(),
            cloned.output_schema.clone(),
            &cloned.scan_config,
        );
        Ok(Arc::new(cloned))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if let Some(aggregate) = &self.scan_config.metadata_aggregate {
            if self.output_schema.fields().len() != aggregate.group_columns.len() + 1
                || self
                    .output_schema
                    .fields()
                    .last()
                    .map(|field| field.data_type())
                    != Some(&DataType::Int64)
            {
                return internal_err!(
                    "Delta metadata aggregation requires group columns and an Int64 weight"
                );
            }
            for name in &aggregate.group_columns {
                self.table_schema.field_with_name(name)?;
            }
        }
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let table_url = self.table_url.clone();
        let table_version = self.version;
        let output_schema = self.schema();
        let scan_config = self.scan_config.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let catalog_managed_commits = self.catalog_managed_commits.clone();
        let limit = self.limit;
        let pushdown_filter = self.pushdown_filter.clone();
        let scan_parallelism = context
            .session_config()
            .target_partitions()
            .div_ceil(self.input.output_partitioning().partition_count().max(1))
            .max(1);
        let state = ScanByAddsStreamState::new(
            input_stream,
            context,
            table_url,
            table_version,
            Arc::clone(&output_schema),
            scan_config,
            lakehouse_table,
            catalog_managed_commits,
            limit,
            pushdown_filter,
            scan_parallelism,
            Arc::clone(&self.scan_metrics),
        );

        let s = stream::try_unfold(state, |mut st| async move {
            loop {
                // Stop before decoding another Add batch or opening another DV/file stream.
                if st.remaining_rows == Some(0) {
                    return Ok(None);
                }

                // Drain current scan stream first.
                if let Some(scan) = &mut st.current_scan {
                    match scan.try_next().await? {
                        Some(batch) => {
                            let Some(remaining_rows) = st.remaining_rows.as_mut() else {
                                return Ok(Some((batch, st)));
                            };
                            if *remaining_rows == 0 {
                                return Ok(None);
                            }
                            let output_rows = batch.num_rows().min(*remaining_rows);
                            *remaining_rows -= output_rows;
                            let batch = if output_rows == batch.num_rows() {
                                batch
                            } else {
                                batch.slice(0, output_rows)
                            };
                            return Ok(Some((batch, st)));
                        }
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

                // Start work from the current metadata batch without waiting for later batches.
                if !st.pending_adds.is_empty() {
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

    fn metrics(&self) -> Option<MetricsSet> {
        let sources = self.scan_metrics.lock().ok()?;
        let mut metrics = MetricsSet::new();
        for source in sources.iter() {
            for metric in source.clone_inner().iter() {
                metrics.push(Arc::clone(metric));
            }
        }
        Some(metrics)
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if args.partition().is_none() {
            Ok(Arc::new(self.statistics.clone()))
        } else {
            Ok(Arc::new(Statistics::new_unknown(self.schema().as_ref())))
        }
    }
}

fn sanitize_statistics_to_schema(mut statistics: Statistics, schema: &SchemaRef) -> Statistics {
    if statistics.column_statistics.len() != schema.fields().len() {
        return Statistics::new_unknown(schema.as_ref());
    }

    sanitize_statistics_for_schema(schema, &mut statistics);

    statistics
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
        }?;
        if let Some(aggregate) = &self.scan_config.metadata_aggregate {
            write!(f, ", metadata_groups={:?}", aggregate.group_columns)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use url::Url;

    use super::{DeltaScanByAddsExec, metadata_record_batches};
    use crate::datasource::scan::map_statistics_to_schema;
    use crate::snapshot::GroupedCountMetadataRow;

    #[test]
    fn metadata_batches_bound_rows_without_expanding_count_weights() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, true),
            Field::new("weight", DataType::Int64, false),
        ]));
        let rows = (0..3)
            .map(|_| GroupedCountMetadataRow {
                group_values: vec![ScalarValue::Utf8(Some("x".repeat(50)))],
                count: 43_000_000,
            })
            .collect::<Vec<_>>();
        let batches = metadata_record_batches(&rows, &schema, 2)?;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(1), 0)?,
            ScalarValue::Int64(Some(43_000_000))
        );
        assert!(
            batches
                .iter()
                .all(|batch| batch.get_array_memory_size() < 4096)
        );

        let wide_rows = (0..3)
            .map(|_| GroupedCountMetadataRow {
                group_values: vec![ScalarValue::Utf8(Some("x".repeat(8 * 1024 * 1024)))],
                count: 1,
            })
            .collect::<Vec<_>>();
        let batches = metadata_record_batches(&wide_rows, &schema, 8192)?;
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| batch.num_rows() == 1));
        Ok(())
    }

    #[test]
    fn scan_preserves_only_decoded_file_path_hash_partitioning() -> Result<()> {
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::Partitioning;
        use datafusion::physical_plan::repartition::RepartitionExec;

        use crate::datasource::{DeltaScanConfig, PATH_COLUMN};
        use crate::physical_plan::DeltaDecodePath;

        let metadata_schema = Arc::new(Schema::new(vec![Field::new(
            PATH_COLUMN,
            DataType::Utf8,
            true,
        )]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("file", DataType::Utf8, true),
        ]));
        let config = DeltaScanConfig {
            file_column_name: Some("file".into()),
            ..Default::default()
        };
        let decoded = DeltaDecodePath::expression(PATH_COLUMN, &metadata_schema)?;
        let encoded: Arc<dyn PhysicalExpr> = Arc::new(Column::new(PATH_COLUMN, 0));
        for (partitioning, preserves_hash) in [
            (Partitioning::Hash(vec![decoded], 4), true),
            (Partitioning::Hash(vec![encoded], 4), false),
            (Partitioning::RoundRobinBatch(4), false),
        ] {
            let input = RepartitionExec::try_new(
                Arc::new(EmptyExec::new(Arc::clone(&metadata_schema))),
                partitioning,
            )?;
            let properties = DeltaScanByAddsExec::compute_properties(
                &input,
                Arc::clone(&output_schema),
                &config,
            );
            if preserves_hash {
                let expected: Arc<dyn PhysicalExpr> = Arc::new(Column::new("file", 1));
                assert_eq!(
                    properties.output_partitioning(),
                    &Partitioning::Hash(vec![expected], 4)
                );
            } else {
                assert!(matches!(
                    properties.output_partitioning(),
                    Partitioning::UnknownPartitioning(4)
                ));
            }
            let projected_schema = Arc::new(output_schema.project(&[0])?);
            let properties =
                DeltaScanByAddsExec::compute_properties(&input, projected_schema, &config);
            assert!(matches!(
                properties.output_partitioning(),
                Partitioning::UnknownPartitioning(4)
            ));
        }
        Ok(())
    }

    #[test]
    fn optimizer_keeps_file_partitioning_below_scan() -> Result<()> {
        use datafusion::common::config::ConfigOptions;
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::{ExecutionPlan, Partitioning};

        use crate::datasource::{DeltaScanConfig, PATH_COLUMN};
        use crate::physical_plan::DeltaDecodePath;

        let schema = Arc::new(Schema::new(vec![Field::new(
            PATH_COLUMN,
            DataType::Utf8,
            true,
        )]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(4));
        let path = DeltaDecodePath::expression(PATH_COLUMN, &schema)?;
        let scan: Arc<dyn ExecutionPlan> = Arc::new(DeltaScanByAddsExec::new(
            input,
            Url::parse("file:///tmp/delta-table")
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            0,
            Arc::clone(&schema),
            schema,
            DeltaScanConfig {
                file_column_name: Some(PATH_COLUMN.into()),
                hash_partition_files: true,
                ..Default::default()
            },
            None,
            None,
            None,
            None,
            None,
        ));
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 4;
        let optimized = EnsureRequirements::new().optimize(scan, &config)?;
        let scan = optimized
            .downcast_ref::<DeltaScanByAddsExec>()
            .ok_or_else(|| DataFusionError::Internal("scan must remain the root".into()))?;
        let repartition = scan
            .input()
            .downcast_ref::<RepartitionExec>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "file metadata must be partitioned before scanning".into(),
                )
            })?;
        assert_eq!(
            repartition.partitioning(),
            &Partitioning::Hash(vec![path], 4)
        );
        assert!(matches!(
            scan.properties().output_partitioning(),
            Partitioning::Hash(_, 4)
        ));
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
            None,
            None,
        )
        .with_output_statistics(Some(table_stats));

        let stats = StatisticsContext::new()
            .compute(&scan, &StatisticsArgs::new())
            .ok();
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
            None,
            None,
        )
        .with_output_statistics(Some(output_stats));

        let stats = StatisticsContext::new().compute(&scan, &StatisticsArgs::new())?;
        assert_eq!(stats.num_rows, Precision::Exact(88));
        assert_eq!(stats.column_statistics.len(), 1);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Exact(6));
        Ok(())
    }
}
