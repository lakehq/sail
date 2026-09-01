//! Physical execution nodes for row-level Merge-on-Read deletion vector writing.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};
use datafusion::arrow::compute::SortOptions;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, apply_expression_roots,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::stream::{self, StreamExt};
use object_store::ObjectStore;
use sail_common_datafusion::datasource::{OPERATION_COLUMN, RowLevelOperationType};
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;
use url::Url;

use crate::deletion_vector::{DeletionVectorBitmap, DeletionVectorWriter};
use crate::physical_plan::{
    COL_ACTION, ExecCommitMeta, current_timestamp_millis, decode_adds_from_batch,
    delta_action_schema, encode_actions, meta_adds,
};
use crate::schema::PhysicalPartitionColumn;
use crate::spec::{Action, Add, DeltaOperation, RemoveOptions};
use crate::transaction::OperationMetrics;

/// Update an Add action's stats to reflect that the bounds are now wide (non-tight)
/// because a Deletion Vector has been added or updated.
///
/// When a DV is present, min/max statistics may include values
/// from logically-deleted rows, so `tightBounds` must be set to `false`.
pub(crate) fn widen_stats_bounds(stats_json: Option<&str>) -> Option<String> {
    let json = stats_json?;
    match crate::spec::Stats::from_json_str(json) {
        Ok(mut stats) => {
            if stats.tight_bounds {
                stats.tight_bounds = false;
                stats.to_json_string().ok()
            } else {
                // Already wide — return the original string unchanged to avoid re-encoding.
                Some(json.to_string())
            }
        }
        Err(e) => {
            log::warn!("failed to parse stats JSON for tightBounds update: {e}");
            None
        }
    }
}

/// Physical execution node that writes deletion vectors for Merge-on-Read operations.
///
/// 1. Reads metadata (Add actions) for files to process from its input partition
/// 2. Scans each file to identify which row indices match the condition
/// 3. Writes DV files containing bitmaps of deleted row indices
/// 4. Emits Remove(old_add) + Add(path, dv=descriptor) commit actions

#[derive(Debug)]
pub struct DeletionVectorWriterExec {
    /// Input plan producing Add-action metadata for files to process.
    input: Arc<dyn ExecutionPlan>,
    /// Table URL for object store resolution.
    table_url: Url,
    /// Physical predicate to evaluate on each row.
    condition: Arc<dyn PhysicalExpr>,
    /// Table schema for reading files.
    table_schema: datafusion::arrow::datatypes::SchemaRef,
    /// Table version.
    version: i64,
    /// Mapping from replay output partition column names to Delta log partition value keys.
    partition_value_columns: Option<Vec<PhysicalPartitionColumn>>,
    /// The delta operation to record in the commit log.
    operation: Option<crate::spec::DeltaOperation>,
    /// Metrics set.
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties.
    cache: Arc<PlanProperties>,
}

impl DeletionVectorWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        condition: Arc<dyn PhysicalExpr>,
        table_schema: datafusion::arrow::datatypes::SchemaRef,
        version: i64,
        partition_value_columns: Option<Vec<PhysicalPartitionColumn>>,
        operation: Option<crate::spec::DeltaOperation>,
    ) -> Result<Self> {
        let schema = delta_action_schema()?;
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            table_url,
            condition,
            table_schema,
            version,
            partition_value_columns,
            operation,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn condition(&self) -> &Arc<dyn PhysicalExpr> {
        &self.condition
    }

    pub fn table_schema(&self) -> &datafusion::arrow::datatypes::SchemaRef {
        &self.table_schema
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn partition_value_columns(&self) -> Option<&[PhysicalPartitionColumn]> {
        self.partition_value_columns.as_deref()
    }

    pub fn operation(&self) -> Option<&crate::spec::DeltaOperation> {
        self.operation.as_ref()
    }
}

/// Classifies invalidated rows supplied to a row-level deletion-vector writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionVectorRowOperationMode {
    /// Every invalidated row is produced by an UPDATE.
    Update,
    /// Every invalidated row is produced by a DELETE.
    Delete,
    /// UPDATE and DELETE rows are distinguished by the typed operation column.
    Mixed,
}

/// Configuration for writing row-level deletion vectors.
#[derive(Debug, Clone)]
pub struct DeletionVectorRowsWriterConfig {
    path_column: String,
    row_index_column: String,
    operation_mode: DeletionVectorRowOperationMode,
    version: i64,
    partition_value_columns: Option<Vec<PhysicalPartitionColumn>>,
    operation: Option<DeltaOperation>,
}

impl DeletionVectorRowsWriterConfig {
    pub fn new(
        path_column: impl Into<String>,
        row_index_column: impl Into<String>,
        operation_mode: DeletionVectorRowOperationMode,
        version: i64,
        partition_value_columns: Option<Vec<PhysicalPartitionColumn>>,
        operation: Option<DeltaOperation>,
    ) -> Self {
        Self {
            path_column: path_column.into(),
            row_index_column: row_index_column.into(),
            operation_mode,
            version,
            partition_value_columns,
            operation,
        }
    }
}

/// Physical execution node that writes deletion vectors from file path + row-index rows.
///
/// The row-level logical plan has already identified the exact target rows to invalidate.
#[derive(Debug)]
pub struct DeletionVectorRowsWriterExec {
    input: Arc<dyn ExecutionPlan>,
    adds_input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    path_column: String,
    row_index_column: String,
    operation_mode: DeletionVectorRowOperationMode,
    version: i64,
    partition_value_columns: Option<Vec<PhysicalPartitionColumn>>,
    operation: Option<crate::spec::DeltaOperation>,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl DeletionVectorRowsWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        adds_input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        config: DeletionVectorRowsWriterConfig,
    ) -> Result<Self> {
        let DeletionVectorRowsWriterConfig {
            path_column,
            row_index_column,
            operation_mode,
            version,
            partition_value_columns,
            operation,
        } = config;
        input
            .schema()
            .index_of(&path_column)
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        input
            .schema()
            .index_of(&row_index_column)
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        if matches!(operation_mode, DeletionVectorRowOperationMode::Mixed) {
            input
                .schema()
                .index_of(OPERATION_COLUMN)
                .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        }
        adds_input
            .schema()
            .index_of(&path_column)
            .map_err(|e| DataFusionError::Plan(format!("{e}")))?;

        let schema = delta_action_schema()?;
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            adds_input,
            table_url,
            path_column,
            row_index_column,
            operation_mode,
            version,
            partition_value_columns,
            operation,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn adds_input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.adds_input
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn path_column(&self) -> &str {
        &self.path_column
    }

    pub fn row_index_column(&self) -> &str {
        &self.row_index_column
    }

    pub fn operation_mode(&self) -> DeletionVectorRowOperationMode {
        self.operation_mode
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn partition_value_columns(&self) -> Option<&[PhysicalPartitionColumn]> {
        self.partition_value_columns.as_deref()
    }

    pub fn operation(&self) -> Option<&crate::spec::DeltaOperation> {
        self.operation.as_ref()
    }
}

impl DisplayAs for DeletionVectorRowsWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeletionVectorRowsWriterExec: path_column={}, row_index_column={}",
                    self.path_column, self.row_index_column
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeletionVectorRowsWriterExec")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeletionVectorRowOperation {
    Update,
    Delete,
}

#[derive(Debug, Default)]
struct RowLevelDvBitmaps {
    update_bitmap: DeletionVectorBitmap,
    delete_bitmap: DeletionVectorBitmap,
}

impl RowLevelDvBitmaps {
    fn insert(&mut self, operation: DeletionVectorRowOperation, row_index: u64) {
        match operation {
            DeletionVectorRowOperation::Update => {
                self.update_bitmap.insert(row_index);
            }
            DeletionVectorRowOperation::Delete => {
                self.delete_bitmap.insert(row_index);
            }
        }
    }
}

struct ReconciledRowLevelDv {
    final_bitmap: DeletionVectorBitmap,
    newly_updated_rows: u64,
    newly_deleted_rows: u64,
}

struct RowLevelDvWriteStats {
    newly_updated_rows: u64,
    newly_deleted_rows: u64,
    had_existing_dv: bool,
}

#[derive(Default)]
struct RowLevelDvWriteMetrics {
    newly_updated_rows: u64,
    newly_deleted_rows: u64,
    num_dv_added: u64,
    num_dv_updated: u64,
}

impl RowLevelDvWriteMetrics {
    fn record_file(&mut self, stats: RowLevelDvWriteStats) {
        self.newly_updated_rows = self
            .newly_updated_rows
            .saturating_add(stats.newly_updated_rows);
        self.newly_deleted_rows = self
            .newly_deleted_rows
            .saturating_add(stats.newly_deleted_rows);
        self.num_dv_added = self.num_dv_added.saturating_add(1);
        if stats.had_existing_dv {
            self.num_dv_updated = self.num_dv_updated.saturating_add(1);
        }
    }

    fn newly_invalidated_rows(&self) -> u64 {
        self.newly_updated_rows
            .saturating_add(self.newly_deleted_rows)
    }
}

fn merge_deletion_vector_row_operation(
    operation_column: &dyn Array,
    row: usize,
) -> Result<DeletionVectorRowOperation> {
    let operation_value = if let Some(values) =
        operation_column.as_any().downcast_ref::<Int32Array>()
    {
        if values.is_null(row) {
            return Err(DataFusionError::Execution(format!(
                "row-level DV operation column '{OPERATION_COLUMN}' must not contain nulls"
            )));
        }
        i64::from(values.value(row))
    } else if let Some(values) = operation_column.as_any().downcast_ref::<Int64Array>() {
        if values.is_null(row) {
            return Err(DataFusionError::Execution(format!(
                "row-level DV operation column '{OPERATION_COLUMN}' must not contain nulls"
            )));
        }
        values.value(row)
    } else {
        return Err(DataFusionError::Internal(format!(
            "row-level DV operation column '{OPERATION_COLUMN}' must be Int32 or Int64, got {:?}",
            operation_column.data_type()
        )));
    };

    let operation = RowLevelOperationType::try_from(operation_value).map_err(|value| {
        DataFusionError::Internal(format!(
            "row-level MERGE DV input contains unknown operation value {value}"
        ))
    })?;
    match operation {
        RowLevelOperationType::MatchedUpdate | RowLevelOperationType::NotMatchedBySourceUpdate => {
            Ok(DeletionVectorRowOperation::Update)
        }
        RowLevelOperationType::MatchedDelete | RowLevelOperationType::NotMatchedBySourceDelete => {
            Ok(DeletionVectorRowOperation::Delete)
        }
        operation => Err(DataFusionError::Internal(format!(
            "row-level MERGE DV input contains unsupported operation {operation:?}"
        ))),
    }
}

fn reconcile_row_level_dv_bitmaps(
    path: &str,
    mut existing_bitmap: DeletionVectorBitmap,
    bitmaps: &RowLevelDvBitmaps,
) -> Result<ReconciledRowLevelDv> {
    if !bitmaps
        .update_bitmap
        .inner()
        .is_disjoint(bitmaps.delete_bitmap.inner())
    {
        return Err(DataFusionError::Execution(format!(
            "row-level DV assigns the same row in file '{path}' to both UPDATE and DELETE"
        )));
    }

    let new_updates =
        DeletionVectorBitmap::from_treemap(bitmaps.update_bitmap.inner() - existing_bitmap.inner());
    let deletes_without_existing =
        DeletionVectorBitmap::from_treemap(bitmaps.delete_bitmap.inner() - existing_bitmap.inner());
    let new_deletes =
        DeletionVectorBitmap::from_treemap(deletes_without_existing.inner() - new_updates.inner());

    existing_bitmap.union_with(&bitmaps.update_bitmap);
    existing_bitmap.union_with(&bitmaps.delete_bitmap);

    Ok(ReconciledRowLevelDv {
        final_bitmap: existing_bitmap,
        newly_updated_rows: new_updates.len(),
        newly_deleted_rows: new_deletes.len(),
    })
}

async fn write_row_level_dv_actions_for_path(
    path: String,
    bitmaps: RowLevelDvBitmaps,
    add_by_path: &HashMap<String, Add>,
    object_store: &Arc<dyn ObjectStore>,
    table_url: &Url,
    dv_writer: &DeletionVectorWriter,
    deletion_timestamp: i64,
    output_actions: &mut Vec<Action>,
) -> Result<Option<RowLevelDvWriteStats>> {
    let add = add_by_path.get(&path).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "row-level DV references file '{path}' that is not active in Delta snapshot"
        ))
    })?;

    let had_existing_dv = add.deletion_vector.is_some();
    let existing_bitmap = if let Some(existing_dv) = &add.deletion_vector {
        crate::deletion_vector::read_deletion_vector(object_store.as_ref(), table_url, existing_dv)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    } else {
        DeletionVectorBitmap::new()
    };
    let reconciled = reconcile_row_level_dv_bitmaps(&path, existing_bitmap, &bitmaps)?;

    if reconciled
        .newly_updated_rows
        .saturating_add(reconciled.newly_deleted_rows)
        == 0
    {
        return Ok(None);
    }

    let dv_descriptor = dv_writer
        .write(&reconciled.final_bitmap)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let remove = Add {
        path: add.path.clone(),
        partition_values: add.partition_values.clone(),
        size: add.size,
        modification_time: add.modification_time,
        data_change: true,
        stats: add.stats.clone(),
        tags: add.tags.clone(),
        deletion_vector: add.deletion_vector.clone(),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
        clustering_provider: add.clustering_provider.clone(),
        commit_version: None,
        commit_timestamp: None,
    }
    .into_remove_with_options(
        deletion_timestamp,
        RemoveOptions {
            extended_file_metadata: Some(true),
            include_tags: false,
        },
    );
    output_actions.push(Action::Remove(remove));

    let new_stats = widen_stats_bounds(add.stats.as_deref()).or_else(|| add.stats.clone());
    let new_add = Add {
        path: add.path.clone(),
        partition_values: add.partition_values.clone(),
        size: add.size,
        modification_time: add.modification_time,
        data_change: true,
        stats: new_stats,
        tags: add.tags.clone(),
        deletion_vector: Some(dv_descriptor),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
        clustering_provider: add.clustering_provider.clone(),
        commit_version: None,
        commit_timestamp: None,
    };
    output_actions.push(Action::Add(new_add));

    Ok(Some(RowLevelDvWriteStats {
        newly_updated_rows: reconciled.newly_updated_rows,
        newly_deleted_rows: reconciled.newly_deleted_rows,
        had_existing_dv,
    }))
}

#[async_trait]
impl ExecutionPlan for DeletionVectorRowsWriterExec {
    fn name(&self) -> &'static str {
        "DeletionVectorRowsWriterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input, &self.adds_input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        if children.len() != 2 {
            return internal_err!("DeletionVectorRowsWriterExec requires exactly two children");
        }
        Ok(Arc::new(DeletionVectorRowsWriterExec::new(
            children[0].clone(),
            children[1].clone(),
            self.table_url.clone(),
            DeletionVectorRowsWriterConfig::new(
                self.path_column.clone(),
                self.row_index_column.clone(),
                self.operation_mode,
                self.version,
                self.partition_value_columns.clone(),
                self.operation.clone(),
            ),
        )?))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let dist_for = |plan: &Arc<dyn ExecutionPlan>| -> Distribution {
            let idx = match plan.schema().index_of(&self.path_column) {
                Ok(i) => i,
                Err(_) => return Distribution::SinglePartition,
            };
            let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&self.path_column, idx));
            Distribution::KeyPartitioned(vec![expr])
        };
        vec![dist_for(&self.input), dist_for(&self.adds_input)]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let idx = match self.input.schema().index_of(&self.path_column) {
            Ok(i) => i,
            Err(_) => return vec![None, None],
        };
        let Some(ordering) = LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new(&self.path_column, idx)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }]) else {
            return vec![None, None];
        };
        vec![Some(OrderingRequirements::from(ordering)), None]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let input_partition_count = input.output_partitioning().partition_count().max(1);
        let adds_input = Arc::clone(&self.adds_input);
        let adds_partition_count = adds_input.output_partitioning().partition_count().max(1);
        if input_partition_count != adds_partition_count {
            return internal_err!(
                "DeletionVectorRowsWriterExec requires aligned input partitions, got {input_partition_count} DV row partitions and {adds_partition_count} Add partitions"
            );
        }
        if partition >= input_partition_count {
            return internal_err!(
                "DeletionVectorRowsWriterExec partition {partition} exceeds partition count {input_partition_count}"
            );
        }
        let table_url = self.table_url.clone();
        let path_column = self.path_column.clone();
        let row_index_column = self.row_index_column.clone();
        let partition_value_columns = self.partition_value_columns.clone();
        let operation = self.operation.clone();
        let operation_mode = self.operation_mode;

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
            let exec_start = Instant::now();

            let mut add_by_path = HashMap::new();
            let mut adds_stream = adds_input.execute(partition, context.clone())?;
            while let Some(batch_result) = adds_stream.next().await {
                let batch = batch_result?;
                let adds = if batch.column_by_name(COL_ACTION).is_some() {
                    decode_adds_from_batch(&batch)?
                } else {
                    meta_adds::decode_adds_from_meta_batch_with_partition_value_columns(
                        &batch,
                        partition_value_columns.as_deref(),
                    )?
                };
                for add in adds {
                    add_by_path.insert(add.path.clone(), add);
                }
            }

            let object_store = context
                .runtime_env()
                .object_store_registry
                .get_store(&table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let dv_writer = DeletionVectorWriter::new(Arc::clone(&object_store), table_url.clone());
            let deletion_timestamp = current_timestamp_millis()?;
            let mut output_actions: Vec<Action> = Vec::new();
            let mut write_metrics = RowLevelDvWriteMetrics::default();
            let mut current_path: Option<String> = None;
            let mut current_bitmaps = RowLevelDvBitmaps::default();

            let mut stream = input.execute(partition, context.clone())?;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let path_idx = batch.schema().index_of(&path_column)?;
                let row_index_idx = batch.schema().index_of(&row_index_column)?;

                let paths = batch
                    .column(path_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "row-level DV path column '{path_column}' must be Utf8"
                        ))
                    })?;
                let row_indices = batch
                    .column(row_index_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "row-level DV row-index column '{row_index_column}' must be Int64"
                        ))
                    })?;
                let operation_column = if matches!(
                    operation_mode,
                    DeletionVectorRowOperationMode::Mixed
                ) {
                    Some(batch.column_by_name(OPERATION_COLUMN).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "row-level MERGE DV input is missing required column '{OPERATION_COLUMN}'"
                        ))
                    })?)
                } else {
                    None
                };
                for row in 0..batch.num_rows() {
                    if paths.is_null(row) || row_indices.is_null(row) {
                        return Err(DataFusionError::Execution(
                            "row-level DV rows must have non-null file path and row index"
                                .to_string(),
                        ));
                    }
                    let row_index = row_indices.value(row);
                    if row_index < 0 {
                        return Err(DataFusionError::Execution(format!(
                            "row-level DV row index must be non-negative, got {row_index}"
                        )));
                    }
                    let path = paths.value(row);
                    if current_path
                        .as_deref()
                        .is_some_and(|current| current != path)
                    {
                        let flushed_path =
                            current_path.replace(path.to_string()).ok_or_else(|| {
                                DataFusionError::Internal("missing row-level DV path".into())
                            })?;
                        let flushed_bitmaps = std::mem::take(&mut current_bitmaps);
                        if let Some(stats) = write_row_level_dv_actions_for_path(
                            flushed_path,
                            flushed_bitmaps,
                            &add_by_path,
                            &object_store,
                            &table_url,
                            &dv_writer,
                            deletion_timestamp,
                            &mut output_actions,
                        )
                        .await?
                        {
                            write_metrics.record_file(stats);
                        }
                    } else if current_path.is_none() {
                        current_path = Some(path.to_string());
                    }
                    let row_operation = match operation_mode {
                        DeletionVectorRowOperationMode::Update => {
                            DeletionVectorRowOperation::Update
                        }
                        DeletionVectorRowOperationMode::Delete => {
                            DeletionVectorRowOperation::Delete
                        }
                        DeletionVectorRowOperationMode::Mixed => {
                            let operation_column = operation_column.ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "row-level MERGE DV input is missing required column '{OPERATION_COLUMN}'"
                                ))
                            })?;
                            merge_deletion_vector_row_operation(operation_column.as_ref(), row)?
                        }
                    };
                    current_bitmaps.insert(row_operation, row_index as u64);
                }
            }

            if let Some(path) = current_path
                && let Some(stats) = write_row_level_dv_actions_for_path(
                    path,
                    current_bitmaps,
                    &add_by_path,
                    &object_store,
                    &table_url,
                    &dv_writer,
                    deletion_timestamp,
                    &mut output_actions,
                )
                .await?
            {
                write_metrics.record_file(stats);
            }

            if output_actions.is_empty() {
                return encode_actions(Vec::new(), None);
            }

            let total_invalidated_rows = write_metrics.newly_invalidated_rows();
            output_rows.add(total_invalidated_rows as usize);
            log::debug!(
                "row-level DV write partition {partition}: affected_files={}, dv_updated={}, \
                 updated_rows={}, deleted_rows={}, invalidated_rows={total_invalidated_rows}",
                write_metrics.num_dv_added,
                write_metrics.num_dv_updated,
                write_metrics.newly_updated_rows,
                write_metrics.newly_deleted_rows,
            );

            let target_rows_deleted = write_metrics.newly_deleted_rows;
            let execution_time_ms = Some(exec_start.elapsed().as_millis() as u64);
            let (row_count, operation_metrics) = match operation.as_ref() {
                Some(DeltaOperation::Update { .. }) => (
                    0,
                    OperationMetrics {
                        execution_time_ms,
                        num_removed_files: Some(write_metrics.num_dv_added),
                        num_deletion_vectors_added: Some(write_metrics.num_dv_added),
                        num_deletion_vectors_updated: Some(write_metrics.num_dv_updated),
                        num_deletion_vectors_removed: Some(write_metrics.num_dv_updated),
                        ..Default::default()
                    },
                ),
                _ => (
                    target_rows_deleted,
                    OperationMetrics {
                        execution_time_ms,
                        num_removed_files: Some(write_metrics.num_dv_added),
                        num_added_files: Some(write_metrics.num_dv_added),
                        num_target_rows_deleted: Some(target_rows_deleted),
                        num_target_deletion_vectors_added: Some(write_metrics.num_dv_added),
                        num_target_deletion_vectors_updated: Some(write_metrics.num_dv_updated),
                        num_target_deletion_vectors_removed: Some(write_metrics.num_dv_updated),
                        ..Default::default()
                    },
                ),
            };

            encode_actions(
                output_actions,
                Some(ExecCommitMeta {
                    row_count,
                    operation,
                    operation_metrics,
                }),
            )
        };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for DeletionVectorWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeletionVectorWriterExec: condition={}", self.condition)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeletionVectorWriterExec")
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for DeletionVectorWriterExec {
    fn name(&self) -> &'static str {
        "DeletionVectorWriterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots([&self.condition], f)
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
            return internal_err!("DeletionVectorWriterExec requires exactly one child");
        }
        Ok(Arc::new(DeletionVectorWriterExec::new(
            children[0].clone(),
            self.table_url.clone(),
            self.condition.clone(),
            self.table_schema.clone(),
            self.version,
            self.partition_value_columns.clone(),
            self.operation.clone(),
        )?))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream = self.input.execute(partition, context.clone())?;
        let table_url = self.table_url.clone();
        let condition = self.condition.clone();
        let table_schema = self.table_schema.clone();
        let operation = self.operation.clone();
        let partition_value_columns = self.partition_value_columns.clone();

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let _output_bytes = MetricBuilder::new(&self.metrics).output_bytes(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
            let exec_start = Instant::now();

            // Phase 1: Collect Add actions assigned to this partition
            let mut adds_to_process: Vec<Add> = Vec::new();
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                if batch.column_by_name(COL_ACTION).is_some() {
                    adds_to_process.extend(decode_adds_from_batch(&batch)?);
                } else {
                    adds_to_process.extend(
                        meta_adds::decode_adds_from_meta_batch_with_partition_value_columns(
                            &batch,
                            partition_value_columns.as_deref(),
                        )?,
                    );
                }
            }

            if adds_to_process.is_empty() {
                return encode_actions(Vec::new(), None);
            }

            // Phase 2: For each file, scan and identify matching row indices
            let object_store = context
                .runtime_env()
                .object_store_registry
                .get_store(&table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let dv_writer = DeletionVectorWriter::new(Arc::clone(&object_store), table_url.clone());

            let deletion_timestamp = current_timestamp_millis()?;
            let mut output_actions: Vec<Action> = Vec::new();
            let mut total_deleted_rows: u64 = 0;
            let mut num_dv_added: u64 = 0;
            let mut num_dv_updated: u64 = 0;
            let mut scan_time_ms: u64 = 0;

            for add in &adds_to_process {
                let scan_start = Instant::now();
                let matching_rows = scan_file_for_matching_rows(
                    add,
                    &table_url,
                    &table_schema,
                    &condition,
                    &context,
                )
                .await?;
                scan_time_ms = scan_time_ms.saturating_add(scan_start.elapsed().as_millis() as u64);

                if matching_rows.is_empty() {
                    continue;
                }

                // Build bitmap from matching row indices
                let bitmap = DeletionVectorBitmap::from_row_indices(matching_rows);

                // If the file already has an existing DV, merge with it.
                // Track how many of the matching rows are genuinely new (not already deleted).
                let (final_bitmap, had_existing_dv, newly_deleted_count) =
                    if let Some(existing_dv) = &add.deletion_vector {
                        let mut existing = crate::deletion_vector::read_deletion_vector(
                            object_store.as_ref(),
                            &table_url,
                            existing_dv,
                        )
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        let before = existing.len();
                        existing.union_with(&bitmap);
                        let newly_deleted = existing.len() - before;
                        (existing, true, newly_deleted)
                    } else {
                        let count = bitmap.len();
                        (bitmap, false, count)
                    };

                // All matching rows were already logically deleted by the existing DV —
                // the union produced no change, so there is nothing to commit.
                if newly_deleted_count == 0 {
                    continue;
                }

                // Write the new (or merged) DV
                let dv_descriptor = dv_writer
                    .write(&final_bitmap)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Emit Remove for old Add entry (with its old DV, if any)
                let remove = Add {
                    path: add.path.clone(),
                    partition_values: add.partition_values.clone(),
                    size: add.size,
                    modification_time: add.modification_time,
                    data_change: true,
                    stats: add.stats.clone(),
                    tags: add.tags.clone(),
                    deletion_vector: add.deletion_vector.clone(),
                    base_row_id: add.base_row_id,
                    default_row_commit_version: add.default_row_commit_version,
                    clustering_provider: add.clustering_provider.clone(),
                    commit_version: None,
                    commit_timestamp: None,
                }
                .into_remove_with_options(
                    deletion_timestamp,
                    RemoveOptions {
                        extended_file_metadata: Some(true),
                        include_tags: false,
                    },
                );
                output_actions.push(Action::Remove(remove));

                // Emit Add with new DV descriptor (same physical file). stats.tightBounds
                // must be false when a DV is present, because deleted rows may have held
                // the extreme min/max values.
                let new_stats =
                    widen_stats_bounds(add.stats.as_deref()).or_else(|| add.stats.clone());
                let new_add = Add {
                    path: add.path.clone(),
                    partition_values: add.partition_values.clone(),
                    size: add.size,
                    modification_time: add.modification_time,
                    data_change: true,
                    stats: new_stats,
                    tags: add.tags.clone(),
                    deletion_vector: Some(dv_descriptor),
                    base_row_id: add.base_row_id,
                    default_row_commit_version: add.default_row_commit_version,
                    clustering_provider: add.clustering_provider.clone(),
                    commit_version: None,
                    commit_timestamp: None,
                };
                output_actions.push(Action::Add(new_add));

                total_deleted_rows += newly_deleted_count;
                num_dv_added += 1;
                if had_existing_dv {
                    num_dv_updated += 1;
                }
            }

            let num_affected_files = num_dv_added;
            output_rows.add(total_deleted_rows as usize);

            log::debug!(
                "DV write partition {partition}: affected_files={num_affected_files}, \
                 dv_updated={num_dv_updated}, deleted_rows={total_deleted_rows}"
            );

            let operation_metrics = OperationMetrics {
                execution_time_ms: Some(exec_start.elapsed().as_millis() as u64),
                scan_time_ms: Some(scan_time_ms),
                num_removed_files: Some(num_affected_files),
                num_added_files: Some(num_affected_files),
                num_deleted_rows: Some(total_deleted_rows),
                num_copied_rows: Some(0),
                num_deletion_vectors_added: Some(num_dv_added),
                num_deletion_vectors_updated: Some(num_dv_updated),
                num_deletion_vectors_removed: Some(num_dv_updated),
                ..Default::default()
            };

            encode_actions(
                output_actions,
                Some(ExecCommitMeta {
                    row_count: total_deleted_rows,
                    operation,
                    operation_metrics,
                }),
            )
        };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Scan a single file and return the row indices that match the given condition.
async fn scan_file_for_matching_rows(
    add: &Add,
    table_url: &Url,
    table_schema: &datafusion::arrow::datatypes::SchemaRef,
    condition: &Arc<dyn PhysicalExpr>,
    context: &Arc<TaskContext>,
) -> Result<Vec<u64>> {
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use object_store::path::Path;

    // Build a simple single-file scan directly from the parquet file.
    let object_store_url = datafusion::datasource::object_store::ObjectStoreUrl::parse(
        &table_url[..url::Position::BeforePath],
    )?;

    // The add.path is relative to the table root (e.g., "part-00001-...parquet").
    // We must prefix it with the table root path so the object store can find it.
    let table_root = Path::from(table_url.path());
    let file_location = Path::from(format!(
        "{}{}{}",
        table_root,
        object_store::path::DELIMITER,
        add.path
    ));
    let file_size = add.size as u64;
    let partitioned_file = PartitionedFile::new(file_location.to_string(), file_size);

    let parquet_source = ParquetSource::new(Arc::clone(table_schema));
    let file_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
        Arc::new(parquet_source);

    let file_group = FileGroup::from(vec![partitioned_file]);
    let file_scan_config = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(vec![file_group])
        .with_expr_adapter(Some(Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {})))
        .build();

    let parquet_exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(file_scan_config);

    let mut matching_indices: Vec<u64> = Vec::new();
    let mut global_row_offset: u64 = 0;

    // Execute and evaluate the condition batch-by-batch
    let partitions = parquet_exec.output_partitioning().partition_count();
    for partition in 0..partitions {
        let mut stream = parquet_exec.execute(partition, Arc::clone(context))?;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            // Evaluate the delete condition on this batch
            let result = condition.evaluate(&batch)?;
            let bool_array = result.into_array(num_rows).map_err(|e| {
                DataFusionError::Internal(format!("condition evaluation error: {e}"))
            })?;
            let bool_array = bool_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "delete condition did not produce BooleanArray".into(),
                    )
                })?;

            // Collect row indices where the condition is true
            for i in 0..num_rows {
                if bool_array.is_valid(i) && bool_array.value(i) {
                    matching_indices.push(global_row_offset + i as u64);
                }
            }

            global_row_offset += num_rows as u64;
        }
    }

    Ok(matching_indices)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_level_dv_reconciliation_excludes_existing_and_duplicate_rows() -> Result<()> {
        let existing_bitmap = DeletionVectorBitmap::from_row_indices([1, 4]);
        let mut bitmaps = RowLevelDvBitmaps::default();
        bitmaps.insert(DeletionVectorRowOperation::Update, 1);
        bitmaps.insert(DeletionVectorRowOperation::Update, 2);
        bitmaps.insert(DeletionVectorRowOperation::Update, 2);
        bitmaps.insert(DeletionVectorRowOperation::Delete, 3);
        bitmaps.insert(DeletionVectorRowOperation::Delete, 3);
        bitmaps.insert(DeletionVectorRowOperation::Delete, 4);

        let reconciled = reconcile_row_level_dv_bitmaps("part.parquet", existing_bitmap, &bitmaps)?;

        assert_eq!(reconciled.newly_updated_rows, 1);
        assert_eq!(reconciled.newly_deleted_rows, 1);
        assert_eq!(reconciled.final_bitmap.len(), 4);
        for row_index in [1, 2, 3, 4] {
            assert!(reconciled.final_bitmap.contains(row_index));
        }
        Ok(())
    }

    #[test]
    fn row_level_dv_metrics_are_aggregated_after_per_file_reconciliation() -> Result<()> {
        let mut first_file_bitmaps = RowLevelDvBitmaps::default();
        first_file_bitmaps.insert(DeletionVectorRowOperation::Update, 1);
        first_file_bitmaps.insert(DeletionVectorRowOperation::Delete, 2);
        let first_file = reconcile_row_level_dv_bitmaps(
            "first.parquet",
            DeletionVectorBitmap::from_row_indices([1]),
            &first_file_bitmaps,
        )?;

        let mut second_file_bitmaps = RowLevelDvBitmaps::default();
        second_file_bitmaps.insert(DeletionVectorRowOperation::Update, 5);
        second_file_bitmaps.insert(DeletionVectorRowOperation::Update, 6);
        let second_file = reconcile_row_level_dv_bitmaps(
            "second.parquet",
            DeletionVectorBitmap::new(),
            &second_file_bitmaps,
        )?;

        let mut metrics = RowLevelDvWriteMetrics::default();
        metrics.record_file(RowLevelDvWriteStats {
            newly_updated_rows: first_file.newly_updated_rows,
            newly_deleted_rows: first_file.newly_deleted_rows,
            had_existing_dv: true,
        });
        metrics.record_file(RowLevelDvWriteStats {
            newly_updated_rows: second_file.newly_updated_rows,
            newly_deleted_rows: second_file.newly_deleted_rows,
            had_existing_dv: false,
        });

        assert_eq!(metrics.newly_updated_rows, 2);
        assert_eq!(metrics.newly_deleted_rows, 1);
        assert_eq!(metrics.newly_invalidated_rows(), 3);
        assert_eq!(metrics.num_dv_added, 2);
        assert_eq!(metrics.num_dv_updated, 1);
        Ok(())
    }

    #[test]
    fn row_level_dv_rejects_update_delete_conflicts() {
        let mut bitmaps = RowLevelDvBitmaps::default();
        bitmaps.insert(DeletionVectorRowOperation::Update, 7);
        bitmaps.insert(DeletionVectorRowOperation::Delete, 7);

        let result = reconcile_row_level_dv_bitmaps(
            "conflict.parquet",
            DeletionVectorBitmap::new(),
            &bitmaps,
        );
        assert!(matches!(
            result,
            Err(DataFusionError::Execution(message))
                if message.contains("both UPDATE and DELETE")
                    && message.contains("conflict.parquet")
        ));
    }

    #[test]
    fn merge_dv_operation_column_decodes_to_typed_operations() -> Result<()> {
        let operations = Int32Array::from(vec![
            RowLevelOperationType::MatchedUpdate.as_i32(),
            RowLevelOperationType::NotMatchedBySourceDelete.as_i32(),
        ]);
        assert_eq!(
            merge_deletion_vector_row_operation(&operations, 0)?,
            DeletionVectorRowOperation::Update
        );
        assert_eq!(
            merge_deletion_vector_row_operation(&operations, 1)?,
            DeletionVectorRowOperation::Delete
        );

        let operation = Int64Array::from(vec![i64::from(
            RowLevelOperationType::NotMatchedBySourceUpdate.as_i32(),
        )]);
        assert_eq!(
            merge_deletion_vector_row_operation(&operation, 0)?,
            DeletionVectorRowOperation::Update
        );
        Ok(())
    }
}
