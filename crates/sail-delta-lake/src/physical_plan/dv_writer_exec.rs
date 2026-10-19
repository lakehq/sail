//! Physical execution nodes for row-level Merge-on-Read deletion vector writing.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
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
use url::Url;

use crate::deletion_vector::{DeletionVectorBitmap, DeletionVectorWriter};
use crate::physical_plan::{
    COL_ACTION, DeltaDecodePath, ExecCommitMeta, current_timestamp_millis, decode_adds_from_batch,
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
    metadata_path: Arc<dyn PhysicalExpr>,
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
        if matches!(operation_mode, DeletionVectorRowOperationMode::Mixed)
            || matches!(operation.as_ref(), Some(DeltaOperation::Merge { .. }))
        {
            input
                .schema()
                .index_of(OPERATION_COLUMN)
                .map_err(|e| DataFusionError::Plan(format!("{e}")))?;
        }
        let metadata_path = DeltaDecodePath::expression(&path_column, &adds_input.schema())?;

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
            metadata_path,
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
    MatchedDelete,
    NotMatchedBySourceDelete,
}

#[derive(Debug, Default)]
struct RowLevelDvBitmaps {
    update_bitmap: DeletionVectorBitmap,
    matched_delete_bitmap: DeletionVectorBitmap,
    not_matched_by_source_delete_bitmap: DeletionVectorBitmap,
}

impl RowLevelDvBitmaps {
    fn union_with(&mut self, bitmaps: &Self) {
        self.update_bitmap.union_with(&bitmaps.update_bitmap);
        self.matched_delete_bitmap
            .union_with(&bitmaps.matched_delete_bitmap);
        self.not_matched_by_source_delete_bitmap
            .union_with(&bitmaps.not_matched_by_source_delete_bitmap);
    }

    fn insert(&mut self, operation: DeletionVectorRowOperation, row_index: u64) {
        match operation {
            DeletionVectorRowOperation::Update => {
                self.update_bitmap.insert(row_index);
            }
            DeletionVectorRowOperation::MatchedDelete => {
                self.matched_delete_bitmap.insert(row_index);
            }
            DeletionVectorRowOperation::NotMatchedBySourceDelete => {
                self.not_matched_by_source_delete_bitmap.insert(row_index);
            }
        }
    }
}

struct ReconciledRowLevelDv {
    final_bitmap: DeletionVectorBitmap,
    newly_updated_rows: u64,
    newly_matched_deleted_rows: u64,
    newly_not_matched_by_source_deleted_rows: u64,
}

struct RowLevelDvWriteStats {
    newly_updated_rows: u64,
    newly_matched_deleted_rows: u64,
    newly_not_matched_by_source_deleted_rows: u64,
    had_existing_dv: bool,
}

#[derive(Default)]
struct RowLevelDvWriteMetrics {
    newly_updated_rows: u64,
    newly_matched_deleted_rows: u64,
    newly_not_matched_by_source_deleted_rows: u64,
    num_dv_added: u64,
    num_dv_updated: u64,
}

impl RowLevelDvWriteMetrics {
    fn record_file(&mut self, stats: RowLevelDvWriteStats) {
        self.newly_updated_rows = self
            .newly_updated_rows
            .saturating_add(stats.newly_updated_rows);
        self.newly_matched_deleted_rows = self
            .newly_matched_deleted_rows
            .saturating_add(stats.newly_matched_deleted_rows);
        self.newly_not_matched_by_source_deleted_rows = self
            .newly_not_matched_by_source_deleted_rows
            .saturating_add(stats.newly_not_matched_by_source_deleted_rows);
        self.num_dv_added = self.num_dv_added.saturating_add(1);
        if stats.had_existing_dv {
            self.num_dv_updated = self.num_dv_updated.saturating_add(1);
        }
    }

    fn newly_invalidated_rows(&self) -> u64 {
        self.newly_updated_rows
            .saturating_add(self.newly_deleted_rows())
    }

    fn newly_deleted_rows(&self) -> u64 {
        self.newly_matched_deleted_rows
            .saturating_add(self.newly_not_matched_by_source_deleted_rows)
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
        RowLevelOperationType::MatchedDelete => Ok(DeletionVectorRowOperation::MatchedDelete),
        RowLevelOperationType::NotMatchedBySourceDelete => {
            Ok(DeletionVectorRowOperation::NotMatchedBySourceDelete)
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
        .is_disjoint(bitmaps.matched_delete_bitmap.inner())
        || !bitmaps
            .update_bitmap
            .inner()
            .is_disjoint(bitmaps.not_matched_by_source_delete_bitmap.inner())
    {
        return Err(DataFusionError::Execution(format!(
            "row-level DV assigns the same row in file '{path}' to both UPDATE and DELETE"
        )));
    }
    if !bitmaps
        .matched_delete_bitmap
        .inner()
        .is_disjoint(bitmaps.not_matched_by_source_delete_bitmap.inner())
    {
        return Err(DataFusionError::Execution(format!(
            "row-level DV assigns the same row in file '{path}' to multiple DELETE categories"
        )));
    }

    let new_updates =
        DeletionVectorBitmap::from_treemap(bitmaps.update_bitmap.inner() - existing_bitmap.inner());
    let new_matched_deletes = DeletionVectorBitmap::from_treemap(
        bitmaps.matched_delete_bitmap.inner() - existing_bitmap.inner(),
    );
    let new_not_matched_by_source_deletes = DeletionVectorBitmap::from_treemap(
        bitmaps.not_matched_by_source_delete_bitmap.inner() - existing_bitmap.inner(),
    );

    existing_bitmap.union_with(&bitmaps.update_bitmap);
    existing_bitmap.union_with(&bitmaps.matched_delete_bitmap);
    existing_bitmap.union_with(&bitmaps.not_matched_by_source_delete_bitmap);

    Ok(ReconciledRowLevelDv {
        final_bitmap: existing_bitmap,
        newly_updated_rows: new_updates.len(),
        newly_matched_deleted_rows: new_matched_deletes.len(),
        newly_not_matched_by_source_deleted_rows: new_not_matched_by_source_deletes.len(),
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
        .saturating_add(reconciled.newly_matched_deleted_rows)
        .saturating_add(reconciled.newly_not_matched_by_source_deleted_rows)
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
        newly_matched_deleted_rows: reconciled.newly_matched_deleted_rows,
        newly_not_matched_by_source_deleted_rows: reconciled
            .newly_not_matched_by_source_deleted_rows,
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
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots([&self.metadata_path], f)
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
        vec![
            dist_for(&self.input),
            Distribution::KeyPartitioned(vec![Arc::clone(&self.metadata_path)]),
        ]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        if matches!(self.operation, Some(DeltaOperation::Delete { .. })) {
            return vec![None, None];
        }
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
        let classify_merge_operations =
            matches!(operation.as_ref(), Some(DeltaOperation::Merge { .. }));
        let collect_file_bitmaps =
            matches!(operation.as_ref(), Some(DeltaOperation::Delete { .. }));

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
            // DELETE scans may interleave files. Buffer compressed positions, not sorted rows.
            let mut file_bitmaps: HashMap<String, RowLevelDvBitmaps> = HashMap::new();

            let mut stream = input.execute(partition, context.clone())?;
            let mut scan_time_ms = 0u64;
            loop {
                let scan_start = Instant::now();
                let next = stream.next().await;
                scan_time_ms = scan_time_ms.saturating_add(scan_start.elapsed().as_millis() as u64);
                let Some(batch_result) = next else {
                    break;
                };
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
                let operation_column = if classify_merge_operations
                    || matches!(operation_mode, DeletionVectorRowOperationMode::Mixed)
                {
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
                        if collect_file_bitmaps {
                            file_bitmaps
                                .entry(flushed_path)
                                .and_modify(|bitmaps| bitmaps.union_with(&flushed_bitmaps))
                                .or_insert(flushed_bitmaps);
                        } else if let Some(stats) = write_row_level_dv_actions_for_path(
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
                    let row_operation = if classify_merge_operations
                        || matches!(operation_mode, DeletionVectorRowOperationMode::Mixed)
                    {
                        let operation_column = operation_column.ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "row-level MERGE DV input is missing required column '{OPERATION_COLUMN}'"
                            ))
                        })?;
                        merge_deletion_vector_row_operation(operation_column.as_ref(), row)?
                    } else {
                        match operation_mode {
                            DeletionVectorRowOperationMode::Update => {
                                DeletionVectorRowOperation::Update
                            }
                            DeletionVectorRowOperationMode::Delete => {
                                DeletionVectorRowOperation::MatchedDelete
                            }
                            DeletionVectorRowOperationMode::Mixed => {
                                return Err(DataFusionError::Internal(
                                    "mixed row-level DV operations require an operation column"
                                        .to_string(),
                                ));
                            }
                        }
                    };
                    current_bitmaps.insert(row_operation, row_index as u64);
                }
            }

            if let Some(path) = current_path {
                file_bitmaps
                    .entry(path)
                    .and_modify(|bitmaps| bitmaps.union_with(&current_bitmaps))
                    .or_insert(current_bitmaps);
            }
            for (path, bitmaps) in file_bitmaps {
                if let Some(stats) = write_row_level_dv_actions_for_path(
                    path,
                    bitmaps,
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
                write_metrics.newly_deleted_rows(),
            );

            let target_rows_deleted = write_metrics.newly_deleted_rows();
            let execution_time_ms = Some(exec_start.elapsed().as_millis() as u64);
            let (row_count, operation_metrics) = match operation.as_ref() {
                Some(DeltaOperation::Delete { .. }) => (
                    target_rows_deleted,
                    OperationMetrics {
                        execution_time_ms,
                        scan_time_ms: Some(scan_time_ms),
                        num_removed_files: Some(write_metrics.num_dv_added),
                        num_added_files: Some(write_metrics.num_dv_added),
                        num_deleted_rows: Some(target_rows_deleted),
                        num_copied_rows: Some(0),
                        num_deletion_vectors_added: Some(write_metrics.num_dv_added),
                        num_deletion_vectors_updated: Some(write_metrics.num_dv_updated),
                        num_deletion_vectors_removed: Some(write_metrics.num_dv_updated),
                        ..Default::default()
                    },
                ),
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
                        num_target_rows_matched_deleted: Some(
                            write_metrics.newly_matched_deleted_rows,
                        ),
                        num_target_rows_not_matched_by_source_deleted: Some(
                            write_metrics.newly_not_matched_by_source_deleted_rows,
                        ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delete_dv_writer_merges_interleaved_files_across_batches() -> Result<()> {
        use datafusion::arrow::array::{ArrayRef, RecordBatch};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use object_store::memory::InMemory;

        use crate::datasource::PATH_COLUMN;
        use crate::physical_plan::decode_actions_and_meta_from_batch;

        let first_path = "a+b%20c.parquet";
        let batches = [
            (vec![first_path, "b.parquet", first_path], vec![1, 2, 3]),
            (vec!["b.parquet", first_path], vec![4, 1]),
        ]
        .into_iter()
        .map(|(paths, indices)| {
            RecordBatch::try_from_iter(vec![
                (PATH_COLUMN, Arc::new(StringArray::from(paths)) as ArrayRef),
                ("row_index", Arc::new(Int64Array::from(indices)) as ArrayRef),
            ])
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
        let row_schema = batches[0].schema();
        let input = MemorySourceConfig::try_new_exec(&[batches], row_schema, None)?;
        let metadata = RecordBatch::try_from_iter(vec![(
            PATH_COLUMN,
            Arc::new(StringArray::from(vec!["a+b%2520c.parquet", "b.parquet"])) as ArrayRef,
        )])?;
        let adds_input =
            MemorySourceConfig::try_new_exec(&[vec![metadata.clone()]], metadata.schema(), None)?;
        let table_url = Url::parse("memory://dv/table/")
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let context = SessionContext::new();
        context.register_object_store(&table_url, Arc::clone(&store));
        let writer = DeletionVectorRowsWriterExec::new(
            input,
            adds_input,
            table_url.clone(),
            DeletionVectorRowsWriterConfig::new(
                PATH_COLUMN,
                "row_index",
                DeletionVectorRowOperationMode::Delete,
                0,
                Some(vec![]),
                Some(DeltaOperation::Delete { predicate: None }),
            ),
        )?;
        assert!(writer.required_input_ordering().iter().all(Option::is_none));
        let batches = collect(Arc::new(writer), context.task_ctx()).await?;
        let mut adds = HashMap::new();
        let mut removes = 0;
        let mut deleted_rows = 0;
        for batch in batches {
            let (actions, metadata) = decode_actions_and_meta_from_batch(&batch)?;
            for action in actions {
                match action {
                    Action::Add(add) => {
                        assert!(adds.insert(add.path.clone(), add).is_none());
                    }
                    Action::Remove(_) => removes += 1,
                    _ => return internal_err!("unexpected action from DV writer"),
                }
            }
            for metadata in metadata {
                deleted_rows += metadata.row_count;
            }
        }
        assert_eq!(removes, 2);
        assert_eq!(adds.len(), 2);
        assert_eq!(deleted_rows, 4);
        for (path, expected) in [(first_path, vec![1, 3]), ("b.parquet", vec![2, 4])] {
            let descriptor = adds
                .get(path)
                .and_then(|add| add.deletion_vector.as_ref())
                .ok_or_else(|| {
                    DataFusionError::Internal("missing output deletion vector".into())
                })?;
            let bitmap = crate::deletion_vector::read_deletion_vector(
                store.as_ref(),
                &table_url,
                descriptor,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            assert_eq!(bitmap.inner().iter().collect::<Vec<_>>(), expected);
        }
        Ok(())
    }

    #[test]
    fn row_level_dv_distribution_uses_decoded_metadata_paths() -> Result<()> {
        use datafusion::arrow::array::{ArrayRef, RecordBatch};
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion_common::cast::as_string_array;

        let decoded = "part=a+b%20c/file.parquet";
        let rows = RecordBatch::try_from_iter(vec![
            (
                "path",
                Arc::new(StringArray::from(vec![decoded])) as ArrayRef,
            ),
            ("row_index", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        ])?;
        let metadata = RecordBatch::try_from_iter(vec![(
            "path",
            Arc::new(StringArray::from(vec!["part=a+b%2520c/file.parquet"])) as ArrayRef,
        )])?;
        let writer = DeletionVectorRowsWriterExec::new(
            Arc::new(EmptyExec::new(rows.schema())),
            Arc::new(EmptyExec::new(metadata.schema())),
            Url::parse("file:///tmp/delta-table")
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            DeletionVectorRowsWriterConfig::new(
                "path",
                "row_index",
                DeletionVectorRowOperationMode::Update,
                0,
                None,
                None,
            ),
        )?;
        for (distribution, batch) in writer
            .input_distribution_requirements()
            .into_per_child()
            .iter()
            .zip([rows, metadata])
        {
            let Distribution::KeyPartitioned(expressions) = distribution else {
                return internal_err!("DV inputs must be partitioned by their file path");
            };
            assert_eq!(expressions.len(), 1);
            let paths = expressions[0].evaluate(&batch)?.into_array(1)?;
            assert_eq!(as_string_array(&paths)?, &StringArray::from(vec![decoded]));
        }
        Ok(())
    }

    #[test]
    fn row_level_dv_reconciliation_excludes_existing_and_duplicate_rows() -> Result<()> {
        let existing_bitmap = DeletionVectorBitmap::from_row_indices([1, 4]);
        let mut bitmaps = RowLevelDvBitmaps::default();
        bitmaps.insert(DeletionVectorRowOperation::Update, 1);
        bitmaps.insert(DeletionVectorRowOperation::Update, 2);
        bitmaps.insert(DeletionVectorRowOperation::Update, 2);
        bitmaps.insert(DeletionVectorRowOperation::MatchedDelete, 3);
        bitmaps.insert(DeletionVectorRowOperation::MatchedDelete, 3);
        bitmaps.insert(DeletionVectorRowOperation::NotMatchedBySourceDelete, 4);
        bitmaps.insert(DeletionVectorRowOperation::NotMatchedBySourceDelete, 5);

        let reconciled = reconcile_row_level_dv_bitmaps("part.parquet", existing_bitmap, &bitmaps)?;

        assert_eq!(reconciled.newly_updated_rows, 1);
        assert_eq!(reconciled.newly_matched_deleted_rows, 1);
        assert_eq!(reconciled.newly_not_matched_by_source_deleted_rows, 1);
        assert_eq!(reconciled.final_bitmap.len(), 5);
        for row_index in [1, 2, 3, 4, 5] {
            assert!(reconciled.final_bitmap.contains(row_index));
        }
        Ok(())
    }

    #[test]
    fn row_level_dv_metrics_are_aggregated_after_per_file_reconciliation() -> Result<()> {
        let mut first_file_bitmaps = RowLevelDvBitmaps::default();
        first_file_bitmaps.insert(DeletionVectorRowOperation::Update, 1);
        first_file_bitmaps.insert(DeletionVectorRowOperation::MatchedDelete, 2);
        let first_file = reconcile_row_level_dv_bitmaps(
            "first.parquet",
            DeletionVectorBitmap::from_row_indices([1]),
            &first_file_bitmaps,
        )?;

        let mut second_file_bitmaps = RowLevelDvBitmaps::default();
        second_file_bitmaps.insert(DeletionVectorRowOperation::Update, 5);
        second_file_bitmaps.insert(DeletionVectorRowOperation::Update, 6);
        second_file_bitmaps.insert(DeletionVectorRowOperation::NotMatchedBySourceDelete, 7);
        let second_file = reconcile_row_level_dv_bitmaps(
            "second.parquet",
            DeletionVectorBitmap::new(),
            &second_file_bitmaps,
        )?;

        let mut metrics = RowLevelDvWriteMetrics::default();
        metrics.record_file(RowLevelDvWriteStats {
            newly_updated_rows: first_file.newly_updated_rows,
            newly_matched_deleted_rows: first_file.newly_matched_deleted_rows,
            newly_not_matched_by_source_deleted_rows: first_file
                .newly_not_matched_by_source_deleted_rows,
            had_existing_dv: true,
        });
        metrics.record_file(RowLevelDvWriteStats {
            newly_updated_rows: second_file.newly_updated_rows,
            newly_matched_deleted_rows: second_file.newly_matched_deleted_rows,
            newly_not_matched_by_source_deleted_rows: second_file
                .newly_not_matched_by_source_deleted_rows,
            had_existing_dv: false,
        });

        assert_eq!(metrics.newly_updated_rows, 2);
        assert_eq!(metrics.newly_matched_deleted_rows, 1);
        assert_eq!(metrics.newly_not_matched_by_source_deleted_rows, 1);
        assert_eq!(metrics.newly_deleted_rows(), 2);
        assert_eq!(metrics.newly_invalidated_rows(), 4);
        assert_eq!(metrics.num_dv_added, 2);
        assert_eq!(metrics.num_dv_updated, 1);
        Ok(())
    }

    #[test]
    fn row_level_dv_rejects_update_delete_conflicts() {
        let mut bitmaps = RowLevelDvBitmaps::default();
        bitmaps.insert(DeletionVectorRowOperation::Update, 7);
        bitmaps.insert(DeletionVectorRowOperation::MatchedDelete, 7);

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
    fn row_level_dv_rejects_conflicting_delete_categories() {
        let mut bitmaps = RowLevelDvBitmaps::default();
        bitmaps.insert(DeletionVectorRowOperation::MatchedDelete, 7);
        bitmaps.insert(DeletionVectorRowOperation::NotMatchedBySourceDelete, 7);

        let result = reconcile_row_level_dv_bitmaps(
            "conflict.parquet",
            DeletionVectorBitmap::new(),
            &bitmaps,
        );
        assert!(matches!(
            result,
            Err(DataFusionError::Execution(message))
                if message.contains("multiple DELETE categories")
                    && message.contains("conflict.parquet")
        ));
    }

    #[test]
    fn merge_dv_operation_column_decodes_to_typed_operations() -> Result<()> {
        let operations = Int32Array::from(vec![
            RowLevelOperationType::MatchedUpdate.as_i32(),
            RowLevelOperationType::MatchedDelete.as_i32(),
            RowLevelOperationType::NotMatchedBySourceDelete.as_i32(),
        ]);
        assert_eq!(
            merge_deletion_vector_row_operation(&operations, 0)?,
            DeletionVectorRowOperation::Update
        );
        assert_eq!(
            merge_deletion_vector_row_operation(&operations, 1)?,
            DeletionVectorRowOperation::MatchedDelete
        );
        assert_eq!(
            merge_deletion_vector_row_operation(&operations, 2)?,
            DeletionVectorRowOperation::NotMatchedBySourceDelete
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
