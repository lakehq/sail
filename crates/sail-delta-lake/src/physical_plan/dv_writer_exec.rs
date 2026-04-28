//! Physical execution node for Merge-on-Read deletion vector writing.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use futures::stream::{self, StreamExt};
use url::Url;

use crate::deletion_vector::{DeletionVectorBitmap, DeletionVectorWriter};
use crate::kernel::transaction::OperationMetrics;
use crate::physical_plan::{
    current_timestamp_millis, decode_adds_from_batch, delta_action_schema, encode_actions,
    meta_adds, ExecCommitMeta, COL_ACTION,
};
use crate::spec::{Action, Add, RemoveOptions};

/// Update an Add action's stats to reflect that the bounds are now wide (non-tight)
/// because a Deletion Vector has been added or updated.
///
/// When a DV is present, min/max statistics may include values
/// from logically-deleted rows, so `tightBounds` must be set to `false`.
fn widen_stats_bounds(stats_json: Option<&str>) -> Option<String> {
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
    /// The delta operation to record in the commit log.
    operation: Option<crate::kernel::DeltaOperation>,
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
        operation: Option<crate::kernel::DeltaOperation>,
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

    pub fn operation(&self) -> Option<&crate::kernel::DeltaOperation> {
        self.operation.as_ref()
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

    fn as_any(&self) -> &dyn Any {
        self
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
                    adds_to_process.extend(meta_adds::decode_adds_from_meta_batch(&batch, None)?);
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

            for add in &adds_to_process {
                let matching_rows = scan_file_for_matching_rows(
                    add,
                    &table_url,
                    &table_schema,
                    &condition,
                    &context,
                )
                .await?;

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
                num_removed_files: Some(num_affected_files),
                num_added_files: Some(num_affected_files),
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
        &add.path
    ));
    let file_size = add.size as u64;
    let partitioned_file = PartitionedFile::new(file_location.to_string(), file_size);

    let parquet_source = ParquetSource::new(Arc::clone(table_schema));
    let file_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
        Arc::new(parquet_source);

    let file_group = FileGroup::from(vec![partitioned_file]);
    let file_scan_config = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(vec![file_group])
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
