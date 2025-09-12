use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use deltalake::kernel::{Action, Remove};
use deltalake::logstore::StorageConfig;
use deltalake::protocol::DeltaOperation;
use futures::{stream, StreamExt};
use url::Url;
use uuid::Uuid;

use crate::delta_datafusion::schema_rewriter::DeltaPhysicalExprAdapterFactory;
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL};
use crate::operations::write::execution::{prepare_predicate_actions_physical, WriterStatsConfig};
use crate::table::open_table_with_object_store;

/// Physical execution node for Delta Lake delete operations
#[derive(Debug)]
pub struct DeltaDeleteExec {
    input: Arc<dyn ExecutionPlan>,
    condition: Arc<dyn PhysicalExpr>,
    table_url: Url,          // Keep for opening table
    table_schema: SchemaRef, // Schema of the table for condition evaluation
    version: i64,
    cache: PlanProperties,
}

impl DeltaDeleteExec {
    /// Create a new DeltaDeleteExec instance
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        condition: Arc<dyn PhysicalExpr>,
        table_schema: SchemaRef,
        version: i64,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("num_deleted_rows", DataType::UInt64, false),
            Field::new("num_added_files", DataType::UInt64, false),
            Field::new("num_removed_files", DataType::UInt64, false),
        ]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            condition,
            table_schema,
            version,
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    /// Get the table URL
    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    /// Get the delete condition
    pub fn condition(&self) -> &Arc<dyn PhysicalExpr> {
        &self.condition
    }

    /// Get the input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the table schema
    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    /// Get the table version
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Core execution logic for delete operation
    async fn execute_delete(&self, context: Arc<TaskContext>) -> Result<RecordBatch> {
        let object_store = context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut table = open_table_with_object_store(
            self.table_url.clone(),
            object_store,
            StorageConfig::default(),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        table
            .load_version(self.version)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();

        // Check append-only constraint
        PROTOCOL
            .check_append_only(&snapshot.snapshot)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Execute find files plan
        let mut stream = self.input.execute(0, context.clone())?;
        let mut candidate_adds = vec![];
        let mut partition_scan = None;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;

            if batch.num_rows() == 0 {
                continue;
            }

            if partition_scan.is_none() {
                let scan_col = batch
                    .column_by_name("partition_scan")
                    .ok_or_else(|| {
                        DataFusionError::Internal("partition_scan column not found".to_string())
                    })?
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast partition_scan column to BooleanArray".to_string(),
                        )
                    })?;
                partition_scan = Some(scan_col.value(0));
            }

            let adds_col = batch
                .column_by_name("add")
                .ok_or_else(|| DataFusionError::Internal("add column not found".to_string()))?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Failed to downcast add column to StringArray".to_string(),
                    )
                })?;

            for i in 0..adds_col.len() {
                let add_json = adds_col.value(i);
                let add: deltalake::kernel::Add = serde_json::from_str(add_json)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                candidate_adds.push(add);
            }
        }

        let partition_scan = partition_scan.unwrap_or(true); // Default to true if no rows

        if candidate_adds.is_empty() {
            // No files match the condition, return zeros
            let result_batch = RecordBatch::try_new(
                self.schema(),
                vec![
                    Arc::new(UInt64Array::from(vec![0u64])), // num_deleted_rows
                    Arc::new(UInt64Array::from(vec![0u64])), // num_added_files
                    Arc::new(UInt64Array::from(vec![0u64])), // num_removed_files
                ],
            )?;
            return Ok(result_batch);
        }

        let operation_id = Uuid::new_v4();
        let deletion_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .as_millis() as i64;

        let num_deleted_rows = 0u64; // TODO: Calculate actual deleted rows
        let mut actions: Vec<Action> = Vec::new();

        if partition_scan {
            // Partition-only scan: we can only remove entire files
            // TODO: We cannot calculate exact deleted rows in this case
            let num_removed_files = candidate_adds.len();

            // Create Remove actions for all candidate files
            for file_to_remove in candidate_adds {
                actions.push(Action::Remove(Remove {
                    path: file_to_remove.path,
                    deletion_timestamp: Some(deletion_timestamp),
                    data_change: true,
                    extended_file_metadata: Some(true),
                    partition_values: Some(file_to_remove.partition_values),
                    size: Some(file_to_remove.size),
                    deletion_vector: file_to_remove.deletion_vector,
                    tags: None,
                    base_row_id: file_to_remove.base_row_id,
                    default_row_commit_version: file_to_remove.default_row_commit_version,
                }));
            }

            // TODO: For partition-only scans, we estimate deleted rows as 0
            // since we can't easily calculate without scanning the files

            let result_batch = RecordBatch::try_new(
                self.schema(),
                vec![
                    Arc::new(UInt64Array::from(vec![num_deleted_rows])),
                    Arc::new(UInt64Array::from(vec![0u64])), // num_added_files (no rewrite)
                    Arc::new(UInt64Array::from(vec![num_removed_files as u64])),
                ],
            )?;

            // Commit the transaction
            if !actions.is_empty() {
                let operation = DeltaOperation::Delete {
                    predicate: Some(format!("{:?}", self.condition)),
                };

                CommitBuilder::from(CommitProperties::default())
                    .with_actions(actions)
                    .build(Some(&snapshot), table.log_store(), operation)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

            Ok(result_batch)
        } else {
            // File-level scan: need to rewrite files with filtered data
            let writer_stats_config = WriterStatsConfig::new(32, None);
            let partition_columns = snapshot.metadata().partition_columns().clone();
            let session_state = SessionStateBuilder::new()
                .with_runtime_env(context.runtime_env().clone())
                .build();
            let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});

            let (predicate_actions, _cdf_df) = prepare_predicate_actions_physical(
                self.condition.clone(),
                table.log_store(),
                &snapshot,
                session_state,
                partition_columns,
                None, // writer_properties
                deletion_timestamp,
                writer_stats_config,
                operation_id,
                &candidate_adds,
                partition_scan,
                adapter_factory.clone(),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Count Add and Remove actions
            let mut num_added_files = 0u64;
            let mut num_removed_files = 0u64;

            for action in &predicate_actions {
                match action {
                    Action::Add(_) => num_added_files += 1,
                    Action::Remove(_) => num_removed_files += 1,
                    _ => {}
                }
            }

            let num_deleted_rows = 0u64; // TODO: Calculate from execution metrics

            let result_batch = RecordBatch::try_new(
                self.schema(),
                vec![
                    Arc::new(UInt64Array::from(vec![num_deleted_rows])),
                    Arc::new(UInt64Array::from(vec![num_added_files])),
                    Arc::new(UInt64Array::from(vec![num_removed_files])),
                ],
            )?;

            // Commit the transaction
            if !predicate_actions.is_empty() {
                let operation = DeltaOperation::Delete {
                    predicate: Some(format!("{:?}", self.condition)),
                };

                CommitBuilder::from(CommitProperties::default())
                    .with_actions(predicate_actions)
                    .build(Some(&snapshot), table.log_store(), operation)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

            Ok(result_batch)
        }
    }
}

#[async_trait]
impl ExecutionPlan for DeltaDeleteExec {
    fn name(&self) -> &'static str {
        "DeltaDeleteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaDeleteExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.condition.clone(),
            self.table_schema.clone(),
            self.version,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaDeleteExec can only be executed in a single partition");
        }

        let schema = self.schema();
        let delete_exec = self.clone();
        let future = async move { delete_exec.execute_delete(context).await };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl Clone for DeltaDeleteExec {
    fn clone(&self) -> Self {
        Self::new(
            self.input.clone(),
            self.table_url.clone(),
            self.condition.clone(),
            self.table_schema.clone(),
            self.version,
        )
    }
}

impl DisplayAs for DeltaDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaDeleteExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
