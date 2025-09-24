use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use deltalake::kernel::Action;
use deltalake::logstore::StorageConfig;
use deltalake::protocol::DeltaOperation;
use futures::{stream, StreamExt};
use url::Url;
use uuid::Uuid;

use crate::delta_datafusion::schema_rewriter::DeltaPhysicalExprAdapterFactory;
use crate::delta_format::{CommitInfo, DeltaRemoveActionsExec};
use crate::kernel::transaction::PROTOCOL;
use crate::operations::write::execution::{prepare_predicate_actions_physical, WriterStatsConfig};
use crate::table::open_table_with_object_store;

/// Physical execution node for Delta Lake delete operations
#[derive(Debug)]
pub struct DeltaDeleteExec {
    input: Arc<dyn ExecutionPlan>,
    condition: Arc<dyn PhysicalExpr>,
    table_url: Url,          // Keep for opening table
    table_schema: SchemaRef, // Schema of the table for condition evaluation
    cache: PlanProperties,
}

impl DeltaDeleteExec {
    /// Create a new DeltaDeleteExec instance
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        condition: Arc<dyn PhysicalExpr>,
        table_schema: SchemaRef,
    ) -> Self {
        // Output schema matches DeltaWriterExec: single column "data" containing CommitInfo JSON
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            condition,
            table_schema,
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

    /// Generate delete actions from candidate files
    async fn generate_delete_actions(
        &self,
        context: Arc<TaskContext>,
        candidate_adds: Vec<deltalake::kernel::Add>,
        partition_scan: bool,
    ) -> Result<Vec<Action>> {
        if partition_scan {
            // Partition scan: only need to generate Remove actions
            DeltaRemoveActionsExec::create_remove_actions(candidate_adds).await
        } else {
            // File scan: need to rewrite files, generate Add and Remove actions
            self.generate_file_rewrite_actions(context, candidate_adds)
                .await
        }
    }

    /// Generate actions for file rewrite (non-partition scan)
    async fn generate_file_rewrite_actions(
        &self,
        context: Arc<TaskContext>,
        candidate_adds: Vec<deltalake::kernel::Add>,
    ) -> Result<Vec<Action>> {
        let object_store = context
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let table = open_table_with_object_store(
            self.table_url.clone(),
            object_store,
            StorageConfig::default(),
        )
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

        let operation_id = Uuid::new_v4();
        let deletion_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .as_millis() as i64;

        // File-level scan: need to rewrite files with filtered data
        let writer_stats_config = WriterStatsConfig::new(32, None);
        let partition_columns = snapshot.metadata().partition_columns().clone();
        let session_state = SessionStateBuilder::new()
            .with_runtime_env(context.runtime_env().clone())
            .build();
        let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory {});

        // TODO: Turn `prepare_predicate_actions_physical` into an `ExecutionPlan` node.
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
            false, // partition_scan = false for file rewrite
            adapter_factory.clone(),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(predicate_actions)
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

        let mut stream = self.input.execute(0, context.clone())?;
        let schema = self.schema();
        let self_clone = self.clone();

        let future = async move {
            // Collect candidate files from input (DeltaFindFilesExec)
            let mut candidate_adds = vec![];
            let mut partition_scan = true; // Default to true if no rows

            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;

                if batch.num_rows() == 0 {
                    continue;
                }

                // Extract partition_scan flag
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
                partition_scan = scan_col.value(0);

                // Extract Add actions
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

            // Generate Actions
            let actions = if candidate_adds.is_empty() {
                vec![]
            } else {
                self_clone
                    .generate_delete_actions(context, candidate_adds, partition_scan)
                    .await?
            };

            // Package actions into CommitInfo
            let operation = DeltaOperation::Delete {
                predicate: Some(format!("{}", fmt_sql(self_clone.condition.as_ref()))),
            };

            let commit_info = CommitInfo {
                row_count: 0, // TODO: Calculate actual deleted rows
                actions,
                initial_actions: Vec::new(),
                operation: Some(operation),
            };

            let json = serde_json::to_string(&commit_info)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let data_array = Arc::new(StringArray::from(vec![json]));

            RecordBatch::try_new(schema, vec![data_array])
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl Clone for DeltaDeleteExec {
    fn clone(&self) -> Self {
        Self::new(
            self.input.clone(),
            self.table_url.clone(),
            self.condition.clone(),
            self.table_schema.clone(),
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
