use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::schema::StructType;
use deltalake::kernel::{Action, Add, Protocol};
use deltalake::logstore::StorageConfig;
use deltalake::protocol::{DeltaOperation, SaveMode};
use futures::stream::{self, StreamExt};
use url::Url;

use crate::delta_format::CommitInfo;
use crate::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use crate::table::{create_delta_table_with_object_store, open_table_with_object_store};

/// Physical execution node for Delta Lake commit operations
#[derive(Debug)]
pub struct DeltaCommitExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    partition_columns: Vec<String>,
    table_exists: bool,
    sink_schema: SchemaRef,
    cache: PlanProperties,
}

impl DeltaCommitExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<String>,
        table_exists: bool,
        sink_schema: SchemaRef,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            true,
        )]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            partition_columns,
            table_exists,
            sink_schema,
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

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sink_schema(&self) -> &SchemaRef {
        &self.sink_schema
    }
}

#[async_trait]
impl ExecutionPlan for DeltaCommitExec {
    fn name(&self) -> &'static str {
        "DeltaCommitExec"
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

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaCommitExec requires exactly one child");
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.partition_columns.clone(),
            self.table_exists,
            self.sink_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaCommitExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "DeltaCommitExec requires exactly one input partition, got {}",
                input_partitions
            );
        }

        let stream = self.input.execute(0, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let partition_columns = self.partition_columns.clone();
        let table_exists = self.table_exists;
        let sink_schema = self.sink_schema.clone();

        let schema = self.schema();
        let future = async move {
            let storage_config = StorageConfig::default();
            let object_store = Self::get_object_store(&context, &table_url)?;

            let table = if table_exists {
                open_table_with_object_store(
                    table_url.clone(),
                    object_store.clone(),
                    storage_config.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                create_delta_table_with_object_store(
                    table_url.clone(),
                    object_store.clone(),
                    storage_config.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            let mut total_rows = 0u64;
            let mut has_data = false;
            let mut writer_add_actions: Vec<Add> = Vec::new();
            let mut schema_actions: Vec<Action> = Vec::new();
            let mut initial_actions: Vec<Action> = Vec::new();
            let mut operation: Option<DeltaOperation> = None;
            let mut data = stream;

            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;

                // Extract commit info from the single data column
                if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                    if array.len() > 0 {
                        let commit_info_json = array.value(0);
                        let commit_info: CommitInfo = serde_json::from_str(commit_info_json)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        total_rows += commit_info.row_count;
                        writer_add_actions.extend(commit_info.add_actions);
                        schema_actions.extend(commit_info.schema_actions);

                        if initial_actions.is_empty() {
                            initial_actions = commit_info.initial_actions;
                        }
                        if operation.is_none() {
                            operation = commit_info.operation;
                        }
                        has_data = true;
                    }
                }
            }

            if !has_data {
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            // Use the Add actions from the writer
            let mut actions: Vec<Action> = initial_actions;
            actions.extend(schema_actions); // Add schema actions first
            actions.extend(writer_add_actions.into_iter().map(Action::Add)); // Then add actions

            if actions.is_empty() && !table_exists {
                // For new tables, add protocol and metadata even if no data
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            } else if actions.is_empty() {
                // For existing tables, no actions means no changes
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let operation = if !table_exists {
                let delta_schema: StructType = sink_schema
                    .as_ref()
                    .try_into_kernel()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                #[allow(clippy::unwrap_used)]
                let protocol: Protocol = serde_json::from_value(serde_json::json!({
                    "minReaderVersion": 1,
                    "minWriterVersion": 2,
                }))
                .unwrap();

                #[allow(deprecated)]
                let metadata = deltalake::kernel::new_metadata(
                    &delta_schema,
                    partition_columns.to_vec(),
                    HashMap::<String, String>::new(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                actions.insert(0, Action::Protocol(protocol.clone()));
                actions.insert(1, Action::Metadata(metadata.clone()));

                DeltaOperation::Create {
                    mode: SaveMode::ErrorIfExists,
                    location: table_url.to_string(),
                    protocol,
                    metadata,
                }
            } else {
                operation.clone().unwrap_or(DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: if partition_columns.is_empty() {
                        None
                    } else {
                        Some(partition_columns.to_vec())
                    },
                    predicate: None,
                })
            };

            let snapshot = if table_exists {
                Some(
                    table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                )
            } else {
                None
            };
            let reference = snapshot.as_ref().map(|s| *s as &dyn TableReference);

            CommitBuilder::from(CommitProperties::default())
                .with_actions(actions)
                .build(reference, table.log_store(), operation)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let array = Arc::new(UInt64Array::from(vec![total_rows]));
            let batch = RecordBatch::try_new(schema, vec![array])?;
            Ok(batch)
        };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DeltaCommitExec {
    fn get_object_store(
        context: &Arc<TaskContext>,
        table_url: &Url,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        context
            .runtime_env()
            .object_store_registry
            .get_store(table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

impl DisplayAs for DeltaCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaCommitExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
