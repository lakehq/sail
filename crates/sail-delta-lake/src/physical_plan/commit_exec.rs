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
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::StructType;
use delta_kernel::EngineData;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::kernel::models::{Action, Add, Metadata, Protocol, Remove, RemoveOptions};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, TableReference};
use crate::kernel::{DeltaOperation, SaveMode};
use crate::physical_plan::{current_timestamp_millis, CommitInfo};
use crate::storage::{get_object_store_from_context, StorageConfig};
use crate::table::{create_delta_table_with_object_store, open_table_with_object_store};

/// Physical execution node for Delta Lake commit operations
#[derive(Debug)]
pub struct DeltaCommitExec {
    /// The plan that produces action metadata (Add and Remove actions).
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    partition_columns: Vec<String>,
    table_exists: bool,
    sink_schema: SchemaRef,
    sink_mode: PhysicalSinkMode,
    cache: PlanProperties,
}

impl DeltaCommitExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<String>,
        table_exists: bool,
        sink_schema: SchemaRef,
        sink_mode: PhysicalSinkMode,
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
            sink_mode,
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

    pub fn sink_mode(&self) -> &PhysicalSinkMode {
        &self.sink_mode
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
            children[0].clone(),
            self.table_url.clone(),
            self.partition_columns.clone(),
            self.table_exists,
            self.sink_schema.clone(),
            self.sink_mode.clone(),
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

        let input_stream = self.input.execute(0, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let partition_columns = self.partition_columns.clone();
        let table_exists = self.table_exists;
        let sink_schema = self.sink_schema.clone();
        let sink_mode = self.sink_mode.clone();

        let schema = self.schema();
        let future = async move {
            let storage_config = StorageConfig;
            let object_store = get_object_store_from_context(&context, &table_url)?;

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
            let mut actions: Vec<Action> = Vec::new();
            let mut initial_actions: Vec<Action> = Vec::new();
            let mut operation: Option<DeltaOperation> = None;
            let mut data = input_stream;

            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;

                // Extract commit info from the single data column (skip empty/invalid rows)
                if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                    for i in 0..array.len() {
                        let s = array.value(i);
                        if s.trim().is_empty() {
                            continue;
                        }
                        let parsed: Result<CommitInfo, _> = serde_json::from_str(s);
                        let commit_info = match parsed {
                            Ok(ci) => ci,
                            Err(e) => {
                                return Err(DataFusionError::External(Box::new(e)));
                            }
                        };

                        total_rows += commit_info.row_count;
                        actions.extend(commit_info.actions);
                        if initial_actions.is_empty() {
                            initial_actions = commit_info.initial_actions;
                        }
                        if operation.is_none() {
                            operation = commit_info.operation;
                        }
                        has_data = true;
                        break;
                    }
                }
            }

            if !has_data {
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            // Handle full table overwrite
            if matches!(sink_mode, PhysicalSinkMode::Overwrite) && table_exists {
                let snapshot = table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let all_files: Vec<Add> = snapshot
                    .snapshot()
                    .files(table.log_store().as_ref(), None)
                    .map_ok(|view| view.add_action())
                    .try_collect()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let deletion_timestamp = current_timestamp_millis()?;
                let remove_actions = all_files
                    .into_iter()
                    .map(|add| {
                        Action::Remove(add.into_remove_with_options(
                            deletion_timestamp,
                            RemoveOptions {
                                extended_file_metadata: Some(true),
                                include_tags: false,
                            },
                        ))
                    })
                    .collect::<Vec<_>>();
                actions.extend(remove_actions);
            }

            // Prepend initial actions
            let mut final_actions = initial_actions;
            final_actions.extend(actions.clone());
            let kinds: Vec<&'static str> = final_actions
                .iter()
                .map(|a| match a {
                    Action::Protocol(_) => "Protocol",
                    Action::Metadata(_) => "Metadata",
                    Action::Add(_) => "Add",
                    Action::Remove(_) => "Remove",
                    _ => "Other",
                })
                .collect();
            log::trace!(
                "final_actions_len: {}, final_action_kinds: {:?}",
                final_actions.len(),
                &kinds
            );

            if final_actions.is_empty() && !table_exists {
                // For new tables, add protocol and metadata even if no data
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            } else if final_actions.is_empty() {
                // For existing tables, no actions means no changes
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            // For new tables, always ensure Protocol + Metadata are present and use Create.
            // Even if the writer supplied an operation (e.g., Overwrite), the first commit
            // must initialize the table with protocol and metadata.
            let (operation, final_actions) = if !table_exists {
                let protocol_in_actions = final_actions.iter().find_map(|a| match a {
                    Action::Protocol(p) => Some(p.clone()),
                    _ => None,
                });
                let metadata_in_actions = final_actions.iter().find_map(|a| match a {
                    Action::Metadata(m) => Some(m.clone()),
                    _ => None,
                });

                if let (Some(protocol), Some(metadata)) = (protocol_in_actions, metadata_in_actions)
                {
                    (
                        DeltaOperation::Create {
                            mode: SaveMode::ErrorIfExists,
                            location: table_url.to_string(),
                            protocol,
                            metadata,
                        },
                        final_actions,
                    )
                } else {
                    // Construct minimal protocol/metadata and insert them
                    let delta_schema: StructType = sink_schema
                        .as_ref()
                        .try_into_kernel()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let protocol_json = serde_json::json!({
                        "minReaderVersion": 1,
                        "minWriterVersion": 2,
                    });
                    #[allow(clippy::unwrap_used)]
                    let protocol: Protocol = serde_json::from_value(protocol_json).unwrap();

                    let configuration: HashMap<String, String> = HashMap::new();
                    let metadata = Metadata::try_new(
                        None,
                        None,
                        delta_schema.clone(),
                        partition_columns.to_vec(),
                        Utc::now().timestamp_millis(),
                        configuration,
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let mut updated_actions = final_actions;
                    // Insert in order: Protocol, then Metadata
                    updated_actions.insert(0, Action::Metadata(metadata.clone()));
                    updated_actions.insert(0, Action::Protocol(protocol.clone()));

                    (
                        DeltaOperation::Create {
                            mode: SaveMode::ErrorIfExists,
                            location: table_url.to_string(),
                            protocol,
                            metadata,
                        },
                        updated_actions,
                    )
                }
            } else {
                (
                    operation.clone().unwrap_or(DeltaOperation::Write {
                        mode: SaveMode::Append,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.to_vec())
                        },
                        predicate: None,
                    }),
                    final_actions,
                )
            };

            // FIXME: Hybrid Transaction Strategy
            let use_kernel_txn = table_exists && is_dml_operation(&final_actions);

            if use_kernel_txn {
                let snapshot = table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                commit_with_kernel_transaction(
                    snapshot,
                    &final_actions,
                    &operation,
                    table.log_store().as_ref(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            } else {
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

                let session_state = SessionStateBuilder::new()
                    .with_runtime_env(context.runtime_env().clone())
                    .with_config(context.session_config().clone())
                    .build();

                CommitBuilder::from(CommitProperties::default())
                    .with_actions(final_actions)
                    .build(reference, table.log_store(), operation, &session_state)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

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

fn is_dml_operation(actions: &[Action]) -> bool {
    let has_protocol_or_metadata = actions
        .iter()
        .any(|a| matches!(a, Action::Protocol(_) | Action::Metadata(_)));

    if has_protocol_or_metadata {
        return false;
    }

    let has_add_or_remove = actions
        .iter()
        .any(|a| matches!(a, Action::Add(_) | Action::Remove(_)));

    has_add_or_remove
}

async fn commit_with_kernel_transaction(
    snapshot: &crate::kernel::snapshot::EagerSnapshot,
    actions: &[Action],
    operation: &DeltaOperation,
    log_store: &dyn crate::storage::LogStore,
) -> Result<(), crate::kernel::DeltaTableError> {
    use crate::kernel::DeltaTableError;

    let kernel_snapshot = snapshot.snapshot().inner.clone();
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = kernel_snapshot
        .transaction(committer)
        .map_err(|e| DeltaTableError::generic(e.to_string()))?;

    let operation_str = operation.name();
    txn = txn.with_operation(operation_str.to_string());

    let add_actions: Vec<&Add> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Add(add) => Some(add),
            _ => None,
        })
        .collect();

    let remove_actions: Vec<&Remove> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Remove(remove) => Some(remove),
            _ => None,
        })
        .collect();

    if !add_actions.is_empty() {
        let add_data = convert_adds_to_engine_data(&add_actions, txn.add_files_schema())?;
        txn.add_files(add_data);
    }

    if !remove_actions.is_empty() {
        let remove_data = convert_removes_to_filtered_data(&remove_actions, snapshot)?;
        txn.remove_files(remove_data);
    }

    let engine = log_store.engine(None);
    let result = txn
        .commit(engine.as_ref())
        .map_err(|e| DeltaTableError::generic(e.to_string()))?;

    match result {
        delta_kernel::transaction::CommitResult::CommittedTransaction(_) => Ok(()),
        delta_kernel::transaction::CommitResult::ConflictedTransaction(conflict) => {
            Err(DeltaTableError::generic(format!(
                "Transaction conflict at version {}",
                conflict.conflict_version()
            )))
        }
        delta_kernel::transaction::CommitResult::RetryableTransaction(retryable) => Err(
            DeltaTableError::generic(format!("Retryable transaction error: {}", retryable.error)),
        ),
    }
}

fn convert_adds_to_engine_data(
    adds: &[&Add],
    schema: &delta_kernel::schema::SchemaRef,
) -> Result<Box<dyn EngineData>, crate::kernel::DeltaTableError> {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Int64Array, MapBuilder, MapFieldNames, RecordBatch, StringBuilder, StructBuilder,
    };
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};

    use crate::kernel::DeltaTableError;

    let arrow_schema: ArrowSchema = schema
        .as_ref()
        .try_into_arrow()
        .map_err(|e| DeltaTableError::generic(format!("Failed to convert schema: {}", e)))?;

    let mut path_builder = StringBuilder::new();
    let map_field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut partition_values_builder = MapBuilder::new(
        Some(map_field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut size_builder = Int64Array::builder(adds.len());
    let mut modification_time_builder = Int64Array::builder(adds.len());
    let mut stats_num_records_builder = Int64Array::builder(adds.len());

    for add in adds {
        path_builder.append_value(&add.path);

        for (key, value) in &add.partition_values {
            partition_values_builder.keys().append_value(key);
            if let Some(v) = value {
                partition_values_builder.values().append_value(v);
            } else {
                partition_values_builder.values().append_null();
            }
        }
        partition_values_builder.append(true).map_err(|e| {
            DeltaTableError::generic(format!("Failed to append partition values: {}", e))
        })?;

        size_builder.append_value(add.size);
        modification_time_builder.append_value(add.modification_time);

        let num_records = if let Some(stats_str) = &add.stats {
            serde_json::from_str::<serde_json::Value>(stats_str)
                .ok()
                .and_then(|v| v.get("numRecords")?.as_i64())
                .unwrap_or(0)
        } else {
            0
        };
        stats_num_records_builder.append_value(num_records);
    }

    let path_array: ArrayRef = Arc::new(path_builder.finish());
    let partition_values_array: ArrayRef = Arc::new(partition_values_builder.finish());
    let size_array: ArrayRef = Arc::new(size_builder.finish());
    let modification_time_array: ArrayRef = Arc::new(modification_time_builder.finish());

    let mut stats_builder = StructBuilder::new(
        vec![Field::new("numRecords", ArrowDataType::Int64, true)],
        vec![Box::new(stats_num_records_builder)],
    );
    for _ in 0..adds.len() {
        stats_builder.append(true);
    }
    let stats_array: ArrayRef = Arc::new(stats_builder.finish());

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            path_array,
            partition_values_array,
            size_array,
            modification_time_array,
            stats_array,
        ],
    )
    .map_err(|e| DeltaTableError::generic(format!("Failed to create record batch: {}", e)))?;

    Ok(Box::new(ArrowEngineData::new(batch)))
}

fn convert_removes_to_filtered_data(
    removes: &[&Remove],
    snapshot: &crate::kernel::snapshot::EagerSnapshot,
) -> Result<delta_kernel::engine_data::FilteredEngineData, crate::kernel::DeltaTableError> {
    use std::collections::HashSet;
    use std::sync::Arc;

    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::Schema as ArrowSchema;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::scan::scan_row_schema;

    use crate::kernel::DeltaTableError;

    let files_batch = &snapshot.files;

    let path_column = files_batch
        .column_by_name("path")
        .ok_or_else(|| DeltaTableError::generic("path column not found in snapshot files"))?;

    let path_array = path_column
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .ok_or_else(|| DeltaTableError::generic("path column is not a string array"))?;

    let remove_paths: HashSet<&str> = removes.iter().map(|r| r.path.as_str()).collect();

    let mut selection_vector = Vec::with_capacity(files_batch.num_rows());
    for i in 0..files_batch.num_rows() {
        let file_path = path_array.value(i);
        selection_vector.push(remove_paths.contains(file_path));
    }

    let has_any_selected = selection_vector.iter().any(|&selected| selected);
    if !has_any_selected {
        return Err(DeltaTableError::generic(
            "No files found in snapshot matching remove actions",
        ));
    }

    let kernel_scan_schema: ArrowSchema = scan_row_schema()
        .as_ref()
        .try_into_arrow()
        .map_err(|e| DeltaTableError::generic(format!("Failed to convert scan schema: {}", e)))?;

    let mut columns = Vec::new();
    for field in kernel_scan_schema.fields() {
        let column = files_batch.column_by_name(field.name()).ok_or_else(|| {
            DeltaTableError::generic(format!(
                "Required column '{}' not found in snapshot files",
                field.name()
            ))
        })?;
        columns.push(column.clone());
    }

    let kernel_batch = RecordBatch::try_new(Arc::new(kernel_scan_schema), columns)
        .map_err(|e| DeltaTableError::generic(format!("Failed to create kernel batch: {}", e)))?;

    let engine_data = Box::new(ArrowEngineData::new(kernel_batch));
    delta_kernel::engine_data::FilteredEngineData::try_new(engine_data, selection_vector)
        .map_err(|e| DeltaTableError::generic(e.to_string()))
}
