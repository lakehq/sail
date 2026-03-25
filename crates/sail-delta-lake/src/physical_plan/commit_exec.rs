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
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::{self, StreamExt};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::kernel::transaction::{CommitBuilder, CommitProperties, OperationMetrics};
use crate::kernel::{DeltaOperation, SaveMode};
use crate::physical_plan::action_schema::ExecCommitMeta;
use crate::physical_plan::{decode_actions_and_meta_from_batch, COL_ACTION};
use crate::schema::{
    metadata_for_create_with_struct_type, normalize_delta_schema, protocol_for_create,
};
use crate::spec::{Action, StructType};
use crate::storage::{get_object_store_from_context, StorageConfig};
use crate::table::{create_delta_table_with_object_store, open_table_with_object_store};

const METRIC_NUM_COMMIT_RETRIES: &str = "num_commit_retries";
const METRIC_CHECKPOINT_CREATED: &str = "checkpoint_created";
const METRIC_LOG_FILES_CLEANED: &str = "log_files_cleaned";

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
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
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
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let plan_metrics = self.metrics.clone();

        let table_url = self.table_url.clone();
        let partition_columns = self.partition_columns.clone();
        let table_exists = self.table_exists;
        let sink_schema = self.sink_schema.clone();
        let schema = self.schema();
        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
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
            // "data" actions (Add/Remove/other) and "initial" actions (Protocol/Metadata)
            // are kept separate so we can preserve the required action ordering on commit.
            let mut actions: Vec<Action> = Vec::new();
            let mut initial_actions: Vec<Action> = Vec::new();
            let mut operation: Option<DeltaOperation> = None;
            let mut operation_metrics = OperationMetrics::default();
            let mut data = input_stream;

            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;

                // Arrow-native action rows + optional CommitMeta row only.
                if batch.column_by_name(COL_ACTION).is_some() {
                    let (decoded_actions, decoded_meta) =
                        decode_actions_and_meta_from_batch(&batch)?;
                    for a in decoded_actions {
                        match a {
                            Action::Protocol(_) | Action::Metadata(_) => initial_actions.push(a),
                            _ => actions.push(a),
                        }
                    }
                    if let Some(ExecCommitMeta {
                        row_count,
                        operation: op,
                        operation_metrics: metrics,
                    }) = decoded_meta
                    {
                        total_rows = total_rows.saturating_add(row_count);
                        if operation.is_none() {
                            operation = op;
                        }
                        operation_metrics.merge(metrics);
                    }
                    has_data = has_data || batch.num_rows() > 0;
                } else {
                    return Err(DataFusionError::Plan(
                        "DeltaCommitExec input must be delta action rows (action_type: UInt8)"
                            .to_string(),
                    ));
                }
            }

            if !has_data {
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            // Prepend initial actions
            let mut final_actions = initial_actions;
            final_actions.extend(actions);
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
                            protocol: Box::new(protocol),
                            metadata: Box::new(metadata),
                        },
                        final_actions,
                    )
                } else {
                    // Construct minimal protocol/metadata and insert them
                    let normalized_sink = normalize_delta_schema(&sink_schema);
                    let protocol = protocol_for_create(false, false, false)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let metadata = metadata_for_create_with_struct_type(
                        StructType::try_from(normalized_sink.as_ref())
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                        partition_columns.to_vec(),
                        Utc::now().timestamp_millis(),
                        HashMap::new(),
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
                            protocol: Box::new(protocol),
                            metadata: Box::new(metadata),
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

            let snapshot = if table_exists {
                Some(
                    table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                )
            } else {
                None
            };
            let reference = snapshot.cloned();

            let finalized_commit = CommitBuilder::from(
                CommitProperties::default().with_operation_metrics(operation_metrics),
            )
            .with_actions(final_actions)
            .build(reference, table.log_store(), operation)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let retries =
                usize::try_from(finalized_commit.metrics.num_retries).unwrap_or(usize::MAX);
            MetricBuilder::new(&plan_metrics)
                .global_counter(METRIC_NUM_COMMIT_RETRIES)
                .add(retries);

            if finalized_commit.metrics.new_checkpoint_created {
                MetricBuilder::new(&plan_metrics)
                    .global_counter(METRIC_CHECKPOINT_CREATED)
                    .add(1);
            }

            let cleaned = usize::try_from(finalized_commit.metrics.num_log_files_cleaned_up)
                .unwrap_or(usize::MAX);
            MetricBuilder::new(&plan_metrics)
                .global_counter(METRIC_LOG_FILES_CLEANED)
                .add(cleaned);

            // Expose row count through execution metrics as well.
            output_rows.add(usize::try_from(total_rows).unwrap_or(usize::MAX));

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
