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
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::{self, StreamExt};
use serde_json::Value;

use crate::kernel::models::{Action, Add, RemoveOptions};
use crate::physical_plan::{current_timestamp_millis, CommitInfo};

/// Physical execution node to convert Add actions (from FindFiles) into Remove actions
#[derive(Debug)]
pub struct DeltaRemoveActionsExec {
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl DeltaRemoveActionsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // Output schema must match DeltaWriterExec output schema
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)]));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    pub(crate) async fn create_remove_actions(adds: Vec<Add>) -> Result<Vec<Action>> {
        let deletion_timestamp = current_timestamp_millis()?;

        Ok(adds
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
            .collect())
    }
}

impl DisplayAs for DeltaRemoveActionsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaRemoveActionsExec")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaRemoveActionsExec")
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for DeltaRemoveActionsExec {
    fn name(&self) -> &'static str {
        "DeltaRemoveActionsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
            return internal_err!("DeltaRemoveActionsExec requires exactly one child");
        }
        Ok(Arc::new(DeltaRemoveActionsExec::new(children[0].clone())))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaRemoveActionsExec only supports single partition");
        }

        let mut stream = self.input.execute(0, context)?;
        let schema = self.schema();

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let output_bytes = MetricBuilder::new(&self.metrics).output_bytes(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
            let exec_start = Instant::now();
            let mut adds_to_remove = vec![];
            let mut num_removed_bytes: u64 = 0;

            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;

                // The input should have an "add" column containing JSON-serialized Add actions
                let adds_col = batch
                    .column_by_name("add")
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected 'add' column in input batch".to_string(),
                        )
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected StringArray for 'add' column".to_string(),
                        )
                    })?;

                for add_json in adds_col.iter().flatten() {
                    if add_json.trim().is_empty() {
                        continue;
                    }
                    let add: Add = serde_json::from_str(add_json)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    num_removed_bytes = num_removed_bytes
                        .saturating_add(u64::try_from(add.size).unwrap_or_default());
                    adds_to_remove.push(add);
                }
            }

            let num_removed_files: u64 = adds_to_remove.len() as u64;
            let remove_actions = Self::create_remove_actions(adds_to_remove).await?;

            // For execution metrics, treat removed files/bytes as this node's "output".
            output_rows.add(usize::try_from(num_removed_files).unwrap_or(usize::MAX));
            output_bytes.add(usize::try_from(num_removed_bytes).unwrap_or(usize::MAX));

            let mut operation_metrics: HashMap<String, Value> = HashMap::new();
            operation_metrics.insert(
                "numRemovedFiles".to_string(),
                Value::from(num_removed_files),
            );
            operation_metrics.insert(
                "numRemovedBytes".to_string(),
                Value::from(num_removed_bytes),
            );
            operation_metrics.insert(
                "executionTimeMs".to_string(),
                Value::from(exec_start.elapsed().as_millis() as u64),
            );

            let commit_info = CommitInfo {
                row_count: 0,
                actions: remove_actions,
                initial_actions: Vec::new(),
                operation: None,
                operation_metrics,
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
