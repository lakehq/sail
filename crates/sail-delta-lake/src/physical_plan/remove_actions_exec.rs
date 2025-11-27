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
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::{self, StreamExt};

use crate::kernel::models::{Action, Add, RemoveOptions};
use crate::physical_plan::{current_timestamp_millis, CommitInfo};

/// Physical execution node to convert Add actions (from FindFiles) into Remove actions
#[derive(Debug)]
pub struct DeltaRemoveActionsExec {
    input: Arc<dyn ExecutionPlan>,
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
        Self { input, cache }
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

        let future = async move {
            let mut adds_to_remove = vec![];

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
                    adds_to_remove.push(add);
                }
            }

            let remove_actions = Self::create_remove_actions(adds_to_remove).await?;

            let commit_info = CommitInfo {
                row_count: 0,
                actions: remove_actions,
                initial_actions: Vec::new(),
                operation: None,
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
