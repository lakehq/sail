use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::TryStreamExt;

use crate::physical_plan::action_schema::{encode_removed_data_file_paths, iceberg_action_schema};

/// Converts a stream of touched Iceberg data-file paths into typed commit actions.
#[derive(Debug)]
pub struct IcebergRemoveDataFilesExec {
    input: Arc<dyn ExecutionPlan>,
    file_path_column: String,
    file_path_index: usize,
    cache: Arc<PlanProperties>,
}

impl IcebergRemoveDataFilesExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        file_path_column: impl Into<String>,
    ) -> Result<Self> {
        let file_path_column = file_path_column.into();
        let file_path_index = input.schema().index_of(&file_path_column).map_err(|_| {
            DataFusionError::Plan(format!(
                "Iceberg touched-file plan is missing path column '{file_path_column}'"
            ))
        })?;
        if input.schema().field(file_path_index).data_type() != &DataType::Utf8 {
            return Err(DataFusionError::Plan(format!(
                "Iceberg touched-file column '{file_path_column}' must be Utf8"
            )));
        }
        let output_schema = iceberg_action_schema()?;
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            file_path_column,
            file_path_index,
            cache,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn file_path_column(&self) -> &str {
        &self.file_path_column
    }
}

#[async_trait]
impl ExecutionPlan for IcebergRemoveDataFilesExec {
    fn name(&self) -> &'static str {
        "IcebergRemoveDataFilesExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("IcebergRemoveDataFilesExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            self.file_path_column.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let file_path_index = self.file_path_index;
        let file_path_column = self.file_path_column.clone();
        let schema = self.schema();
        let stream_schema = Arc::clone(&schema);
        let stream = try_stream! {
            while let Some(batch) = input.try_next().await? {
                let paths = batch
                    .column(file_path_index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal(format!(
                        "Iceberg touched-file column '{file_path_column}' is not Utf8"
                    )))?;
                let mut file_paths = Vec::with_capacity(paths.len());
                for row in 0..paths.len() {
                    if paths.is_null(row) {
                        Err(DataFusionError::Execution(format!(
                            "Iceberg touched-file column '{file_path_column}' cannot contain null"
                        )))?;
                    }
                    let path = paths.value(row);
                    if path.is_empty() {
                        Err(DataFusionError::Execution(
                            "Iceberg touched data-file path cannot be empty".to_string(),
                        ))?;
                    }
                    file_paths.push(path.to_string());
                }
                yield encode_removed_data_file_paths(file_paths)?;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

impl DisplayAs for IcebergRemoveDataFilesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(
                f,
                "IcebergRemoveDataFilesExec(file_path_column={})",
                self.file_path_column
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::physical_plan::action_schema::decode_actions_and_meta_from_batch;

    #[test]
    fn encodes_touched_paths_as_remove_actions() -> Result<()> {
        futures::executor::block_on(async {
            let input_schema = Arc::new(Schema::new(vec![Field::new(
                "file_path",
                DataType::Utf8,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![Arc::new(StringArray::from(vec!["a.parquet", "b.parquet"]))],
            )?;
            let input: Arc<dyn ExecutionPlan> =
                MemorySourceConfig::try_new_from_batches(input_schema, vec![batch])?;
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(IcebergRemoveDataFilesExec::try_new(input, "file_path")?);

            let batches =
                datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx()).await?;
            let decoded = decode_actions_and_meta_from_batch(&batches[0])?;
            assert_eq!(
                decoded.removed_data_file_paths,
                vec!["a.parquet", "b.parquet"]
            );
            Ok(())
        })
    }
}
