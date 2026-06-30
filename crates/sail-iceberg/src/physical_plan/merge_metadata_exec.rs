use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::TryStreamExt;

use crate::row_level_metadata::RowLevelMetadataColumns;

#[derive(Debug, Clone)]
pub struct IcebergMergeMetadataExec {
    input: Arc<dyn ExecutionPlan>,
    data_file_path: String,
    file_column_name: Option<String>,
    row_index_column_name: Option<String>,
    output_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl IcebergMergeMetadataExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        data_file_path: String,
        file_column_name: Option<String>,
        row_index_column_name: Option<String>,
    ) -> Result<Self> {
        let output_schema = Arc::new(
            RowLevelMetadataColumns::new(
                file_column_name.as_deref(),
                row_index_column_name.as_deref(),
            )
            .append_to_schema(input.schema().as_ref())?,
        );
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            data_file_path,
            file_column_name,
            row_index_column_name,
            output_schema,
            cache,
        })
    }
}

#[async_trait]
impl ExecutionPlan for IcebergMergeMetadataExec {
    fn name(&self) -> &str {
        "IcebergMergeMetadataExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergMergeMetadataExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.data_file_path.clone(),
            self.file_column_name.clone(),
            self.row_index_column_name.clone(),
        )?))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergMergeMetadataExec only supports partition 0, got {partition}"
            )));
        }

        let child = self.input.execute(0, context)?;
        let output_schema = self.output_schema.clone();
        let schema_for_adapter = output_schema.clone();
        let data_file_path = self.data_file_path.clone();
        let include_file = self.file_column_name.is_some();
        let include_row_index = self.row_index_column_name.is_some();

        let stream = try_stream! {
            let mut row_offset = 0i64;
            let mut stream = child;
            while let Some(batch) = stream.try_next().await? {
                let rows = batch.num_rows();
                let mut columns = batch.columns().to_vec();
                if include_file {
                    let values = (0..rows)
                        .map(|_| Some(data_file_path.as_str()))
                        .collect::<Vec<_>>();
                    columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
                }
                if include_row_index {
                    let values = (0..rows)
                        .map(|idx| row_offset + idx as i64)
                        .collect::<Vec<_>>();
                    columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
                    row_offset += rows as i64;
                }

                yield RecordBatch::try_new(output_schema.clone(), columns)?;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            Box::pin(stream),
        )))
    }
}

impl DisplayAs for IcebergMergeMetadataExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(
                f,
                "IcebergMergeMetadataExec: data_file={}",
                self.data_file_path
            ),
        }
    }
}
