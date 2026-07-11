use std::collections::HashSet;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, Int32Array, Int64Array};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::TryStreamExt;
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, MERGE_SOURCE_METRIC_COLUMN, OPERATION_COLUMN,
    RowLevelOperationType,
};

use crate::row_level_metadata::{MERGE_PARTITION_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};

#[derive(Debug, Clone)]
pub struct IcebergMergeDataRowsExec {
    input: Arc<dyn ExecutionPlan>,
    operation_index: usize,
    keep_indices: Vec<usize>,
    output_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl IcebergMergeDataRowsExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let operation_index = input_schema.index_of(OPERATION_COLUMN)?;
        let internal_columns: HashSet<&str> = [
            MERGE_FILE_COLUMN,
            MERGE_ROW_INDEX_COLUMN,
            MERGE_SOURCE_METRIC_COLUMN,
            OPERATION_COLUMN,
            MERGE_PARTITION_SPEC_ID_COLUMN,
            MERGE_PARTITION_COLUMN,
        ]
        .into_iter()
        .collect();
        let keep_indices = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                (!internal_columns.contains(field.name().as_str())).then_some(idx)
            })
            .collect::<Vec<_>>();
        let fields = keep_indices
            .iter()
            .map(|idx| input_schema.field(*idx).clone())
            .collect::<Vec<_>>();
        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            operation_index,
            keep_indices,
            output_schema,
            cache,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

#[async_trait]
impl ExecutionPlan for IcebergMergeDataRowsExec {
    fn name(&self) -> &str {
        "IcebergMergeDataRowsExec"
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
                "IcebergMergeDataRowsExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::try_new(Arc::clone(&children[0]))?))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child = self.input.execute(partition, context)?;
        let operation_index = self.operation_index;
        let keep_indices = self.keep_indices.clone();
        let output_schema = self.output_schema.clone();
        let schema_for_adapter = output_schema.clone();

        let stream = try_stream! {
            let mut stream = child;
            while let Some(batch) = stream.try_next().await? {
                let mask = merge_data_row_mask(&batch, operation_index)?;
                let filtered = filter_record_batch(&batch, &mask)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                if filtered.num_rows() == 0 {
                    continue;
                }
                let columns = keep_indices
                    .iter()
                    .map(|idx| filtered.column(*idx).clone())
                    .collect::<Vec<_>>();
                yield RecordBatch::try_new(output_schema.clone(), columns)?;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            Box::pin(stream),
        )))
    }
}

impl DisplayAs for IcebergMergeDataRowsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(f, "IcebergMergeDataRowsExec"),
        }
    }
}

fn merge_data_row_mask(batch: &RecordBatch, operation_index: usize) -> Result<BooleanArray> {
    let column = batch.column(operation_index);
    let keep = |value: i32| {
        value == RowLevelOperationType::Insert.as_i32()
            || value == RowLevelOperationType::MatchedUpdate.as_i32()
            || value == RowLevelOperationType::NotMatchedBySourceUpdate.as_i32()
    };

    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        let mask = (0..values.len())
            .map(|idx| (!values.is_null(idx)).then(|| keep(values.value(idx))))
            .collect::<BooleanArray>();
        return Ok(mask);
    }

    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        let mask = (0..values.len())
            .map(|idx| (!values.is_null(idx)).then(|| keep(values.value(idx) as i32)))
            .collect::<BooleanArray>();
        return Ok(mask);
    }

    Err(DataFusionError::Internal(format!(
        "{OPERATION_COLUMN} must be Int32 or Int64"
    )))
}
