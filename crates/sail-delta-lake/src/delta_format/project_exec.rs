use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::StreamExt;

/// Prefix for partition columns to avoid conflicts with user data columns
pub(crate) const PARTITION_COLUMN_PREFIX: &str = "__partition_";

/// DeltaProjectExec creates copies of partition columns with special prefixes.
#[derive(Debug, Clone)]
pub struct DeltaProjectExec {
    input: Arc<dyn ExecutionPlan>,
    /// User-specified partition column names (original column names)
    partition_columns: Vec<String>,
    /// Output schema containing original columns and prefixed partition columns
    output_schema: ArrowSchemaRef,
    cache: PlanProperties,
}

impl DeltaProjectExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, partition_columns: Vec<String>) -> DFResult<Self> {
        let output_schema = Self::create_output_schema(&input.schema(), &partition_columns)?;
        let cache = Self::compute_properties(input.properties(), output_schema.clone());

        Ok(Self {
            input,
            partition_columns,
            output_schema,
            cache,
        })
    }

    /// Create output schema based on input schema and partition column names
    fn create_output_schema(
        input_schema: &ArrowSchema,
        partition_columns: &[String],
    ) -> DFResult<ArrowSchemaRef> {
        if partition_columns.is_empty() {
            return Ok(Arc::new(input_schema.clone()));
        }

        let mut fields: Vec<Arc<Field>> = input_schema.fields().to_vec();

        // Add a prefixed copy field for each partition column
        for col_name in partition_columns {
            let field = input_schema.field_with_name(col_name)?;
            let partition_field = Field::new(
                format!("{}{}", PARTITION_COLUMN_PREFIX, col_name),
                field.data_type().clone(),
                field.is_nullable(),
            );
            fields.push(Arc::new(partition_field));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    fn compute_properties(
        input_properties: &PlanProperties,
        schema: ArrowSchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            input_properties.output_partitioning().clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        )
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    /// Process a single RecordBatch, adding partition column copies
    fn project_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        if self.partition_columns.is_empty() {
            return Ok(batch);
        }

        let mut projected_columns = batch.columns().to_vec();

        for col_name in &self.partition_columns {
            let column_index = batch.schema().index_of(col_name)?;
            projected_columns.push(batch.column(column_index).clone());
        }

        RecordBatch::try_new(self.output_schema.clone(), projected_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl ExecutionPlan for DeltaProjectExec {
    fn name(&self) -> &'static str {
        "DeltaProjectExec"
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DeltaProjectExec should have exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(DeltaProjectExec::new(
            children[0].clone(),
            self.partition_columns.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let self_clone = self.clone();

        let stream = input_stream.map(move |batch_result| {
            let batch = batch_result?;
            self_clone.project_batch(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}

impl DisplayAs for DeltaProjectExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaProjectExec: partition_columns={:?}",
                    self.partition_columns
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaProjectExec")
            }
        }
    }
}
