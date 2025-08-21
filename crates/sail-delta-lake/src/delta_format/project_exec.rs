use std::any::Any;
use std::collections::HashSet;
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

/// `DeltaProjectExec` is responsible for reordering columns so that partition columns
/// are placed at the end of the `RecordBatch`.
#[derive(Debug, Clone)]
pub struct DeltaProjectExec {
    input: Arc<dyn ExecutionPlan>,
    /// User-specified partition column names.
    partition_columns: Vec<String>,
    /// The output schema with partition columns moved to the end.
    output_schema: ArrowSchemaRef,
    /// The projection indices to reorder columns.
    projection_indices: Vec<usize>,
    cache: PlanProperties,
}

impl DeltaProjectExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, partition_columns: Vec<String>) -> DFResult<Self> {
        let input_schema = input.schema();
        let (output_schema, projection_indices) =
            Self::create_output_schema_and_projection(&input_schema, &partition_columns)?;

        let cache = Self::compute_properties(input.properties(), output_schema.clone());

        Ok(Self {
            input,
            partition_columns,
            output_schema,
            cache,
            projection_indices,
        })
    }

    /// Creates an output schema with partition columns moved to the end and returns the
    /// projection indices needed to perform this reordering.
    fn create_output_schema_and_projection(
        input_schema: &ArrowSchema,
        partition_columns: &[String],
    ) -> DFResult<(ArrowSchemaRef, Vec<usize>)> {
        let mut non_partition_fields: Vec<Arc<Field>> = Vec::new();
        let mut partition_fields: Vec<Arc<Field>> = Vec::new();
        let mut non_partition_indices = Vec::new();
        let mut partition_indices_map = std::collections::HashMap::new();

        let partition_set: HashSet<&String> = partition_columns.iter().collect();

        for (i, field) in input_schema.fields().iter().enumerate() {
            if partition_set.contains(field.name()) {
                partition_indices_map.insert(field.name().clone(), i);
            } else {
                non_partition_fields.push(field.clone());
                non_partition_indices.push(i);
            }
        }

        // Ensure partition columns are added in the specified order.
        for col_name in partition_columns {
            let idx = *partition_indices_map.get(col_name).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Partition column '{}' not found in schema",
                    col_name
                ))
            })?;
            partition_fields.push(Arc::new(input_schema.field(idx).clone()));
        }

        let mut fields = non_partition_fields;
        fields.extend(partition_fields);

        let mut projection_indices = non_partition_indices;
        for col_name in partition_columns {
            #[allow(clippy::unwrap_used)]
            projection_indices.push(*partition_indices_map.get(col_name).unwrap());
        }

        Ok((Arc::new(ArrowSchema::new(fields)), projection_indices))
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

    /// Process a single RecordBatch, reordering columns so partition columns are at the end
    fn project_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        if self.partition_columns.is_empty() || self.projection_indices.is_empty() {
            return Ok(batch);
        }

        batch
            .project(&self.projection_indices)
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
