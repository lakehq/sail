use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, Int64Builder, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::projection::ProjectionMapping;
use datafusion_physical_expr::{Distribution, PhysicalExpr};
use futures::StreamExt;
use sail_common_datafusion::datasource::{RowLevelOperationType, OPERATION_COLUMN};

use crate::datasource::{
    is_metadata_struct_field, METADATA_ROW_COMMIT_VERSION_FIELD, METADATA_ROW_ID_FIELD,
};

/// Replaces Delta's synthetic row-tracking `_metadata` struct with the hidden
/// materialized Parquet columns used to preserve stable row IDs across rewrites.
#[derive(Debug)]
pub struct RowTrackingMaterializeExec {
    input: Arc<dyn ExecutionPlan>,
    metadata_column_name: String,
    row_id_column_name: String,
    row_commit_version_column_name: String,
    cache: Arc<PlanProperties>,
}

impl RowTrackingMaterializeExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        metadata_column_name: impl Into<String>,
        row_id_column_name: impl Into<String>,
        row_commit_version_column_name: impl Into<String>,
    ) -> Result<Self> {
        let metadata_column_name = metadata_column_name.into();
        let row_id_column_name = row_id_column_name.into();
        let row_commit_version_column_name = row_commit_version_column_name.into();
        let schema = materialized_schema(
            input.schema(),
            &metadata_column_name,
            &row_id_column_name,
            &row_commit_version_column_name,
        );
        let cache = Arc::new(compute_properties(
            &input,
            schema,
            &metadata_column_name,
            &row_id_column_name,
            &row_commit_version_column_name,
        )?);
        Ok(Self {
            input,
            metadata_column_name,
            row_id_column_name,
            row_commit_version_column_name,
            cache,
        })
    }

    pub fn input(&self) -> Arc<dyn ExecutionPlan> {
        Arc::clone(&self.input)
    }

    pub fn metadata_column_name(&self) -> &str {
        &self.metadata_column_name
    }

    pub fn row_id_column_name(&self) -> &str {
        &self.row_id_column_name
    }

    pub fn row_commit_version_column_name(&self) -> &str {
        &self.row_commit_version_column_name
    }
}

impl DisplayAs for RowTrackingMaterializeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RowTrackingMaterializeExec: metadata={}, row_id={}, row_commit_version={}",
                    self.metadata_column_name,
                    self.row_id_column_name,
                    self.row_commit_version_column_name
                )
            }
            DisplayFormatType::TreeRender => write!(f, "RowTrackingMaterializeExec"),
        }
    }
}

#[async_trait]
impl ExecutionPlan for RowTrackingMaterializeExec {
    fn name(&self) -> &'static str {
        "RowTrackingMaterializeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
        if children.len() != 1 {
            return internal_err!("RowTrackingMaterializeExec requires exactly one child");
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.metadata_column_name.clone(),
            self.row_id_column_name.clone(),
            self.row_commit_version_column_name.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let output_schema = self.schema();
        let metadata_column_name = self.metadata_column_name.clone();
        let row_id_column_name = self.row_id_column_name.clone();
        let row_commit_version_column_name = self.row_commit_version_column_name.clone();

        let stream = stream.map(move |batch| {
            materialize_batch(
                batch?,
                Arc::clone(&output_schema),
                &metadata_column_name,
                &row_id_column_name,
                &row_commit_version_column_name,
            )
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        self.input
            .with_preserve_order(preserve_order)
            .and_then(|input| {
                Self::try_new(
                    input,
                    self.metadata_column_name.clone(),
                    self.row_id_column_name.clone(),
                    self.row_commit_version_column_name.clone(),
                )
                .map(|exec| Arc::new(exec) as Arc<dyn ExecutionPlan>)
                .ok()
            })
    }
}

fn is_target_metadata_field(field: &Field, metadata_column_name: &str) -> bool {
    is_metadata_struct_field(field)
        || (field.name() == metadata_column_name
            && matches!(field.data_type(), DataType::Struct(_)))
}

fn materialized_schema(
    input_schema: SchemaRef,
    metadata_column_name: &str,
    row_id_column_name: &str,
    row_commit_version_column_name: &str,
) -> SchemaRef {
    let mut fields = input_schema
        .fields()
        .iter()
        .filter(|field| {
            !is_target_metadata_field(field, metadata_column_name)
                && field.name() != row_id_column_name
                && field.name() != row_commit_version_column_name
        })
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(
        row_id_column_name.to_string(),
        DataType::Int64,
        true,
    ));
    fields.push(Field::new(
        row_commit_version_column_name.to_string(),
        DataType::Int64,
        true,
    ));
    Arc::new(Schema::new(fields))
}

fn compute_properties(
    input: &Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    metadata_column_name: &str,
    row_id_column_name: &str,
    row_commit_version_column_name: &str,
) -> Result<PlanProperties> {
    let input_schema = input.schema();
    let projection_exprs = input_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|&(_index, field)| {
            !is_target_metadata_field(field, metadata_column_name)
                && field.name() != row_id_column_name
                && field.name() != row_commit_version_column_name
        })
        .map(|(index, field)| {
            (
                Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        });
    let projection_mapping = ProjectionMapping::try_new(projection_exprs, &input_schema)?;
    let input_eq_properties = input.equivalence_properties();
    let eq_properties = input_eq_properties.project(&projection_mapping, output_schema);
    let output_partitioning = input
        .output_partitioning()
        .project(&projection_mapping, input_eq_properties);

    Ok(PlanProperties::new(
        eq_properties,
        output_partitioning,
        input.pipeline_behavior(),
        input.boundedness(),
    ))
}

fn materialize_batch(
    batch: RecordBatch,
    output_schema: SchemaRef,
    metadata_column_name: &str,
    row_id_column_name: &str,
    row_commit_version_column_name: &str,
) -> Result<RecordBatch> {
    let metadata_index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| is_target_metadata_field(field, metadata_column_name));
    let metadata_array = metadata_index
        .map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Delta row tracking metadata column '{}' must be a StructArray",
                        batch.schema().field(index).name()
                    ))
                })
        })
        .transpose()?;
    let operation_array = row_level_operation_array(&batch)?;

    let mut columns = Vec::with_capacity(output_schema.fields().len());
    for field in batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            (!is_target_metadata_field(field, metadata_column_name)
                && field.name() != row_id_column_name
                && field.name() != row_commit_version_column_name)
                .then_some(index)
        })
    {
        columns.push(Arc::clone(batch.column(field)));
    }
    columns.push(extract_metadata_i64(
        metadata_array,
        METADATA_ROW_ID_FIELD,
        batch.num_rows(),
        None,
    )?);
    columns.push(extract_metadata_i64(
        metadata_array,
        METADATA_ROW_COMMIT_VERSION_FIELD,
        batch.num_rows(),
        operation_array,
    )?);

    RecordBatch::try_new(output_schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn extract_metadata_i64(
    metadata_array: Option<&StructArray>,
    field_name: &str,
    num_rows: usize,
    operation_array: Option<RowLevelOperationArray<'_>>,
) -> Result<ArrayRef> {
    let Some(metadata_array) = metadata_array else {
        return Ok(Arc::new(Int64Array::new_null(num_rows)));
    };
    let field_index = metadata_array
        .fields()
        .iter()
        .position(|field| field.name() == field_name)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Delta row tracking metadata struct is missing '{field_name}'"
            ))
        })?;
    let values = metadata_array
        .column(field_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Delta row tracking metadata field '{field_name}' must be Int64"
            ))
        })?;
    let mut builder = Int64Builder::with_capacity(num_rows);
    for row in 0..num_rows {
        if operation_array.is_some_and(|array| array.is_update(row)) {
            builder.append_null();
        } else if metadata_array.is_valid(row) && values.is_valid(row) {
            builder.append_value(values.value(row));
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[derive(Clone, Copy)]
enum RowLevelOperationArray<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl RowLevelOperationArray<'_> {
    fn is_update(self, row: usize) -> bool {
        let value = match self {
            Self::Int32(array) if array.is_valid(row) => i64::from(array.value(row)),
            Self::Int64(array) if array.is_valid(row) => array.value(row),
            _ => return false,
        };
        value == i64::from(RowLevelOperationType::MatchedUpdate.as_i32())
            || value == i64::from(RowLevelOperationType::NotMatchedBySourceUpdate.as_i32())
    }
}

fn row_level_operation_array(batch: &RecordBatch) -> Result<Option<RowLevelOperationArray<'_>>> {
    let schema = batch.schema();
    let Some((index, field)) = schema.column_with_name(OPERATION_COLUMN) else {
        return Ok(None);
    };
    let array = batch.column(index);
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        Ok(Some(RowLevelOperationArray::Int32(array)))
    } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        Ok(Some(RowLevelOperationArray::Int64(array)))
    } else {
        Err(DataFusionError::Plan(format!(
            "row-level operation column '{OPERATION_COLUMN}' must be Int32 or Int64, got {:?}",
            field.data_type()
        )))
    }
}
