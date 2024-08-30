use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, RecordBatch};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow_cast::cast;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{Expr, LogicalPlan, Projection};

use crate::spec::DEFAULT_LIST_FIELD_NAME;

pub fn cast_record_batch(batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let fields = schema.fields();
    let columns = batch.columns();
    let columns = fields
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            let data_type = field.data_type();
            let column = cast(column, data_type)?;
            Ok(column)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn read_record_batches(data: Vec<u8>) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn rename_schema(schema: &SchemaRef, names: &[String]) -> Result<SchemaRef> {
    if schema.fields().len() != names.len() {
        return plan_err!(
            "cannot rename fields for schema with {} fields using {} names",
            schema.fields().len(),
            names.len()
        );
    }
    let fields = schema
        .fields()
        .iter()
        .zip(names.iter())
        .map(|(field, name)| field.as_ref().clone().with_name(name))
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

pub fn rename_logical_plan(plan: LogicalPlan, names: &[String]) -> Result<LogicalPlan> {
    if plan.schema().fields().len() != names.len() {
        return exec_err!(
            "cannot rename fields for logical plan with {} fields using {} names",
            plan.schema().fields().len(),
            names.len()
        );
    }
    let expr = plan
        .schema()
        .columns()
        .into_iter()
        .zip(names.iter())
        .map(|(column, name)| Expr::Column(column).alias(name))
        .collect();
    // The logical plan schema requires field names to be unique.
    // To support duplicate field names, construct the physical plan directly.
    Ok(LogicalPlan::Projection(Projection::try_new(
        expr,
        Arc::new(plan),
    )?))
}

pub fn rename_physical_plan(
    plan: Arc<dyn ExecutionPlan>,
    names: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.schema().fields().len() != names.len() {
        return exec_err!(
            "cannot rename fields for physical plan with {} fields using {} names",
            plan.schema().fields().len(),
            names.len()
        );
    }
    let expr = plan
        .schema()
        .fields()
        .iter()
        .zip(names.iter())
        .enumerate()
        .map(|(i, (field, name))| {
            (
                Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                name.to_string(),
            )
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(expr, plan)?))
}

pub fn array_into_list_array(arr: ArrayRef, nullable: bool) -> ListArray {
    let offsets = OffsetBuffer::from_lengths([arr.len()]);
    ListArray::new(
        Arc::new(new_list_field(arr.data_type().to_owned(), nullable)),
        offsets,
        arr,
        None,
    )
}

pub fn new_list_field(data_type: DataType, nullable: bool) -> Field {
    Field::new(DEFAULT_LIST_FIELD_NAME, data_type, nullable)
}
