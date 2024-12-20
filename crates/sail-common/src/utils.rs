use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{Expr, LogicalPlan, Projection};
use futures::StreamExt;

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

pub fn read_record_batches(data: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn write_record_batches(batches: &[RecordBatch], schema: &Schema) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(output)
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

pub fn record_batch_with_schema(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        batch.columns().to_vec(),
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )?)
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
        .map(|(column, name)| {
            let relation = column.relation.clone();
            Expr::Column(column).alias_qualified(relation, name)
        })
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

pub fn rename_record_batch_stream(
    stream: SendableRecordBatchStream,
    names: &[String],
) -> Result<SendableRecordBatchStream> {
    let schema = rename_schema(&stream.schema(), names)?;
    let stream = {
        let schema = schema.clone();
        stream.map(move |x| match x {
            Ok(batch) => Ok(record_batch_with_schema(batch, &schema)?),
            Err(e) => Err(e),
        })
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
