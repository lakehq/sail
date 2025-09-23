use std::io::Cursor;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion_common::Result;

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

pub fn record_batch_with_schema(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        batch.columns().to_vec(),
        &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
    )?)
}
