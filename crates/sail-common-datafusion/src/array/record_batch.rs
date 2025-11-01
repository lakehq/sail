use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
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

/// Helper function to handle timezone adjustment for timestamp arrays.
fn adjust_timestamp_timezone<T>(array: &ArrayRef, target_tz: Option<Arc<str>>) -> Result<ArrayRef>
where
    T: ArrowTimestampType,
{
    let timestamp_array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(format!(
                "Failed to downcast to timestamp array type: {:?}",
                array.data_type()
            ))
        })?;

    Ok(Arc::new(
        timestamp_array.clone().with_timezone_opt(target_tz),
    ))
}

/// Cast a RecordBatch to a target schema with relaxed timezone handling.
///
/// This function is similar to `cast_record_batch` but handles timestamp timezone
/// differences more gracefully by reinterpreting timezone metadata without converting
/// the underlying values. This is useful for Iceberg writes where timezone metadata
/// needs to be adjusted but the actual timestamp values should remain unchanged.
pub fn cast_record_batch_relaxed_tz(
    batch: &RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());

    for field in target.fields() {
        let idx = batch
            .schema()
            .index_of(field.name())
            .map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?;
        let src = batch.column(idx);
        let src_dt = src.data_type();
        let tgt_dt = field.data_type();

        // Fast path: exact match
        if src_dt == tgt_dt {
            cols.push(src.clone());
            continue;
        }

        // Special handling for timestamp timezone differences
        // Only adjust timezone if time units match but timezone differs
        let handled = match (src_dt, tgt_dt) {
            (DataType::Timestamp(src_unit, _), DataType::Timestamp(tgt_unit, tgt_tz))
                if src_unit == tgt_unit =>
            {
                let adjusted = match src_unit {
                    TimeUnit::Second => {
                        adjust_timestamp_timezone::<TimestampSecondType>(src, tgt_tz.clone())?
                    }
                    TimeUnit::Millisecond => {
                        adjust_timestamp_timezone::<TimestampMillisecondType>(src, tgt_tz.clone())?
                    }
                    TimeUnit::Microsecond => {
                        adjust_timestamp_timezone::<TimestampMicrosecondType>(src, tgt_tz.clone())?
                    }
                    TimeUnit::Nanosecond => {
                        adjust_timestamp_timezone::<TimestampNanosecondType>(src, tgt_tz.clone())?
                    }
                };
                cols.push(adjusted);
                true
            }
            _ => false,
        };

        // If not handled by timezone logic, use standard cast
        if !handled {
            let casted = cast(src, tgt_dt)?;
            cols.push(casted);
        }
    }

    Ok(RecordBatch::try_new(target.clone(), cols)?)
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
