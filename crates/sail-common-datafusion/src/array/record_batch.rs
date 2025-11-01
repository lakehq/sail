use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, RecordBatch, RecordBatchOptions, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
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
        let handled = match (src_dt, tgt_dt) {
            // Remove timezone
            (
                DataType::Timestamp(TimeUnit::Second, Some(_)),
                DataType::Timestamp(TimeUnit::Second, None),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampSecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(None::<Arc<str>>)));
                true
            }
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some(_)),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampMillisecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(None::<Arc<str>>)));
                true
            }
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampMicrosecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(None::<Arc<str>>)));
                true
            }
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(_)),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampNanosecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(None::<Arc<str>>)));
                true
            }
            // Add timezone
            (
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Second, Some(tz)),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampSecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(Some(tz.clone()))));
                true
            }
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampMillisecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(Some(tz.clone()))));
                true
            }
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some(tz)),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampMicrosecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(Some(tz.clone()))));
                true
            }
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)),
            ) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(
                            "downcast TimestampNanosecondArray failed".to_string(),
                        )
                    })?;
                cols.push(Arc::new(arr.clone().with_timezone_opt(Some(tz.clone()))));
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
