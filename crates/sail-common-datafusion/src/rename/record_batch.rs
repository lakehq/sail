use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

use crate::array::record_batch;
use crate::rename::schema;

pub fn rename_record_batch_stream(
    stream: SendableRecordBatchStream,
    names: &[String],
) -> datafusion_common::Result<SendableRecordBatchStream> {
    let schema = schema::rename_schema(&stream.schema(), names)?;
    let stream = {
        let schema = schema.clone();
        stream.map(move |x| match x {
            Ok(batch) => Ok(record_batch::record_batch_with_schema(batch, &schema)?),
            Err(e) => Err(e),
        })
    };
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
