use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    new_null_array, Array, ArrayData, ArrayRef, BinaryArray, BooleanArray, RecordBatch,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::{exec_datafusion_err, exec_err, Result};
use futures::{Stream, TryStreamExt};

use crate::array::placeholder::{placeholder_array, placeholder_boolean_array};
use crate::streaming::event::marker::FlowMarker;
use crate::streaming::event::schema::{to_flow_event_schema, try_from_flow_event_schema};
use crate::streaming::event::stream::{FlowEventStream, SendableFlowEventStream};
use crate::streaming::event::FlowEvent;

/// A stream for encoded flow events.
/// The encoded [`RecordBatch`] has a special schema.
/// The first field contains the encoded marker if not null.
/// The other fields are valid only if the marker is null.
/// The second field is the retraction flag for each data row.
/// For a data event, the marker array only contains the null buffer,
/// which adds 1-bit overhead for each row in a data event.
/// The retracted field adds another 1-bit overhead for each row in a data event.
pub struct EncodedFlowEventStream {
    inner: SendableFlowEventStream,
    schema: SchemaRef,
}

impl EncodedFlowEventStream {
    pub fn new(stream: SendableFlowEventStream) -> Self {
        let schema = to_flow_event_schema(&stream.schema());
        Self {
            inner: stream,
            schema: Arc::new(schema),
        }
    }

    pub fn encode(&self, event: FlowEvent) -> Result<RecordBatch> {
        let columns = match event {
            FlowEvent::Data { batch, retracted } => {
                let mut columns: Vec<ArrayRef> = vec![
                    new_null_array(&DataType::Binary, batch.num_rows()),
                    Arc::new(retracted),
                ];
                columns.extend(batch.columns().iter().cloned());
                columns
            }
            FlowEvent::Marker(marker) => {
                let marker = {
                    let values = marker.encode()?;
                    let offsets = vec![0, values.len() as i32];
                    let array_data = ArrayData::builder(DataType::Binary)
                        .len(1)
                        .add_buffer(Buffer::from(offsets))
                        .add_buffer(Buffer::from(values))
                        .build()?;
                    Arc::new(BinaryArray::from(array_data))
                };
                let retracted = placeholder_boolean_array(1);
                let mut columns: Vec<ArrayRef> = vec![marker, retracted];
                for field in self.inner.schema().fields() {
                    if field.is_nullable() {
                        columns.push(new_null_array(field.data_type(), 1));
                    } else {
                        columns.push(placeholder_array(field.data_type(), 1)?);
                    }
                }
                columns
            }
        };
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl RecordBatchStream for EncodedFlowEventStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EncodedFlowEventStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll_next(cx)
            .map(|x| x.map(|x| x.and_then(|x| this.encode(x))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// An internal helper stream to decode flow events from encoded [`RecordBatch`].
/// Since a single [`RecordBatch`] may contain multiple events, a user-facing
/// stream should be created by flattening this stream.
struct DecodedMultiFlowEventStream {
    inner: SendableRecordBatchStream,
    /// The schema of the data batches for the decoded events.
    schema: SchemaRef,
}

impl DecodedMultiFlowEventStream {
    fn try_new(stream: SendableRecordBatchStream) -> Result<Self> {
        let schema = try_from_flow_event_schema(&stream.schema())?;
        Ok(Self {
            inner: stream,
            schema: Arc::new(schema),
        })
    }

    fn get_special_columns_and_data<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<(&'a BinaryArray, &'a BooleanArray, RecordBatch)> {
        let mut columns = batch.columns().iter();
        let Some(marker) = columns.next() else {
            return exec_err!("missing marker array");
        };
        let marker = marker
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("invalid marker array type"))?;
        let Some(retracted) = columns.next() else {
            return exec_err!("missing retracted array");
        };
        let retracted = retracted
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| exec_datafusion_err!("invalid retracted array type"))?;
        let data = RecordBatch::try_new(self.schema.clone(), columns.cloned().collect())?;
        Ok((marker, retracted, data))
    }

    fn decode(&self, batch: RecordBatch) -> Result<Vec<FlowEvent>> {
        // We slice the batch rows into either a single marker row,
        // or continuous non-marker rows (data rows).
        let mut events = vec![];
        let (marker, retracted, data) = self.get_special_columns_and_data(&batch)?;
        let mut start_data_index = None;
        for i in 0..batch.num_rows() {
            if marker.is_valid(i) {
                // flush the data rows before the marker
                if let Some(start) = start_data_index {
                    let length = i - start;
                    events.push(FlowEvent::Data {
                        batch: data.slice(start, length),
                        retracted: retracted.slice(start, length),
                    });
                    start_data_index = None;
                }
                let marker = FlowMarker::decode(marker.value(i))?;
                events.push(FlowEvent::Marker(marker));
            } else if start_data_index.is_none() {
                start_data_index = Some(i);
            }
        }
        // flush the remaining data rows
        if let Some(start) = start_data_index {
            let length = batch.num_rows() - start;
            events.push(FlowEvent::Data {
                batch: data.slice(start, length),
                retracted: retracted.slice(start, length),
            });
        }
        Ok(events)
    }
}

impl Stream for DecodedMultiFlowEventStream {
    type Item = Result<Vec<FlowEvent>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll_next(cx)
            .map(|x| x.map(|x| x.and_then(|x| this.decode(x))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// A record batch stream for decoded flow events.
pub struct DecodedFlowEventStream {
    inner: Pin<Box<dyn Stream<Item = Result<FlowEvent>> + Send>>,
    schema: SchemaRef,
}

impl DecodedFlowEventStream {
    pub fn try_new(stream: SendableRecordBatchStream) -> Result<Self> {
        let inner = DecodedMultiFlowEventStream::try_new(stream)?;
        let schema = inner.schema.clone();
        let inner = inner
            .map_ok(|events| futures::stream::iter(events.into_iter().map(Ok)))
            .try_flatten();
        Ok(Self {
            inner: Box::pin(inner),
            schema,
        })
    }
}

impl Stream for DecodedFlowEventStream {
    type Item = Result<FlowEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.as_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl FlowEventStream for DecodedFlowEventStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
