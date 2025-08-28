use std::iter::once;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayData, ArrayRef, BinaryArray, NullArray, RecordBatch};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result};
use futures::{Stream, TryStreamExt};

use crate::event::{FlowEvent, FlowEventStream, FlowMarker, SendableFlowEventStream};
use crate::field::{is_marker_field, marker_field};

/// A stream of [`RecordBatch`] for encoded flow events.
/// The encoded [`RecordBatch`] has a field [`MARKER_FIELD_NAME`] of type
/// [`DataType::Binary`] and contains the encoded marker if not null.
/// The other fields contain the data batch if the marker is null.
/// The marker field is always the first field in the schema.
/// For a data event, the marker array only contains the null buffer, so each row only has
/// 1-bit overhead.
pub struct EncodedFlowEventStream {
    inner: SendableFlowEventStream,
    schema: SchemaRef,
}

impl EncodedFlowEventStream {
    pub fn new(stream: SendableFlowEventStream) -> Self {
        let fields = once(marker_field())
            .chain(stream.schema().fields().iter().map(|x| x.as_ref().clone()))
            .collect::<Vec<_>>();
        Self {
            inner: stream,
            schema: Arc::new(Schema::new(fields)),
        }
    }

    pub fn encode(&self, event: FlowEvent) -> Result<RecordBatch> {
        let columns = match event {
            FlowEvent::Data(batch) => {
                once(cast(&NullArray::new(batch.num_rows()), &DataType::Binary)?)
                    .chain(batch.columns().iter().cloned())
                    .collect()
            }
            FlowEvent::Marker(marker) => {
                let values = marker.encode()?;
                let offsets = vec![0, values.len() as i32];
                let array_data = ArrayData::builder(DataType::Binary)
                    .len(1)
                    .add_buffer(Buffer::from(offsets))
                    .add_buffer(Buffer::from(values))
                    .build()?;
                let mut columns: Vec<ArrayRef> = vec![Arc::new(BinaryArray::from(array_data))];
                for field in self.inner.schema().fields() {
                    columns.push(cast(&NullArray::new(1), field.data_type())?);
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
        let schema = stream.schema();
        let mut iter = schema.fields().iter();
        if !iter.next().is_some_and(|x| is_marker_field(x.as_ref())) {
            return plan_err!("missing marker field for event schema");
        };
        let fields = iter.cloned().collect::<Vec<_>>();
        Ok(Self {
            inner: stream,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    fn get_marker_and_data<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<(&'a BinaryArray, RecordBatch)> {
        let mut iter = batch.columns().iter();
        let Some(marker) = iter.next() else {
            return exec_err!("invalid event batch column count");
        };
        let marker = marker
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("invalid marker array type"))?;
        let data = RecordBatch::try_new(self.schema.clone(), iter.cloned().collect())?;
        Ok((marker, data))
    }

    fn decode(&self, batch: RecordBatch) -> Result<Vec<FlowEvent>> {
        // We slice the batch rows into either a single marker row,
        // or continuous non-null data rows.
        let mut events = vec![];
        let (marker, data) = self.get_marker_and_data(&batch)?;
        let mut start_data_index = None;
        for i in 0..batch.num_rows() {
            if marker.is_valid(i) {
                // flush the data rows before the marker
                if let Some(start) = start_data_index {
                    events.push(FlowEvent::Data(data.slice(start, i - start)));
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
            events.push(FlowEvent::Data(data.slice(start, batch.num_rows() - start)));
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
