use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    new_null_array, Array, ArrayData, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BinaryArray,
    BinaryViewArray, BooleanArray, BooleanBuilder, LargeBinaryArray, LargeStringArray, NullArray,
    PrimitiveBuilder, RecordBatch, StringArray, StringViewArray,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, SchemaRef,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::{exec_datafusion_err, exec_err, internal_err, Result};
use futures::{Stream, TryStreamExt};

use crate::streaming::event::{FlowEvent, FlowEventStream, FlowMarker, SendableFlowEventStream};
use crate::streaming::schema::{to_flow_event_schema, try_from_flow_event_schema};

/// A stream of [`RecordBatch`] for encoded flow events.
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
                    retracted,
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
                        retracted: Arc::new(retracted.slice(start, length)),
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
                retracted: Arc::new(retracted.slice(start, length)),
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

fn placeholder_array(data_type: &DataType, length: usize) -> Result<ArrayRef> {
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::new(length))),
        DataType::Boolean => Ok(placeholder_boolean_array(length)),
        DataType::Int8 => Ok(placeholder_primitive_array::<Int8Type>(length)),
        DataType::Int16 => Ok(placeholder_primitive_array::<Int16Type>(length)),
        DataType::Int32 => Ok(placeholder_primitive_array::<Int32Type>(length)),
        DataType::Int64 => Ok(placeholder_primitive_array::<Int64Type>(length)),
        DataType::UInt8 => Ok(placeholder_primitive_array::<UInt8Type>(length)),
        DataType::UInt16 => Ok(placeholder_primitive_array::<UInt16Type>(length)),
        DataType::UInt32 => Ok(placeholder_primitive_array::<UInt32Type>(length)),
        DataType::UInt64 => Ok(placeholder_primitive_array::<UInt64Type>(length)),
        DataType::Float16 => Ok(placeholder_primitive_array::<Float16Type>(length)),
        DataType::Float32 => Ok(placeholder_primitive_array::<Float32Type>(length)),
        DataType::Float64 => Ok(placeholder_primitive_array::<Float64Type>(length)),
        DataType::Timestamp(time_unit, tz) => {
            match time_unit {
                TimeUnit::Second => Ok(placeholder_timestamp_array::<TimestampSecondType>(
                    length,
                    tz.clone(),
                )),
                TimeUnit::Millisecond => Ok(
                    placeholder_timestamp_array::<TimestampMillisecondType>(length, tz.clone()),
                ),
                TimeUnit::Microsecond => Ok(
                    placeholder_timestamp_array::<TimestampMicrosecondType>(length, tz.clone()),
                ),
                TimeUnit::Nanosecond => Ok(placeholder_timestamp_array::<TimestampNanosecondType>(
                    length,
                    tz.clone(),
                )),
            }
        }
        DataType::Date32 => Ok(placeholder_primitive_array::<Date32Type>(length)),
        DataType::Date64 => Ok(placeholder_primitive_array::<Date64Type>(length)),
        DataType::Time32(time_unit) => match time_unit {
            TimeUnit::Second => Ok(placeholder_primitive_array::<Time32SecondType>(length)),
            TimeUnit::Millisecond => {
                Ok(placeholder_primitive_array::<Time32MillisecondType>(length))
            }
            TimeUnit::Microsecond | TimeUnit::Nanosecond => {
                internal_err!("invalid data type: {data_type:?}")
            }
        },
        DataType::Time64(time_unit) => match time_unit {
            TimeUnit::Second | TimeUnit::Millisecond => {
                internal_err!("invalid data type: {data_type:?}")
            }
            TimeUnit::Microsecond => {
                Ok(placeholder_primitive_array::<Time64MicrosecondType>(length))
            }
            TimeUnit::Nanosecond => Ok(placeholder_primitive_array::<Time64NanosecondType>(length)),
        },
        DataType::Duration(time_unit) => match time_unit {
            TimeUnit::Second => Ok(placeholder_primitive_array::<DurationSecondType>(length)),
            TimeUnit::Millisecond => Ok(placeholder_primitive_array::<DurationMillisecondType>(
                length,
            )),
            TimeUnit::Microsecond => Ok(placeholder_primitive_array::<DurationMicrosecondType>(
                length,
            )),
            TimeUnit::Nanosecond => Ok(placeholder_primitive_array::<DurationNanosecondType>(
                length,
            )),
        },
        DataType::Interval(interval_unit) => match interval_unit {
            IntervalUnit::YearMonth => {
                Ok(placeholder_primitive_array::<IntervalYearMonthType>(length))
            }
            IntervalUnit::DayTime => Ok(placeholder_primitive_array::<IntervalDayTimeType>(length)),
            IntervalUnit::MonthDayNano => Ok(
                placeholder_primitive_array::<IntervalMonthDayNanoType>(length),
            ),
        },
        DataType::Binary => Ok(Arc::new(BinaryArray::from(vec![&[] as &[u8]]))),
        DataType::FixedSizeBinary(_size) => {
            todo!()
        }
        DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from(vec![&[] as &[u8]]))),
        DataType::BinaryView => Ok(Arc::new(BinaryViewArray::from(vec![&[] as &[u8]]))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![""]))),
        DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from(vec![""]))),
        DataType::Utf8View => Ok(Arc::new(StringViewArray::from(vec![""]))),
        DataType::List(_) => {
            todo!()
        }
        DataType::ListView(_) => {
            todo!()
        }
        DataType::FixedSizeList(_, _) => {
            todo!()
        }
        DataType::LargeList(_) => {
            todo!()
        }
        DataType::LargeListView(_) => {
            todo!()
        }
        DataType::Struct(_) => {
            todo!()
        }
        DataType::Union(_, _) => {
            todo!()
        }
        DataType::Dictionary(_, _) => {
            todo!()
        }
        DataType::Decimal128(_, _) => Ok(placeholder_primitive_array::<Decimal128Type>(length)),
        DataType::Decimal256(_, _) => Ok(placeholder_primitive_array::<Decimal256Type>(length)),
        DataType::Map(_, _) => {
            todo!()
        }
        DataType::RunEndEncoded(_, _) => {
            todo!()
        }
    }
}

fn placeholder_boolean_array(length: usize) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(length);
    for _ in 0..length {
        builder.append_value(false);
    }
    Arc::new(builder.finish())
}

fn placeholder_primitive_array<T: ArrowPrimitiveType>(length: usize) -> ArrayRef {
    let mut builder = <PrimitiveBuilder<T>>::with_capacity(length);
    for _ in 0..length {
        builder.append_value(T::Native::ZERO);
    }
    Arc::new(builder.finish())
}

fn placeholder_timestamp_array<T: ArrowTimestampType + ArrowPrimitiveType>(
    length: usize,
    tz: Option<Arc<str>>,
) -> ArrayRef {
    let mut builder = <PrimitiveBuilder<T>>::with_capacity(length);
    for _ in 0..length {
        builder.append_value(T::Native::ZERO);
    }
    Arc::new(builder.finish().with_timezone_opt(tz))
}
