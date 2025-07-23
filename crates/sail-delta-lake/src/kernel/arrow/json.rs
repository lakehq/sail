//! Utitlies for reading JSON files and handling JSON data.

use std::io::{BufRead, BufReader, Cursor};
use std::task::Poll;

use datafusion::arrow::array::{new_null_array, Array, RecordBatch, StringArray};
use datafusion::arrow::json::{reader::Decoder, ReaderBuilder};
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::compute::concat_batches;
use bytes::{Buf, Bytes};
use futures::{ready, Stream, StreamExt};
use object_store::Result as ObjectStoreResult;

use deltalake::{DeltaResult, DeltaTableConfig, DeltaTableError};

#[inline]
pub(crate) fn get_reader(data: &[u8]) -> BufReader<Cursor<&[u8]>> {
    BufReader::new(Cursor::new(data))
}

#[inline]
pub(crate) fn get_decoder(
    schema: ArrowSchemaRef,
    config: &DeltaTableConfig,
) -> DeltaResult<Decoder> {
    Ok(ReaderBuilder::new(schema)
        .with_batch_size(config.log_batch_size)
        .build_decoder()?)
}

fn insert_nulls(
    batches: &mut Vec<RecordBatch>,
    null_count: usize,
    schema: ArrowSchemaRef,
) -> Result<(), ArrowError> {
    let columns = schema
        .fields
        .iter()
        .map(|field| new_null_array(field.data_type(), null_count))
        .collect();
    batches.push(RecordBatch::try_new(schema, columns)?);
    Ok(())
}

/// Parse an array of JSON strings into a record batch.
///
/// Null values in the input array are preseverd in the output record batch.
pub(crate) fn parse_json(
    json_strings: &StringArray,
    output_schema: ArrowSchemaRef,
    config: &DeltaTableConfig,
) -> DeltaResult<RecordBatch> {
    let mut decoder = ReaderBuilder::new(output_schema.clone())
        .with_batch_size(config.log_batch_size)
        .build_decoder()?;
    let mut batches = Vec::new();

    let mut null_count = 0;
    let mut value_count = 0;
    let mut value_start = 0;

    for it in 0..json_strings.len() {
        if json_strings.is_null(it) {
            if value_count > 0 {
                let slice_data = get_nonnull_slice_data(json_strings, value_start, value_count);
                let batch =
                    decode_reader(&mut decoder, get_reader(&slice_data))
                        .collect::<Result<Vec<_>, _>>()?;
                batches.extend(batch);
                value_count = 0;
            }
            null_count += 1;
            continue;
        }
        if value_count == 0 {
            value_start = it;
        }
        if null_count > 0 {
            insert_nulls(&mut batches, null_count, output_schema.clone())?;
            null_count = 0;
        }
        value_count += 1;
    }

    if null_count > 0 {
        insert_nulls(&mut batches, null_count, output_schema.clone())?;
    }

    if value_count > 0 {
        let slice_data = get_nonnull_slice_data(json_strings, value_start, value_count);
        let batch =
            decode_reader(&mut decoder, get_reader(&slice_data)).collect::<Result<Vec<_>, _>>()?;
        batches.extend(batch);
    }

    Ok(concat_batches(&output_schema, &batches)?)
}

/// Get the data of a slice of non-null JSON strings.
fn get_nonnull_slice_data(
    json_strings: &StringArray,
    value_start: usize,
    value_count: usize,
) -> Vec<u8> {
    let slice = json_strings.slice(value_start, value_count);
    slice.iter().fold(Vec::new(), |mut acc, s| {
        acc.extend_from_slice(s.unwrap().as_bytes());
        acc
    })
}

/// Decode a stream of bytes into a stream of record batches.
pub(crate) fn decode_stream<S: Stream<Item = ObjectStoreResult<Bytes>> + Unpin>(
    mut decoder: Decoder,
    mut input: S,
) -> impl Stream<Item = Result<RecordBatch, DeltaTableError>> {
    let mut buffered = Bytes::new();
    futures::stream::poll_fn(move |cx| {
        loop {
            if buffered.is_empty() {
                buffered = match ready!(input.poll_next_unpin(cx)) {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                    None => break,
                };
            }
            let decoded = match decoder.decode(buffered.as_ref()) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            };
            let read = buffered.len();
            buffered.advance(decoded);
            if decoded != read {
                break;
            }
        }

        Poll::Ready(decoder.flush().map_err(DeltaTableError::from).transpose())
    })
}

/// Decode data provided by a reader into an iterator of record batches.
pub(crate) fn decode_reader<'a, R: BufRead + 'a>(
    decoder: &'a mut Decoder,
    mut reader: R,
) -> impl Iterator<Item = Result<RecordBatch, DeltaTableError>> + 'a {
    let mut next = move || {
        loop {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break; // Input exhausted
            }
            let read = buf.len();
            let decoded = decoder.decode(buf)?;

            reader.consume(decoded);
            if decoded != read {
                break; // Read batch size
            }
        }
        decoder.flush()
    };
    std::iter::from_fn(move || next().map_err(DeltaTableError::from).transpose())
}
