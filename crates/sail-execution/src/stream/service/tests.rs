use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use futures::TryStreamExt;
use prost::Message;

use crate::id::{JobId, TaskStreamKey, TaskStreamKeySet};
use crate::shuffle::ShuffleCompression;
use crate::stream::r#gen::TaskStreamTicket;
use crate::stream::reader::TaskStreamSource;
use crate::stream::service::TaskStreamKeyDecoder;
use crate::stream::service::client::decode_task_stream;
use crate::stream::service::server::encode_task_stream;

type TestError = Box<dyn std::error::Error>;

#[test]
fn ticket_has_the_channels_of_several_tasks() -> Result<(), TestError> {
    let ticket = TaskStreamTicket {
        job_id: 7,
        stage: 3,
        partition: vec![0, 5, 2],
        attempt: vec![1, 0, 0],
        channel: vec![4, 0],
    };
    let keys = TaskStreamKeySet::decode(&ticket.encode_to_vec())?;
    let expected = TaskStreamKeySet {
        job_id: JobId::from(7),
        stage: 3,
        tasks: vec![(0, 1), (5, 0), (2, 0)],
        channels: vec![4, 0],
    };
    assert_eq!(keys, expected);
    let key = |partition, attempt, channel| TaskStreamKey {
        job_id: JobId::from(7),
        stage: 3,
        partition,
        attempt,
        channel,
    };
    assert_eq!(
        keys.keys().collect::<Vec<_>>(),
        vec![
            key(0, 1, 4),
            key(0, 1, 0),
            key(5, 0, 4),
            key(5, 0, 0),
            key(2, 0, 4),
            key(2, 0, 0),
        ]
    );
    // The ticket for a single stream has one task and one channel.
    let ticket = TaskStreamTicket {
        job_id: 7,
        stage: 3,
        partition: vec![0],
        attempt: vec![0],
        channel: vec![0],
    };
    assert_eq!(TaskStreamKey::try_from(ticket.clone())?, key(0, 0, 0));
    let keys = TaskStreamKeySet::decode(&ticket.encode_to_vec())?;
    assert_eq!(keys, key(0, 0, 0).into());
    Ok(())
}

#[test]
fn ticket_without_tasks_or_channels_is_rejected() {
    let ticket = |partition: &[u64], attempt: &[u64], channel: &[u64]| TaskStreamTicket {
        job_id: 7,
        stage: 3,
        partition: partition.to_vec(),
        attempt: attempt.to_vec(),
        channel: channel.to_vec(),
    };
    for invalid in [
        ticket(&[], &[], &[]),
        ticket(&[], &[], &[0]),
        ticket(&[0], &[0], &[]),
        ticket(&[0, 1], &[0], &[0]),
        ticket(&[0], &[0, 1], &[0]),
    ] {
        let result = TaskStreamKeySet::decode(&invalid.encode_to_vec());
        assert!(result.is_err_and(|e| e.code() == tonic::Code::InvalidArgument));
        let result = TaskStreamKey::try_from(invalid);
        assert!(result.is_err_and(|e| e.code() == tonic::Code::InvalidArgument));
    }
    // The driver serves a single stream per request.
    for several in [ticket(&[0, 1], &[0, 0], &[0]), ticket(&[0], &[0], &[0, 1])] {
        assert!(TaskStreamKeySet::decode(&several.encode_to_vec()).is_ok());
        let result = TaskStreamKey::try_from(several);
        assert!(result.is_err_and(|e| e.code() == tonic::Code::InvalidArgument));
    }
}

fn dictionary_schema() -> SchemaRef {
    let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    Arc::new(Schema::new(vec![
        Field::new("k", dictionary, true),
        Field::new("v", DataType::Int32, false),
    ]))
}

/// Creates a batch whose dictionary has only the given values, in the given order.
fn dictionary_batch(values: &[&str], start: i32) -> Result<RecordBatch, TestError> {
    let keys = Int32Array::from_iter_values((0..values.len() as i32).rev());
    let keys = DictionaryArray::<Int32Type>::try_new(
        keys,
        Arc::new(StringArray::from_iter_values(values.iter().copied())),
    )?;
    let numbers = Int32Array::from_iter_values(start..start + values.len() as i32);
    Ok(RecordBatch::try_new(
        dictionary_schema(),
        vec![Arc::new(keys), Arc::new(numbers)],
    )?)
}

fn rows(batch: &RecordBatch) -> Result<Vec<(String, i32)>, TestError> {
    let keys: ArrayRef = cast(batch.column(0), &DataType::Utf8)?;
    let keys = keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("invalid keys")?;
    let numbers = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("invalid numbers")?;
    Ok((0..batch.num_rows())
        .map(|i| (keys.value(i).to_string(), numbers.value(i)))
        .collect())
}

#[tokio::test]
async fn batches_of_several_tasks_are_encoded_in_one_stream() -> Result<(), TestError> {
    // Each task has its own dictionaries, which also differ from one batch to the next.
    let tasks = [
        vec![
            dictionary_batch(&["a", "b"], 0)?,
            dictionary_batch(&["c"], 10)?,
        ],
        vec![
            dictionary_batch(&["x", "y", "z"], 20)?,
            dictionary_batch(&["b", "a"], 30)?,
        ],
        vec![dictionary_batch(&["a"], 40)?],
    ];
    let mut expected = vec![];
    for batch in tasks.iter().flatten() {
        expected.extend(rows(batch)?);
    }
    expected.sort();
    for compression in [
        ShuffleCompression::None,
        ShuffleCompression::Lz4,
        ShuffleCompression::Zstd,
    ] {
        let sources = tasks.iter().map(|batches| {
            let batches = batches.clone().into_iter().map(Ok);
            Box::pin(futures::stream::iter(batches)) as TaskStreamSource
        });
        let source = Box::pin(futures::stream::select_all(sources)) as TaskStreamSource;
        let data = encode_task_stream(source, compression).map_err(|e| e.to_string())?;
        let batches: Vec<RecordBatch> = decode_task_stream(data, dictionary_schema())
            .try_collect()
            .await?;
        assert_eq!(batches.len(), 5);
        let mut actual = vec![];
        for batch in &batches {
            assert_eq!(batch.schema(), dictionary_schema());
            actual.extend(rows(batch)?);
        }
        actual.sort();
        assert_eq!(actual, expected);
    }
    Ok(())
}
