use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use futures::{StreamExt, TryStreamExt};
use riffle_client::{Endpoint, ReadTaskInfo, ShuffleReader, ShuffleWriter, WriteTaskInfo};

use crate::id::TaskKey;
use crate::stream::error::TaskStreamError;
use crate::stream::reader::{TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState, TaskStreamWriter};
use crate::task::definition::{TaskInput, TaskInputLocator};

pub(crate) fn application_id(session_id: &str) -> String {
    format!("sail-session-{session_id}")
}

#[derive(Clone, Debug)]
pub(crate) struct RiffleStreams {
    pub app_id: String,
    pub manager: Endpoint,
}

impl RiffleStreams {
    pub fn writer(
        &self,
        key: TaskKey,
        mappers: usize,
        channels: usize,
    ) -> Arc<dyn TaskStreamWriter> {
        Arc::new(RiffleWriter {
            streams: self.clone(),
            key,
            mappers,
            channels,
        })
    }

    pub fn reader(
        &self,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Arc<dyn TaskStreamReader> {
        Arc::new(RiffleReader {
            streams: self.clone(),
            key,
            input,
            schema,
        })
    }
}

#[derive(Debug)]
struct RiffleWriter {
    streams: RiffleStreams,
    key: TaskKey,
    mappers: usize,
    channels: usize,
}

#[tonic::async_trait]
impl TaskStreamWriter for RiffleWriter {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        let writer = ShuffleWriter::new(
            self.streams.app_id.clone(),
            self.streams.manager.clone(),
            WriteTaskInfo {
                job_id: self.key.job_id.into(),
                stage_id: self.key.stage as u64,
                map_id: partition,
                attempt_id: self.key.attempt,
                num_mappers: self.mappers,
                num_partitions: self.channels,
            },
        )
        .await
        .map_err(|error| DataFusionError::External(error.into()))?;
        Ok(Box::new(RiffleSink {
            writer,
            channels: self.channels,
        }))
    }
}

struct RiffleSink {
    writer: ShuffleWriter,
    channels: usize,
}

#[tonic::async_trait]
impl TaskStreamSink for RiffleSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        if batches.len() != self.channels {
            return Err(DataFusionError::Internal(
                "Riffle output channel count changed".into(),
            ));
        }
        for (partition, batch) in batches.into_iter().enumerate() {
            if let Some(batch) = batch {
                self.writer
                    .write(partition, encode_batch(&batch)?)
                    .await
                    .map_err(|error| DataFusionError::External(error.into()))?;
            }
        }
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.writer
            .finish()
            .await
            .map_err(|error| DataFusionError::External(error.into()))
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.writer
            .abort()
            .await
            .map_err(|error| DataFusionError::External(error.into()))
    }
}

#[derive(Debug)]
struct RiffleReader {
    streams: RiffleStreams,
    key: TaskKey,
    input: TaskInput,
    schema: SchemaRef,
}

#[tonic::async_trait]
impl TaskStreamReader for RiffleReader {
    async fn open(&self, partition: usize) -> Result<TaskStreamSource> {
        let TaskInputLocator::ShuffleService { channels } = self.input.locator.as_ref() else {
            return Err(DataFusionError::Internal(
                "Riffle requires a shuffle service input".into(),
            ));
        };
        let partitions = channels
            .get(partition)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Riffle input partition {partition} not found"))
            })?
            .clone();
        let reader = ShuffleReader::new(
            self.streams.app_id.clone(),
            self.streams.manager.clone(),
            ReadTaskInfo {
                job_id: self.key.job_id.into(),
                upstream_stage_id: self.input.stage as u64,
                partitions,
            },
        )
        .await
        .map_err(|error| DataFusionError::External(error.into()))?;
        let schema = self.schema.clone();
        Ok(Box::pin(
            reader
                .stream()
                .map(move |record| {
                    let record =
                        record.map_err(|error| TaskStreamError::Unknown(format!("{error:#}")))?;
                    decode_batches(record.data, &schema)
                        .map_err(|error| TaskStreamError::Unknown(error.to_string()))
                })
                .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                .try_flatten(),
        ))
    }
}

fn encode_batch(batch: &RecordBatch) -> Result<Bytes> {
    let mut writer = StreamWriter::try_new(Vec::new(), batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(Bytes::from(writer.into_inner()?))
}

fn decode_batches(data: Bytes, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(Cursor::new(data), None)?;
    if reader.schema() != *schema {
        return Err(DataFusionError::Execution(
            "Riffle Arrow schema does not match the stage input".into(),
        ));
    }
    reader
        .map(|batch| batch.map_err(DataFusionError::from))
        .collect()
}
