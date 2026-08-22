use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use futures::{StreamExt, TryStreamExt};
use sail_celeborn::shuffle::ShuffleClient;

use crate::id::{JobId, TaskKey};
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamSink, TaskStreamWriteState};

#[derive(Clone)]
pub(crate) struct CelebornStreamManager {
    client: ShuffleClient,
}

impl CelebornStreamManager {
    pub(crate) fn new(client: ShuffleClient) -> Self {
        Self { client }
    }

    pub(crate) async fn stop(&self) {
        let _ = self.client.stop().await;
    }

    pub(crate) async fn remove_streams(
        &self,
        job_id: JobId,
        stage: Option<usize>,
        unregister: bool,
    ) -> Result<()> {
        let shuffle_ids = self
            .client
            .get_job_shuffle_ids(job_id.into())
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        for (stream_stage, shuffle_id) in shuffle_ids {
            if stage.is_none_or(|stage| stream_stage == stage as u64) {
                if unregister {
                    self.client
                        .unregister_shuffle(shuffle_id)
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                }
                self.client
                    .clean_up_shuffle(shuffle_id)
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
            }
        }
        Ok(())
    }

    pub(crate) async fn create_stream(
        &self,
        key: TaskKey,
        mappers: usize,
        channels: usize,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>> {
        // A Celeborn shuffle spans every map task and reduce channel in one producer stage.
        let shuffle_id = self
            .client
            .get_shuffle_id(key.job_id.into(), key.stage as u64)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        self.client
            .register_shuffle(
                shuffle_id,
                (0..channels)
                    .map(|channel| {
                        i32::try_from(channel)
                            .map_err(|error| DataFusionError::External(Box::new(error)))
                    })
                    .collect::<Result<Vec<_>>>()?,
                false,
                0,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let map_id = i32::try_from(key.partition)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let attempt_id = i32::try_from(key.attempt)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let sinks = (0..channels)
            .map(|channel| {
                Ok(Some(CelebornStreamSink {
                    client: self.client.clone(),
                    shuffle_id,
                    partition_id: i32::try_from(channel)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?,
                    map_id,
                    attempt_id,
                    schema: schema.clone(),
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(CelebornTaskStreamSink {
            sinks,
            client: self.client.clone(),
            shuffle_id,
            map_id,
            attempt_id,
            num_mappers: i32::try_from(mappers)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        }))
    }

    pub(crate) async fn fetch_stream(
        &self,
        job_id: JobId,
        stage: usize,
        channels: Vec<usize>,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let shuffle_id = self
            .client
            .get_shuffle_id(job_id.into(), stage as u64)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let partition_ids = channels
            .iter()
            .map(|channel| {
                i32::try_from(*channel).map_err(|error| DataFusionError::External(Box::new(error)))
            })
            .collect::<Result<Vec<_>>>()?;
        // This initializes routing information in this task runner's client. The
        // lifecycle manager returns the existing reservation for this shuffle.
        self.client
            .register_shuffle(shuffle_id, partition_ids.clone(), false, 0)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let streams = futures::future::join_all(
            partition_ids
                .into_iter()
                .map(|partition_id| self.client.read_partition_stream(shuffle_id, partition_id)),
        )
        .await;
        let streams = streams
            .into_iter()
            .map(|stream| {
                let schema = schema.clone();
                Box::pin(
                    stream
                        .map(move |result| {
                            result
                                .map_err(|error| TaskStreamError::External(Arc::new(error)))
                                .and_then(|data| {
                                    decode_batches(data, &schema).map_err(|error| {
                                        TaskStreamError::Unknown(error.to_string())
                                    })
                                })
                        })
                        .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                        .try_flatten(),
                ) as TaskStreamSource
            })
            .collect::<Vec<TaskStreamSource>>();
        Ok(Box::pin(futures::stream::select_all(streams)))
    }
}

struct CelebornStreamSink {
    client: ShuffleClient,
    shuffle_id: i32,
    partition_id: i32,
    map_id: i32,
    attempt_id: i32,
    schema: SchemaRef,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for CelebornStreamSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        // Reserve the frame length prefix before serializing Arrow IPC so the payload can be
        // passed to Celeborn without copying it into a second framing buffer.
        let mut writer = StreamWriter::try_new(vec![0; size_of::<u32>()], self.schema.as_ref())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        writer
            .write(&batch)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        writer
            .finish()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut data = writer
            .into_inner()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let length = data.len().checked_sub(size_of::<u32>()).ok_or_else(|| {
            DataFusionError::Execution("invalid Celeborn frame length".to_string())
        })?;
        let length =
            u32::try_from(length).map_err(|error| DataFusionError::External(Box::new(error)))?;
        data[..size_of::<u32>()].copy_from_slice(&length.to_be_bytes());
        self.client
            .push_data(
                self.shuffle_id,
                self.partition_id,
                self.map_id,
                self.attempt_id,
                Bytes::from(data),
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

struct CelebornTaskStreamSink {
    sinks: Vec<Option<CelebornStreamSink>>,
    client: ShuffleClient,
    shuffle_id: i32,
    map_id: i32,
    attempt_id: i32,
    num_mappers: i32,
}

#[tonic::async_trait]
impl TaskStreamSink for CelebornTaskStreamSink {
    async fn write(&mut self, channel: usize, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        let state = match self.sinks.get_mut(channel).ok_or_else(|| {
            DataFusionError::Execution(format!("shuffle output channel {channel} not found"))
        })? {
            Some(sink) => sink.write(batch).await?,
            None => TaskStreamWriteState::Closed,
        };
        if state == TaskStreamWriteState::Closed {
            self.sinks[channel] = None;
        }
        Ok(if self.sinks.iter().any(Option::is_some) {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.client
            .mapper_end(
                self.shuffle_id,
                self.map_id,
                self.attempt_id,
                self.num_mappers,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn decode_batches(data: Bytes, _schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let mut offset = 0;
    let mut batches = vec![];
    while offset < data.len() {
        let Some(header) = data.get(offset..offset + size_of::<u32>()) else {
            return Err(DataFusionError::Execution(
                "truncated Celeborn shuffle frame header".to_string(),
            ));
        };
        let length =
            u32::from_be_bytes(header.try_into().map_err(|_| {
                DataFusionError::Execution("invalid Celeborn frame header".to_string())
            })?) as usize;
        offset += size_of::<u32>();
        let Some(frame) = data.get(offset..offset + length) else {
            return Err(DataFusionError::Execution(
                "truncated Celeborn shuffle frame".to_string(),
            ));
        };
        let reader = StreamReader::try_new(Cursor::new(frame), None)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        for batch in reader {
            batches.push(batch.map_err(|error| DataFusionError::External(Box::new(error)))?);
        }
        offset += length;
    }
    Ok(batches)
}
