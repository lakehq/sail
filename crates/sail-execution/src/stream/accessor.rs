// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use futures::future::{join_all, try_join_all};
use indexmap::IndexMap;
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskStreamKey, TaskStreamKeySet, WorkerId};
use crate::stream::merge::merged_stream;
use crate::stream::reader::{TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{
    MultiChannelTaskStreamSink, TaskStreamChannelSink, TaskStreamSink, TaskStreamWriter,
};
use crate::task::definition::{
    TaskInput, TaskInputKey, TaskInputLocator, TaskOutput, TaskOutputLocator,
};
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

pub struct TaskStreamFactory {
    handle: ActorHandle<TaskRunnerActor>,
    context: Arc<TaskContext>,
    celeborn: bool,
    mappers: usize,
}

impl Clone for TaskStreamFactory {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            context: self.context.clone(),
            celeborn: self.celeborn,
            mappers: self.mappers,
        }
    }
}

impl TaskStreamFactory {
    pub fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        celeborn: bool,
        mappers: usize,
    ) -> Self {
        Self {
            handle,
            context,
            celeborn,
            mappers,
        }
    }

    pub fn reader(
        &self,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Arc<dyn TaskStreamReader> {
        Arc::new(MultiChannelTaskStreamReader::new(
            self.handle.clone(),
            self.context.clone(),
            key,
            input,
            schema,
        ))
    }

    pub fn writer(
        &self,
        key: TaskKey,
        output: TaskOutput,
        schema: SchemaRef,
    ) -> Arc<dyn TaskStreamWriter> {
        if self.celeborn && matches!(output.locator, TaskOutputLocator::Blocking) {
            Arc::new(CelebornTaskStreamWriter::new(
                self.handle.clone(),
                self.context.clone(),
                key,
                output.channels(),
                schema,
                self.mappers,
            ))
        } else {
            Arc::new(MultiChannelTaskStreamWriter::new(
                self.handle.clone(),
                self.context.clone(),
                key,
                output,
                schema,
            ))
        }
    }
}

struct TaskStreamAccessor {
    handle: ActorHandle<TaskRunnerActor>,
    context: Arc<TaskContext>,
}

impl TaskStreamAccessor {
    fn new(handle: ActorHandle<TaskRunnerActor>, context: Arc<TaskContext>) -> Self {
        Self { handle, context }
    }

    async fn receive<R>(
        &self,
        message: TaskRunnerMessage,
        rx: oneshot::Receiver<ExecutionResult<R>>,
    ) -> Result<R> {
        self.handle.send(message).await.map_err(|_| {
            DataFusionError::Internal("actor send error for task stream accessor".to_string())
        })?;
        rx.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    async fn create_local_stream(
        &self,
        key: TaskKey,
        channels: usize,
        replicas: usize,
        bounded: bool,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::CreateLocalStream {
                key,
                channels,
                replicas,
                bounded,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn create_storage_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamChannelSink>> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::CreateStorageStream {
                key,
                schema,
                context: self.context.clone(),
                result,
            },
            rx,
        )
        .await
    }

    async fn create_celeborn_stream(
        &self,
        key: TaskKey,
        mappers: usize,
        channels: usize,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::CreateCelebornStream {
                key,
                mappers,
                channels,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_driver_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchDriverStream {
                key,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_worker_streams(
        &self,
        worker_id: WorkerId,
        keys: TaskStreamKeySet,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchWorkerStreams {
                worker_id,
                keys,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_storage_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchStorageStream {
                key,
                schema,
                context: self.context.clone(),
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_celeborn_stream(
        &self,
        job_id: JobId,
        stage: usize,
        channels: Vec<usize>,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchCelebornStream {
                job_id,
                stage,
                channels,
                schema,
                result,
            },
            rx,
        )
        .await
    }
}

pub(crate) struct MultiChannelTaskStreamReader {
    streams: TaskStreamAccessor,
    key: TaskKey,
    input: TaskInput,
    schema: SchemaRef,
}

impl MultiChannelTaskStreamReader {
    fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Self {
        Self {
            streams: TaskStreamAccessor::new(handle, context),
            key,
            input,
            schema,
        }
    }
}

impl fmt::Debug for MultiChannelTaskStreamReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiChannelTaskStreamReader").finish()
    }
}

#[tonic::async_trait]
impl TaskStreamReader for MultiChannelTaskStreamReader {
    async fn open(&self, partition: usize) -> Result<TaskStreamSource> {
        let streams = match self.input.locator.as_ref() {
            TaskInputLocator::Driver { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                try_join_all(keys.iter().map(|key| {
                    self.streams.fetch_driver_stream(
                        key.task_stream_key(
                            self.input.producer_job_id(self.key.job_id),
                            self.input.stage,
                        ),
                        self.schema.clone(),
                    )
                }))
                .await?
            }
            TaskInputLocator::Worker { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                let keys = worker_key_sets(
                    self.input.producer_job_id(self.key.job_id),
                    self.input.stage,
                    keys,
                );
                try_join_all(keys.into_iter().map(|(worker_id, keys)| {
                    self.streams
                        .fetch_worker_streams(worker_id, keys, self.schema.clone())
                }))
                .await?
            }
            TaskInputLocator::Storage { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                try_join_all(keys.iter().map(|key| {
                    self.streams.fetch_storage_stream(
                        key.task_stream_key(
                            self.input.producer_job_id(self.key.job_id),
                            self.input.stage,
                        ),
                        self.schema.clone(),
                    )
                }))
                .await?
            }
            TaskInputLocator::ShuffleService { channels } => {
                let channels = channels.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                vec![
                    self.streams
                        .fetch_celeborn_stream(
                            self.input.producer_job_id(self.key.job_id),
                            self.input.stage,
                            channels.clone(),
                            self.schema.clone(),
                        )
                        .await?,
                ]
            }
        };
        Ok(merged_stream(self.schema.clone(), streams))
    }

    fn close_unopened(&self) -> bool {
        // Pipelined streams stay in the memory of the producing tasks until they are read.
        matches!(
            self.input.locator.as_ref(),
            TaskInputLocator::Driver { .. } | TaskInputLocator::Worker { .. }
        )
    }

    async fn release(&self, partition: usize) -> Result<()> {
        match self.input.locator.as_ref() {
            TaskInputLocator::Worker { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                let keys = worker_key_sets(self.key.job_id, self.input.stage, keys);
                // The streams of each worker are released whether or not the release
                // has failed for another worker.
                let released = join_all(keys.into_iter().map(|(worker_id, mut keys)| {
                    keys.release = true;
                    self.streams
                        .fetch_worker_streams(worker_id, keys, self.schema.clone())
                }))
                .await;
                released.into_iter().try_for_each(|x| x.map(|_| ()))
            }
            // The streams on the driver are opened and closed.
            TaskInputLocator::Driver { .. } => self.open(partition).await.map(|_| ()),
            TaskInputLocator::Storage { .. } | TaskInputLocator::ShuffleService { .. } => Ok(()),
        }
    }
}

/// Groups the streams that a single call of [`TaskStreamReader::open`] reads from
/// workers, so that the streams on each worker are fetched in one request.
///
/// The streams of a group have one consumer, which is the source that the call
/// returns. It reads all of them in arbitrary order and never waits for the data of
/// one of them while it leaves the data of another one unread. This is required for
/// streams that are fetched in one request, since their data arrives through one
/// Flight stream, where data that is not read holds back all the data after it.
/// So the streams of different calls or of different tasks must never be grouped,
/// and the data of a group must never be divided again among several consumers.
/// An input that is read in order has a call for each stream of that order.
fn worker_key_sets(
    job_id: JobId,
    stage: usize,
    keys: &[(WorkerId, TaskInputKey)],
) -> Vec<(WorkerId, TaskStreamKeySet)> {
    // The channels of each task on each worker.
    let mut workers: IndexMap<WorkerId, IndexMap<(usize, usize), Vec<usize>>> = IndexMap::new();
    for (worker_id, key) in keys {
        workers
            .entry(*worker_id)
            .or_default()
            .entry((key.partition, key.attempt))
            .or_default()
            .push(key.channel);
    }
    let mut sets = vec![];
    for (worker_id, tasks) in workers {
        // A set has the same channels for all its tasks. This holds for all the tasks
        // on a worker in the inputs that the scheduler creates, so that there is one
        // set for each worker. Other tasks would get a set of their own.
        let mut channels: IndexMap<Vec<usize>, Vec<(usize, usize)>> = IndexMap::new();
        for (task, task_channels) in tasks {
            channels.entry(task_channels).or_default().push(task);
        }
        for (channels, tasks) in channels {
            let keys = TaskStreamKeySet {
                job_id,
                stage,
                tasks,
                channels,
                release: false,
            };
            sets.push((worker_id, keys));
        }
    }
    sets
}

pub(crate) struct MultiChannelTaskStreamWriter {
    streams: TaskStreamAccessor,
    key: TaskKey,
    output: TaskOutput,
    schema: SchemaRef,
}

impl MultiChannelTaskStreamWriter {
    fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        key: TaskKey,
        output: TaskOutput,
        schema: SchemaRef,
    ) -> Self {
        Self {
            streams: TaskStreamAccessor::new(handle, context),
            key,
            output,
            schema,
        }
    }
}

impl fmt::Debug for MultiChannelTaskStreamWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiChannelTaskStreamWriter").finish()
    }
}

#[tonic::async_trait]
impl TaskStreamWriter for MultiChannelTaskStreamWriter {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        if partition != self.key.partition {
            return Err(DataFusionError::Execution(format!(
                "task stream writer for partition {} cannot open partition {partition}",
                self.key.partition
            )));
        }
        let channels = self.output.channels();
        match &self.output.locator {
            TaskOutputLocator::Pipelined { replicas, bounded } => {
                self.streams
                    .create_local_stream(
                        self.key.clone(),
                        channels,
                        *replicas,
                        *bounded,
                        self.schema.clone(),
                    )
                    .await
            }
            TaskOutputLocator::Blocking => {
                let sinks = try_join_all((0..channels).map(|channel| {
                    self.streams.create_storage_stream(
                        self.key.task_stream_key(channel),
                        self.schema.clone(),
                    )
                }))
                .await?;
                Ok(Box::new(MultiChannelTaskStreamSink {
                    sinks: sinks.into_iter().map(Some).collect(),
                }))
            }
        }
    }
}

pub(crate) struct CelebornTaskStreamWriter {
    streams: TaskStreamAccessor,
    key: TaskKey,
    channels: usize,
    schema: SchemaRef,
    mappers: usize,
}

impl CelebornTaskStreamWriter {
    fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        key: TaskKey,
        channels: usize,
        schema: SchemaRef,
        mappers: usize,
    ) -> Self {
        Self {
            streams: TaskStreamAccessor::new(handle, context),
            key,
            channels,
            schema,
            mappers,
        }
    }
}

impl fmt::Debug for CelebornTaskStreamWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelebornTaskStreamWriter").finish()
    }
}

#[tonic::async_trait]
impl TaskStreamWriter for CelebornTaskStreamWriter {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        if partition != self.key.partition {
            return Err(DataFusionError::Execution(format!(
                "task stream writer for partition {} cannot open partition {partition}",
                self.key.partition
            )));
        }
        self.streams
            .create_celeborn_stream(
                self.key.clone(),
                self.mappers,
                self.channels,
                self.schema.clone(),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_key(partition: usize, attempt: usize, channel: usize) -> TaskInputKey {
        TaskInputKey {
            partition,
            attempt,
            channel,
        }
    }

    /// The worker, the tasks and the channels of each set.
    type Sets = Vec<(u64, Vec<(usize, usize)>, Vec<usize>)>;

    fn sets(keys: &[(u64, TaskInputKey)]) -> Sets {
        let keys = keys
            .iter()
            .map(|(worker_id, key)| (WorkerId::from(*worker_id), key.clone()))
            .collect::<Vec<_>>();
        let sets = worker_key_sets(JobId::from(1), 2, &keys);
        // The sets have exactly the streams of the keys.
        let mut expected = keys
            .iter()
            .map(|(worker_id, key)| (*worker_id, key.task_stream_key(JobId::from(1), 2)))
            .collect::<Vec<_>>();
        let mut actual = sets
            .iter()
            .flat_map(|(worker_id, keys)| keys.keys().map(move |key| (*worker_id, key)))
            .collect::<Vec<_>>();
        let order = |(worker_id, key): &(WorkerId, TaskStreamKey)| {
            (
                u64::from(*worker_id),
                key.partition,
                key.attempt,
                key.channel,
            )
        };
        expected.sort_by_key(order);
        actual.sort_by_key(order);
        assert_eq!(actual, expected);
        sets.into_iter()
            .map(|(worker_id, keys)| (worker_id.into(), keys.tasks, keys.channels))
            .collect()
    }

    #[test]
    fn streams_of_one_call_are_grouped_by_worker() {
        // A shuffle input has one channel of all tasks. The attempts can differ.
        let keys = [
            (7, input_key(0, 0, 3)),
            (5, input_key(1, 2, 3)),
            (7, input_key(2, 0, 3)),
            (9, input_key(3, 0, 3)),
            (5, input_key(4, 0, 3)),
        ];
        assert_eq!(
            sets(&keys),
            vec![
                (7, vec![(0, 0), (2, 0)], vec![3]),
                (5, vec![(1, 2), (4, 0)], vec![3]),
                (9, vec![(3, 0)], vec![3]),
            ]
        );

        // A broadcast input has all channels of all tasks.
        let keys = [
            (7, input_key(0, 0, 0)),
            (7, input_key(0, 0, 1)),
            (5, input_key(1, 0, 0)),
            (5, input_key(1, 0, 1)),
            (7, input_key(2, 1, 0)),
            (7, input_key(2, 1, 1)),
        ];
        assert_eq!(
            sets(&keys),
            vec![
                (7, vec![(0, 0), (2, 1)], vec![0, 1]),
                (5, vec![(1, 0)], vec![0, 1]),
            ]
        );

        // An input that is read in order has all channels of one task in each call,
        // so the streams of different tasks are never grouped.
        let keys = [(7, input_key(4, 0, 0)), (7, input_key(4, 0, 1))];
        assert_eq!(sets(&keys), vec![(7, vec![(4, 0)], vec![0, 1])]);

        assert_eq!(sets(&[]), vec![]);
    }

    #[test]
    fn tasks_with_different_channels_are_not_grouped() {
        let keys = [
            (7, input_key(0, 0, 0)),
            (7, input_key(0, 0, 1)),
            (7, input_key(1, 0, 1)),
            (7, input_key(2, 0, 0)),
            (7, input_key(2, 0, 1)),
            (7, input_key(3, 0, 1)),
            (7, input_key(3, 0, 1)),
        ];
        assert_eq!(
            sets(&keys),
            vec![
                (7, vec![(0, 0), (2, 0)], vec![0, 1]),
                (7, vec![(1, 0)], vec![1]),
                (7, vec![(3, 0)], vec![1, 1]),
            ]
        );
    }
}
