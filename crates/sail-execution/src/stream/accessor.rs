use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use sail_common::actor::{Actor, ActorHandle};

use crate::id::TaskKey;
pub(crate) use crate::stream::local::accessor::LocalStreamAccessorMessage;
use crate::stream::local::accessor::{LocalTaskStreamReader, LocalTaskStreamWriter};
use crate::stream::reader::TaskStreamReader;
pub(crate) use crate::stream::storage::accessor::StorageStreamAccessorMessage;
use crate::stream::storage::accessor::{StorageTaskStreamReader, StorageTaskStreamWriter};
use crate::stream::writer::{
    TaskStreamChannelSink, TaskStreamSink, TaskStreamWriteState, TaskStreamWriter,
};
use crate::task::definition::{TaskInput, TaskInputLocator, TaskOutput, TaskOutputLocator};

pub struct TaskStreamFactory<T: Actor> {
    handle: ActorHandle<T>,
    context: Arc<TaskContext>,
}

impl<T: Actor> Clone for TaskStreamFactory<T> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            context: self.context.clone(),
        }
    }
}

impl<T: Actor> TaskStreamFactory<T> {
    pub fn new(handle: ActorHandle<T>, context: Arc<TaskContext>) -> Self {
        Self { handle, context }
    }

    pub fn reader(
        &self,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Arc<dyn TaskStreamReader>
    where
        T::Message: LocalStreamAccessorMessage + StorageStreamAccessorMessage,
    {
        match &input.locator {
            TaskInputLocator::Storage { .. } => Arc::new(StorageTaskStreamReader::new(
                self.handle.clone(),
                self.context.clone(),
                key,
                input,
                schema,
            )),
            TaskInputLocator::Driver { .. } | TaskInputLocator::Worker { .. } => Arc::new(
                LocalTaskStreamReader::new(self.handle.clone(), key, input, schema),
            ),
        }
    }

    pub fn writer(
        &self,
        key: TaskKey,
        output: TaskOutput,
        schema: SchemaRef,
    ) -> Arc<dyn TaskStreamWriter>
    where
        T::Message: LocalStreamAccessorMessage + StorageStreamAccessorMessage,
    {
        let channels = output.channels();
        match output.locator {
            TaskOutputLocator::Pipelined { replicas } => Arc::new(LocalTaskStreamWriter::new(
                self.handle.clone(),
                key,
                channels,
                replicas,
                schema,
            )),
            TaskOutputLocator::Blocking => Arc::new(StorageTaskStreamWriter::new(
                self.handle.clone(),
                self.context.clone(),
                key,
                channels,
                schema,
            )),
        }
    }
}

pub(crate) struct MultiChannelTaskStreamSink {
    pub(crate) sinks: Vec<Option<Box<dyn TaskStreamChannelSink>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for MultiChannelTaskStreamSink {
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
        for sink in self.sinks.into_iter().flatten() {
            sink.commit().await?;
        }
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        for sink in self.sinks.into_iter().flatten() {
            sink.abort().await?;
        }
        Ok(())
    }
}
