use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use sail_server::actor::{Actor, ActorHandle};
use tokio::sync::oneshot;

use crate::stream::reader::{TaskReadLocation, TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{TaskStreamSink, TaskStreamWriter, TaskWriteLocation};
use crate::stream_accessor::{StreamAccessor, StreamAccessorMessage};

impl<T: Actor> StreamAccessor<T> {
    pub fn new(handle: ActorHandle<T>) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl<T: Actor> TaskStreamReader for StreamAccessor<T>
where
    T::Message: StreamAccessorMessage,
{
    async fn open(
        &self,
        location: &TaskReadLocation,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (tx, rx) = oneshot::channel();
        let event = match location {
            TaskReadLocation::Driver { key } => {
                T::Message::fetch_driver_stream(key.clone(), schema, tx)
            }
            TaskReadLocation::Worker { worker_id, key } => {
                T::Message::fetch_worker_stream(*worker_id, key.clone(), schema, tx)
            }
            TaskReadLocation::Remote { uri, key } => {
                T::Message::fetch_remote_stream(uri.clone(), key.clone(), schema, tx)
            }
        };
        self.handle.send(event).await.map_err(|_| {
            DataFusionError::Internal("actor send error for stream reader".to_string())
        })?;
        rx.await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

#[tonic::async_trait]
impl<T: Actor> TaskStreamWriter for StreamAccessor<T>
where
    T::Message: StreamAccessorMessage,
{
    async fn open(
        &self,
        location: &TaskWriteLocation,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>> {
        let (tx, rx) = oneshot::channel();
        let event = match location {
            TaskWriteLocation::Local { key, storage } => {
                T::Message::create_local_stream(key.clone(), *storage, schema, tx)
            }
            TaskWriteLocation::Remote { uri, key } => {
                T::Message::create_remote_stream(uri.clone(), key.clone(), schema, tx)
            }
        };
        self.handle.send(event).await.map_err(|_| {
            DataFusionError::Internal("actor send error for stream writer".to_string())
        })?;
        rx.await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
