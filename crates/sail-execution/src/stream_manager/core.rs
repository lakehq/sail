use std::collections::HashMap;

use datafusion::arrow::datatypes::SchemaRef;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::stream_manager::local::{LocalStream, MemoryStream};
use crate::stream_manager::options::StreamManagerOptions;
use crate::stream_manager::StreamManager;

impl StreamManager {
    pub fn new(options: StreamManagerOptions) -> Self {
        Self {
            options,
            local_streams: HashMap::new(),
        }
    }

    pub fn create_local_stream(
        &mut self,
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        _schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let mut stream: Box<dyn LocalStream> = match storage {
            LocalStreamStorage::Memory { replicas } => Box::new(MemoryStream::new(
                self.options.worker_stream_buffer,
                replicas,
            )),
            LocalStreamStorage::Disk => {
                return Err(ExecutionError::InternalError(
                    "not implemented: local disk storage".to_string(),
                ))
            }
        };
        let sink = stream.publish()?;
        self.local_streams.insert(key, stream);
        Ok(sink)
    }

    pub fn create_remote_stream(
        &mut self,
        _uri: String,
        _key: TaskStreamKey,
        _schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        Err(ExecutionError::InternalError(
            "not implemented: remote stream".to_string(),
        ))
    }

    pub fn fetch_local_stream(&mut self, key: &TaskStreamKey) -> ExecutionResult<TaskStreamSource> {
        self.local_streams
            .get_mut(&key)
            .map(|x| {
                x.subscribe().map_err(|e| {
                    ExecutionError::InternalError(format!(
                        "failed to read task stream {}: {e}",
                        TaskStreamKeyDisplay(key)
                    ))
                })
            })
            .unwrap_or_else(|| {
                Err(ExecutionError::InternalError(format!(
                    "task stream not found: {}",
                    TaskStreamKeyDisplay(key)
                )))
            })
    }

    pub fn fetch_remote_stream(
        &mut self,
        _uri: String,
        _key: &TaskStreamKey,
        _schema: SchemaRef,
    ) -> ExecutionResult<TaskStreamSource> {
        Err(ExecutionError::InternalError(
            "not implemented: fetch remote stream".to_string(),
        ))
    }

    pub fn remove_local_stream(&mut self, job_id: JobId, stage: Option<usize>) {
        let mut keys = Vec::new();
        for key in self.local_streams.keys() {
            if key.job_id == job_id && stage.is_none_or(|x| key.stage == x) {
                keys.push(key.clone());
            }
        }
        for key in keys {
            self.local_streams.remove(&key);
        }
    }
}
