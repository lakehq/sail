use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use url::Url;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey};
use crate::shuffle::ShuffleCompression;
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// Object-storage implementation for a blocking shuffle stream. Every task attempt owns a
/// directory, allowing retries to be selected by the task scheduler without overwriting an
/// earlier attempt.
#[derive(Clone)]
pub(crate) struct StorageStreamManager {
    path: Option<String>,
    session_id: String,
    max_file_size: usize,
    compression: ShuffleCompression,
}

impl StorageStreamManager {
    pub(crate) fn new(
        path: Option<String>,
        session_id: String,
        max_file_size: usize,
        compression: ShuffleCompression,
    ) -> Self {
        Self {
            path,
            session_id,
            max_file_size,
            compression,
        }
    }

    pub(crate) fn create_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
        context: &TaskContext,
    ) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        if self.max_file_size == 0 {
            return Err(ExecutionError::InvalidArgument(
                "cluster.shuffle_backend.storage.max_file_size must be greater than zero"
                    .to_string(),
            ));
        }
        let (store, prefix) = self.store_and_prefix(&key, context)?;
        let options = IpcWriteOptions::default()
            .try_with_compression(match self.compression {
                ShuffleCompression::None => None,
                ShuffleCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                ShuffleCompression::Zstd => Some(CompressionType::ZSTD),
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Box::new(StorageStreamSink::new(
            store,
            prefix,
            schema,
            options,
            self.max_file_size,
        )?))
    }

    pub(crate) fn fetch_stream(
        &self,
        key: TaskStreamKey,
        _schema: SchemaRef,
        context: &TaskContext,
    ) -> ExecutionResult<TaskStreamSource> {
        let (store, prefix) = self.store_and_prefix(&key, context)?;
        let list_store = Arc::clone(&store);
        let output = futures::stream::once(async move {
            let mut locations = list_store
                .list(Some(&prefix))
                .map_err(|e| TaskStreamError::External(Arc::new(e)))
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .map(|meta| meta.location)
                .collect::<Vec<_>>();
            locations.sort();
            Ok::<_, TaskStreamError>(locations)
        })
        .flat_map(move |result| -> TaskStreamSource {
            match result {
                Ok(locations) => Box::pin(futures::stream::try_unfold(
                    StorageReadState {
                        store: Arc::clone(&store),
                        locations: locations.into_iter(),
                        reader: None,
                    },
                    |mut state| async move {
                        loop {
                            if let Some(reader) = state.reader.as_mut() {
                                match reader.next() {
                                    Some(Ok(batch)) => return Ok(Some((batch, state))),
                                    Some(Err(error)) => {
                                        return Err(TaskStreamError::External(Arc::new(error)));
                                    }
                                    None => state.reader = None,
                                }
                            }
                            let Some(location) = state.locations.next() else {
                                return Ok(None);
                            };
                            let bytes = state
                                .store
                                .get(&location)
                                .await
                                .map_err(|e| TaskStreamError::External(Arc::new(e)))?
                                .bytes()
                                .await
                                .map_err(|e| TaskStreamError::External(Arc::new(e)))?;
                            let reader = StreamReader::try_new(Cursor::new(bytes), None)
                                .map_err(|e| TaskStreamError::External(Arc::new(e)))?;
                            state.reader = Some(Box::new(reader));
                        }
                    },
                )),
                Err(error) => Box::pin(futures::stream::iter(vec![Err(error)])),
            }
        });
        Ok(Box::pin(output))
    }

    pub(crate) async fn remove_streams(
        &self,
        job_id: JobId,
        stage: Option<usize>,
        context: &TaskContext,
    ) -> Result<()> {
        // All shuffle data for a job is beneath this prefix, so terminal job cleanup removes
        // every attempt. Stage cleanup can safely reclaim data once all of its consumers finish.
        let Some((store, prefix)) = self
            .store_and_cleanup_prefix(job_id, stage, context)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        else {
            return Ok(());
        };
        let locations = store
            .list(Some(&prefix))
            .map_ok(|meta| meta.location)
            .boxed();
        store
            .delete_stream(locations)
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    fn store_and_prefix(
        &self,
        key: &TaskStreamKey,
        context: &TaskContext,
    ) -> ExecutionResult<(Arc<dyn ObjectStore>, Path)> {
        let path = self.path.as_deref().ok_or_else(|| {
            ExecutionError::InvalidArgument("missing shuffle storage path".to_string())
        })?;
        let url = Url::parse(path).map_err(|e| ExecutionError::InvalidArgument(e.to_string()))?;
        let store = context
            .runtime_env()
            .object_store_registry
            .get_store(&url)?;
        Ok((store, stream_prefix(&url, key, &self.session_id)?))
    }

    fn store_and_cleanup_prefix(
        &self,
        job_id: JobId,
        stage: Option<usize>,
        context: &TaskContext,
    ) -> ExecutionResult<Option<(Arc<dyn ObjectStore>, Path)>> {
        let Some(path) = self.path.as_deref() else {
            return Ok(None);
        };
        let url = Url::parse(path).map_err(|e| ExecutionError::InvalidArgument(e.to_string()))?;
        let store = context
            .runtime_env()
            .object_store_registry
            .get_store(&url)?;
        Ok(Some((
            store,
            shuffle_prefix(&url, &self.session_id, job_id, stage)?,
        )))
    }
}

struct StorageReadState {
    store: Arc<dyn ObjectStore>,
    locations: std::vec::IntoIter<Path>,
    reader: Option<StorageBatchReader>,
}

type StorageBatchReader =
    Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send>;

struct StorageStreamSink {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    schema: SchemaRef,
    options: IpcWriteOptions,
    max_file_size: usize,
    writer: Option<StreamWriter<Vec<u8>>>,
    file_index: usize,
    has_batches: bool,
}

impl StorageStreamSink {
    fn new(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        schema: SchemaRef,
        options: IpcWriteOptions,
        max_file_size: usize,
    ) -> Result<Self> {
        let writer =
            StreamWriter::try_new_with_options(Vec::new(), schema.as_ref(), options.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            store,
            prefix,
            schema,
            options,
            max_file_size,
            writer: Some(writer),
            file_index: 0,
            has_batches: false,
        })
    }

    fn new_writer(&self) -> Result<StreamWriter<Vec<u8>>> {
        StreamWriter::try_new_with_options(Vec::new(), self.schema.as_ref(), self.options.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn flush_file(&mut self) -> Result<()> {
        if !self.has_batches {
            return Ok(());
        }
        let mut writer = self.writer.take().ok_or_else(|| {
            DataFusionError::Internal("storage shuffle writer is not present".to_string())
        })?;
        writer
            .finish()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let bytes = writer
            .into_inner()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let path = self
            .prefix
            .clone()
            .join(format!("part-{:020}.arrow", self.file_index));
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.file_index += 1;
        self.has_batches = false;
        self.writer = Some(self.new_writer()?);
        Ok(())
    }
}

#[tonic::async_trait]
impl TaskStreamChannelSink for StorageStreamSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        let result = (|| -> Result<bool> {
            let writer = self.writer.as_mut().ok_or_else(|| {
                DataFusionError::Internal("storage shuffle writer is not present".to_string())
            })?;
            writer
                .write(&batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            self.has_batches = true;
            Ok(writer.get_ref().len() >= self.max_file_size)
        })();
        if result? {
            self.flush_file().await?;
        }
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.flush_file().await
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        let locations = self
            .store
            .list(Some(&self.prefix))
            .map_ok(|meta| meta.location)
            .boxed();
        self.store
            .delete_stream(locations)
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }
}

fn stream_prefix(url: &Url, key: &TaskStreamKey, session_id: &str) -> ExecutionResult<Path> {
    Ok(
        shuffle_prefix(url, session_id, key.job_id, Some(key.stage))?
            .join(format!("partition-{}", key.partition))
            .join(format!("attempt-{}", key.attempt))
            .join(format!("channel-{}", key.channel)),
    )
}

fn shuffle_prefix(
    url: &Url,
    session_id: &str,
    job_id: JobId,
    stage: Option<usize>,
) -> ExecutionResult<Path> {
    let mut prefix = Path::from_url_path(url.path())
        .map_err(|e| ExecutionError::InvalidArgument(e.to_string()))?
        .join(session_id)
        .join(format!("job-{job_id}"));
    if let Some(stage) = stage {
        prefix = prefix.join(format!("stage-{stage}"));
    }
    Ok(prefix)
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shuffle_prefix_decodes_url_path() {
        let url = Url::parse("file:///tmp/sail%20shuffle").unwrap();
        assert_eq!(
            shuffle_prefix(&url, "session-1", JobId::from(1), Some(2)).unwrap(),
            Path::from("tmp/sail shuffle/session-1/job-1/stage-2")
        );
    }

    #[test]
    fn stream_prefix_includes_channel() {
        let url = Url::parse("memory:///shuffle").unwrap();
        let key = TaskStreamKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
            channel: 5,
        };
        let prefix = stream_prefix(&url, &key, "session-1").unwrap();
        assert_eq!(
            prefix,
            Path::from("shuffle/session-1/job-1/stage-2/partition-3/attempt-4/channel-5")
        );
    }

    #[test]
    fn missing_storage_path_is_invalid() {
        let manager =
            StorageStreamManager::new(None, "session-1".to_string(), 1, ShuffleCompression::None);
        let key = TaskStreamKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
            channel: 5,
        };
        let error = manager
            .store_and_prefix(&key, &TaskContext::default())
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid argument: missing shuffle storage path"
        );
    }
}
