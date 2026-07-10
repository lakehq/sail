use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreRegistry;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sail_common::config::ShuffleCompression;
use sail_object_store::DynamicObjectStoreRegistry;
use url::Url;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamSink, TaskStreamSinkState};

/// Object-storage implementation for a blocking shuffle stream. Every task attempt owns a
/// directory, allowing retries to be selected by the task scheduler without overwriting an
/// earlier attempt.
pub(super) struct RemoteStreamStorage {
    registry: Arc<DynamicObjectStoreRegistry>,
    storage_url: String,
    max_file_size: usize,
    compression: ShuffleCompression,
}

impl RemoteStreamStorage {
    pub(super) fn new(
        registry: Arc<DynamicObjectStoreRegistry>,
        storage_url: String,
        max_file_size: usize,
        compression: ShuffleCompression,
    ) -> Self {
        Self {
            registry,
            storage_url,
            max_file_size,
            compression,
        }
    }

    pub(super) fn create_stream(
        &self,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        if self.max_file_size == 0 {
            return Err(ExecutionError::InvalidArgument(
                "execution.shuffle.max_file_size must be greater than zero".to_string(),
            ));
        }
        let (store, prefix) = self.store_and_prefix(&uri, &key)?;
        let options = IpcWriteOptions::default()
            .try_with_compression(match self.compression {
                ShuffleCompression::None => None,
                ShuffleCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                ShuffleCompression::Zstd => Some(CompressionType::ZSTD),
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Box::new(RemoteStreamSink::new(
            store,
            prefix,
            schema,
            options,
            self.max_file_size,
        )?))
    }

    pub(super) fn fetch_stream(
        &self,
        uri: String,
        key: TaskStreamKey,
        _schema: SchemaRef,
    ) -> ExecutionResult<TaskStreamSource> {
        let (store, prefix) = self.store_and_prefix(&uri, &key)?;
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
                    RemoteReadState {
                        store: Arc::clone(&store),
                        locations: locations.into_iter(),
                        batches: VecDeque::new(),
                    },
                    |mut state| async move {
                        loop {
                            if let Some(batch) = state.batches.pop_front() {
                                return Ok(Some((batch, state)));
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
                            state.batches = reader
                                .collect::<std::result::Result<Vec<_>, _>>()
                                .map_err(|e| TaskStreamError::External(Arc::new(e)))?
                                .into();
                        }
                    },
                )),
                Err(error) => Box::pin(futures::stream::iter(vec![Err(error)])),
            }
        });
        Ok(Box::pin(output))
    }

    pub(super) async fn remove_streams(&self, job_id: JobId, stage: Option<usize>) -> Result<()> {
        // All shuffle data for a job is beneath this prefix, so terminal job cleanup removes
        // every attempt. Stage cleanup can safely reclaim data once all of its consumers finish.
        let Some((store, prefix)) = self
            .store_and_cleanup_prefix(job_id, stage)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        else {
            return Ok(());
        };
        let locations = store
            .list(Some(&prefix))
            .map_err(|e| DataFusionError::External(Box::new(e)))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .into_iter()
            .map(|meta| Ok(meta.location));
        store
            .delete_stream(Box::pin(futures::stream::iter(locations)))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    fn store_and_prefix(
        &self,
        uri: &str,
        key: &TaskStreamKey,
    ) -> ExecutionResult<(Arc<dyn ObjectStore>, Path)> {
        let url = Url::parse(uri).map_err(|e| ExecutionError::InvalidArgument(e.to_string()))?;
        let store = self.registry.get_store(&url)?;
        let prefix = shuffle_prefix(&url, key.job_id, Some(key.stage));
        Ok((
            store,
            prefix.join(format!(
                "partition-{}/attempt-{}",
                key.partition, key.attempt
            )),
        ))
    }

    fn store_and_cleanup_prefix(
        &self,
        job_id: JobId,
        stage: Option<usize>,
    ) -> ExecutionResult<Option<(Arc<dyn ObjectStore>, Path)>> {
        if self.storage_url.is_empty() {
            return Ok(None);
        }
        let url = Url::parse(&self.storage_url)
            .map_err(|e| ExecutionError::InvalidArgument(e.to_string()))?;
        let store = self.registry.get_store(&url)?;
        Ok(Some((store, shuffle_prefix(&url, job_id, stage))))
    }
}

struct RemoteReadState {
    store: Arc<dyn ObjectStore>,
    locations: std::vec::IntoIter<Path>,
    batches: VecDeque<datafusion::arrow::array::RecordBatch>,
}

struct RemoteStreamSink {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    schema: SchemaRef,
    options: IpcWriteOptions,
    max_file_size: usize,
    writer: Option<StreamWriter<Vec<u8>>>,
    file_index: usize,
    has_batches: bool,
}

impl RemoteStreamSink {
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
        let writer = self.writer.take().ok_or_else(|| {
            DataFusionError::Internal("remote shuffle writer is not present".to_string())
        })?;
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
impl TaskStreamSink for RemoteStreamSink {
    async fn write(
        &mut self,
        batch: TaskStreamResult<datafusion::arrow::array::RecordBatch>,
    ) -> TaskStreamSinkState {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                return TaskStreamSinkState::Error(DataFusionError::External(Box::new(error)));
            }
        };
        let result = (|| -> Result<bool> {
            let writer = self.writer.as_mut().ok_or_else(|| {
                DataFusionError::Internal("remote shuffle writer is not present".to_string())
            })?;
            writer
                .write(&batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            self.has_batches = true;
            Ok(writer.get_ref().len() >= self.max_file_size)
        })();
        match result {
            Ok(true) => match self.flush_file().await {
                Ok(()) => TaskStreamSinkState::Ok,
                Err(e) => TaskStreamSinkState::Error(e),
            },
            Ok(false) => TaskStreamSinkState::Ok,
            Err(e) => TaskStreamSinkState::Error(e),
        }
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        self.flush_file().await
    }
}

fn shuffle_prefix(url: &Url, job_id: JobId, stage: Option<usize>) -> Path {
    let mut prefix = url.path().trim_matches('/').to_string();
    if !prefix.is_empty() {
        prefix.push('/');
    }
    prefix.push_str(&format!("job-{job_id}"));
    if let Some(stage) = stage {
        prefix.push_str(&format!("/stage-{stage}"));
    }
    Path::from(prefix)
}
