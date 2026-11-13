use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamDecoder;
use datafusion::common::{DataFusionError, Result, exec_err, internal_datafusion_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sail_common_datafusion::replay::{ReplayId, ReplayPartition, ReplayPartitionBuilder};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::id::JobId;
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

// Fixed-size objects form one IPC stream. No listing, object index, or complete
// IPC file is buffered by readers or writers. CRC is checked before IPC decode.
const CHUNK_BYTES: usize = 128 * 1024;
const MANIFEST_LIMIT: usize = 1024;

#[derive(Serialize, Deserialize)]
struct Manifest {
    version: u32,
    id: ReplayId,
    length: u64,
    rows: usize,
    decoder_memory_bytes: usize,
    chunks: u64,
}

#[derive(Default)]
pub(super) struct ReplayJobs(Mutex<HashMap<JobId, Arc<ReplayJob>>>);

impl ReplayJobs {
    pub(super) fn get(&self, id: JobId) -> Arc<ReplayJob> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(id)
            .or_default()
            .clone()
    }
}

/// Closing admission and registering an upload are serialized. Uploads outlive
/// canceled task futures until their in-flight store requests have settled.
#[derive(Default)]
pub(super) struct ReplayJob {
    closed: Mutex<bool>,
    cancellation: CancellationToken,
    uploads: TaskTracker,
}

impl ReplayJob {
    fn check(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return exec_err!("replay job is closed");
        }
        Ok(())
    }

    fn spawn(
        &self,
        upload: impl Future<Output = Result<()>> + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let closed = self
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *closed {
            return exec_err!("replay publication is closed");
        }
        Ok(self.uploads.spawn(upload))
    }

    pub(super) fn close(&self) {
        let mut closed = self
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *closed = true;
        self.cancellation.cancel();
        self.uploads.close();
    }

    pub(super) async fn wait(&self) {
        self.uploads.wait().await;
    }
}

pub(super) struct ReplayStorageSink {
    builder: Option<ReplayPartitionBuilder>,
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    id: ReplayId,
    runtime: Arc<RuntimeEnv>,
    job: Arc<ReplayJob>,
}

impl ReplayStorageSink {
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        id: ReplayId,
        schema: SchemaRef,
        runtime: Arc<RuntimeEnv>,
        partition: usize,
        job: Arc<ReplayJob>,
    ) -> Result<Self> {
        job.check()?;
        Ok(Self {
            builder: Some(ReplayPartitionBuilder::try_new(
                schema,
                runtime.clone(),
                partition,
            )?),
            store,
            prefix,
            id,
            runtime,
            job,
        })
    }
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[tonic::async_trait]
impl TaskStreamChannelSink for ReplayStorageSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        self.job.check()?;
        self.builder
            .as_mut()
            .ok_or_else(|| internal_datafusion_err!("closed replay sink"))?
            .append(batch)?;
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.job.check()?;
        let partition = self
            .builder
            .take()
            .ok_or_else(|| internal_datafusion_err!("closed replay sink"))?
            .seal()?;
        let cancel = self.job.cancellation.child_token();
        let _cancel = CancelOnDrop(cancel.clone());
        let job = self.job.clone();
        job.spawn(async move {
            let result = upload(
                self.store.clone(),
                &self.prefix,
                self.id,
                partition,
                self.runtime.clone(),
                &cancel,
            )
            .await;
            if result.is_err() {
                remove_prefix(self.store.clone(), &self.prefix).await?;
            }
            result
        })?
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.builder.take();
        Ok(())
    }
}

fn check_cancel(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        return exec_err!("replay upload canceled");
    }
    Ok(())
}

async fn put_chunk(store: &dyn ObjectStore, prefix: &Path, index: u64, bytes: &[u8]) -> Result<()> {
    let mut payload = Vec::with_capacity(bytes.len() + 4);
    payload.extend_from_slice(&crc32fast::hash(bytes).to_le_bytes());
    payload.extend_from_slice(bytes);
    store
        .put(
            &prefix.child(format!("chunk-{index:020}")),
            PutPayload::from(payload),
        )
        .await?;
    Ok(())
}

async fn upload(
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
    id: ReplayId,
    partition: ReplayPartition,
    runtime: Arc<RuntimeEnv>,
    cancel: &CancellationToken,
) -> Result<()> {
    let reservation = MemoryConsumer::new("ReplayStorageUpload").register(&runtime.memory_pool);
    reservation.try_grow(4 * CHUNK_BYTES + MANIFEST_LIMIT)?;
    let mut export = partition.export(&runtime)?;
    let mut pending = Vec::with_capacity(CHUNK_BYTES);
    let mut chunks = 0;
    while let Some(bytes) = export.chunks.next().await.transpose()? {
        let mut bytes = bytes.as_slice();
        while !bytes.is_empty() {
            let size = bytes.len().min(CHUNK_BYTES - pending.len());
            pending.extend_from_slice(&bytes[..size]);
            bytes = &bytes[size..];
            if pending.len() == CHUNK_BYTES {
                check_cancel(cancel)?;
                put_chunk(store.as_ref(), prefix, chunks, &pending).await?;
                chunks += 1;
                pending.clear();
            }
        }
    }
    if !pending.is_empty() {
        check_cancel(cancel)?;
        put_chunk(store.as_ref(), prefix, chunks, &pending).await?;
        chunks += 1;
    }
    check_cancel(cancel)?;
    let manifest = serde_json::to_vec(&Manifest {
        version: 1,
        id,
        length: export.length,
        rows: export.rows,
        decoder_memory_bytes: export.decoder_memory_bytes,
        chunks,
    })
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if manifest.len() > MANIFEST_LIMIT {
        return exec_err!("replay manifest exceeds limit");
    }
    store
        .put(&prefix.child("manifest.json"), PutPayload::from(manifest))
        .await?;
    // If closure raced with the final store operation, retract its publication
    // before releasing the tracked upload. Terminal cleanup waits for this.
    check_cancel(cancel)
}

pub(super) async fn remove_prefix(store: Arc<dyn ObjectStore>, prefix: &Path) -> Result<()> {
    let locations = store
        .list(Some(prefix))
        .map_ok(|meta| meta.location)
        .boxed();
    store
        .delete_stream(locations)
        .try_for_each(|_| futures::future::ready(Ok(())))
        .await?;
    Ok(())
}

struct ReadState {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    schema: SchemaRef,
    decoder: StreamDecoder,
    buffer: Buffer,
    manifest: Manifest,
    next: u64,
    rows: usize,
    _reservation: MemoryReservation,
    job: Arc<ReplayJob>,
}

pub(super) fn fetch(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    id: ReplayId,
    schema: SchemaRef,
    runtime: Arc<RuntimeEnv>,
    job: Arc<ReplayJob>,
) -> TaskStreamSource {
    let stream = futures::stream::once(async move {
        job.check()?;
        let reservation = MemoryConsumer::new("ReplayStorageRead").register(&runtime.memory_pool);
        reservation.try_grow(MANIFEST_LIMIT)?;
        let path = prefix.child("manifest.json");
        let meta = store.head(&path).await?;
        if meta.size == 0 || meta.size > MANIFEST_LIMIT as u64 {
            return exec_err!("invalid replay manifest length");
        }
        let bytes = store.get_range(&path, 0..meta.size).await?;
        let manifest: Manifest =
            serde_json::from_slice(&bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
        if manifest.version != 1
            || manifest.id != id
            || manifest.chunks != manifest.length.div_ceil(CHUNK_BYTES as u64)
            || (manifest.length == 0 && manifest.rows != 0)
        {
            return exec_err!("invalid replay publication metadata");
        }
        let budget = manifest
            .decoder_memory_bytes
            .checked_add(4 * CHUNK_BYTES + MANIFEST_LIMIT)
            .ok_or_else(|| internal_datafusion_err!("replay reader budget overflow"))?;
        reservation.try_resize(budget)?;
        Ok::<_, DataFusionError>(ReadState {
            store,
            prefix,
            schema,
            decoder: StreamDecoder::new(),
            buffer: Buffer::from_vec(Vec::<u8>::new()),
            manifest,
            next: 0,
            rows: 0,
            _reservation: reservation,
            job,
        })
    })
    .map_ok(|state| {
        futures::stream::try_unfold(state, |mut state| async move {
            loop {
                state.job.check()?;
                while !state.buffer.is_empty() {
                    if let Some(batch) = state.decoder.decode(&mut state.buffer)? {
                        if batch.schema() != state.schema {
                            return exec_err!("replay schema mismatch");
                        }
                        state.rows = state
                            .rows
                            .checked_add(batch.num_rows())
                            .ok_or_else(|| internal_datafusion_err!("replay row count overflow"))?;
                        if state.rows > state.manifest.rows {
                            return exec_err!("replay contains extra rows");
                        }
                        return Ok(Some((batch, state)));
                    }
                }
                if state.next == state.manifest.chunks {
                    state.decoder.finish()?;
                    if state.rows != state.manifest.rows {
                        return exec_err!("replay ended before sealed rows");
                    }
                    return Ok(None);
                }
                let path = state.prefix.child(format!("chunk-{:020}", state.next));
                let length = (state.manifest.length - state.next * CHUNK_BYTES as u64)
                    .min(CHUNK_BYTES as u64);
                if state.store.head(&path).await?.size != length + 4 {
                    return exec_err!("replay chunk length mismatch");
                }
                // Never request the complete IPC file: every range is at most128KiB.
                let bytes = state.store.get_range(&path, 0..length + 4).await?;
                let checksum = u32::from_le_bytes(
                    bytes[..4]
                        .try_into()
                        .map_err(|_| internal_datafusion_err!("invalid checksum"))?,
                );
                if crc32fast::hash(&bytes[4..]) != checksum {
                    return exec_err!("replay chunk checksum mismatch");
                }
                state.buffer = Buffer::from(bytes.slice(4..));
                state.next += 1;
            }
        })
    })
    .try_flatten()
    .map_err(|e| TaskStreamError::External(Arc::new(e)));
    Box::pin(stream)
}

#[cfg(test)]
mod tests;
