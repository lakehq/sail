// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use object_store::path::Path;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutPayloadMut, UploadPart,
};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;

/// Facts about one successfully finalized Parquet object.
pub struct WrittenParquetFile {
    pub path: Path,
    pub file_size: u64,
    pub row_count: u64,
    pub parquet_metadata: ParquetMetaData,
}

/// A single streaming Parquet object writer.
///
/// Table-format-specific routing, schemas, paths, and commit actions stay in
/// the caller. This type owns Parquet encoding, object-store buffering, exact
/// file accounting, and DataFusion memory-pool accounting.
pub struct ParquetFileWriter {
    store: Arc<dyn ObjectStore>,
    path: Path,
    schema: SchemaRef,
    writer: Option<ObjectParquetWriter>,
    reservation: MemoryReservation,
    row_count: u64,
}

impl ParquetFileWriter {
    pub fn try_new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        schema: SchemaRef,
        properties: WriterProperties,
        object_store_buffer_size: usize,
        memory_pool: &Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        Self::try_new_with_options(
            store,
            path,
            schema,
            properties,
            false,
            object_store_buffer_size,
            memory_pool,
        )
    }

    /// Creates a writer while preserving the caller's Arrow metadata setting.
    pub fn try_new_with_options(
        store: Arc<dyn ObjectStore>,
        path: Path,
        schema: SchemaRef,
        properties: WriterProperties,
        skip_arrow_metadata: bool,
        object_store_buffer_size: usize,
        memory_pool: &Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let object_writer = AbortableObjectWriter::new(
            Arc::clone(&store),
            path.clone(),
            object_store_buffer_size.max(1),
        );
        let options = ArrowWriterOptions::new()
            .with_properties(properties)
            .with_skip_arrow_metadata(skip_arrow_metadata);
        let writer =
            AsyncArrowWriter::try_new_with_options(object_writer, Arc::clone(&schema), options)
                .map_err(|error| DataFusionError::ParquetError(Box::new(error)))?;
        let reservation =
            MemoryConsumer::new(format!("ParquetFileWriter[{path}]")).register(memory_pool);
        Ok(Self {
            store,
            path,
            schema,
            writer: Some(writer),
            reservation,
            row_count: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Serialized bytes plus the anticipated size of the in-progress row group.
    pub fn estimated_file_size(&self) -> u64 {
        self.writer
            .as_ref()
            .map(|writer| {
                u64::try_from(
                    writer
                        .bytes_written()
                        .saturating_add(writer.in_progress_size()),
                )
                .unwrap_or(u64::MAX)
            })
            .unwrap_or(0)
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let next_row_count = self
            .row_count
            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                DataFusionError::Execution("Parquet row count is too large".to_string())
            })?)
            .ok_or_else(|| DataFusionError::Execution("Parquet row count overflow".to_string()))?;
        let writer = self.writer.as_mut().ok_or_else(|| {
            DataFusionError::Execution("Parquet file writer is already closed".to_string())
        })?;
        if let Err(error) = writer.write(batch).await {
            self.abort_writer().await;
            return Err(DataFusionError::ParquetError(Box::new(error)));
        }
        if let Err(error) = self.reservation.try_resize(writer.memory_size()) {
            self.abort_writer().await;
            return Err(error);
        }
        self.row_count = next_row_count;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<WrittenParquetFile> {
        let writer = self.writer.take().ok_or_else(|| {
            DataFusionError::Execution("Parquet file writer is already closed".to_string())
        })?;
        let mut writer = AbortOnDrop::new(writer);
        let mut published = PublishedObjectGuard::new(Arc::clone(&self.store), self.path.clone());
        let parquet_metadata = writer
            .writer_mut()?
            .finish()
            .await
            .map_err(|error| DataFusionError::ParquetError(Box::new(error)))?;
        let file_size = u64::try_from(writer.writer()?.bytes_written()).map_err(|_| {
            DataFusionError::Execution("Parquet file size is too large".to_string())
        })?;
        writer.disarm();
        let metadata_row_count = u64::try_from(parquet_metadata.file_metadata().num_rows())
            .map_err(|_| {
                DataFusionError::Execution("Parquet metadata row count is negative".to_string())
            })?;
        if metadata_row_count != self.row_count {
            return Err(DataFusionError::Execution(format!(
                "Parquet row count mismatch: wrote {}, footer reports {metadata_row_count}",
                self.row_count
            )));
        }
        published.disarm();
        Ok(WrittenParquetFile {
            path: self.path.clone(),
            file_size,
            row_count: self.row_count,
            parquet_metadata,
        })
    }

    pub async fn abort(mut self) {
        self.abort_writer().await;
    }

    async fn abort_writer(&mut self) {
        let Some(writer) = self.writer.take() else {
            return;
        };
        abort_writer(writer).await;
    }
}

impl Drop for ParquetFileWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            spawn_abort(writer);
        }
    }
}

#[derive(Debug, Default)]
struct PayloadBuffer {
    chunks: VecDeque<Bytes>,
    len: usize,
}

impl PayloadBuffer {
    fn push(&mut self, bytes: Bytes) {
        self.len = self.len.saturating_add(bytes.len());
        self.chunks.push_back(bytes);
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn take(&mut self, length: usize) -> PutPayload {
        let mut remaining = length.min(self.len);
        let mut payload = PutPayloadMut::new();
        while remaining > 0 {
            let Some(mut chunk) = self.chunks.pop_front() else {
                break;
            };
            if chunk.len() <= remaining {
                remaining -= chunk.len();
                payload.push(chunk);
            } else {
                let head = chunk.split_to(remaining);
                payload.push(head);
                self.chunks.push_front(chunk);
                remaining = 0;
            }
        }
        let consumed = length.min(self.len) - remaining;
        self.len -= consumed;
        payload.into()
    }

    fn take_all(&mut self) -> PutPayload {
        let length = self.len;
        self.take(length)
    }
}

#[derive(Debug)]
struct MultipartWriterState {
    upload: Option<Box<dyn MultipartUpload>>,
    parts: FuturesUnordered<UploadPart>,
}

#[derive(Debug)]
enum ObjectWriterState {
    Buffered,
    Multipart(MultipartWriterState),
    Complete,
}

#[derive(Debug)]
struct AbortableObjectWriter {
    store: Arc<dyn ObjectStore>,
    path: Path,
    part_size: usize,
    max_concurrency: usize,
    buffer: PayloadBuffer,
    state: ObjectWriterState,
}

impl AbortableObjectWriter {
    fn new(store: Arc<dyn ObjectStore>, path: Path, part_size: usize) -> Self {
        Self {
            store,
            path,
            part_size: part_size.max(1),
            max_concurrency: 8,
            buffer: PayloadBuffer::default(),
            state: ObjectWriterState::Buffered,
        }
    }

    async fn write_bytes(&mut self, bytes: Bytes) -> object_store::Result<()> {
        if matches!(&self.state, ObjectWriterState::Complete) {
            return Err(object_store::Error::Generic {
                store: "ParquetFileWriter",
                source: Box::new(std::io::Error::other("object writer is already complete")),
            });
        }
        self.buffer.push(bytes);
        if matches!(&self.state, ObjectWriterState::Buffered) && self.buffer.len() >= self.part_size
        {
            let upload = self
                .store
                .put_multipart_opts(&self.path, PutMultipartOptions::default())
                .await?;
            self.state = ObjectWriterState::Multipart(MultipartWriterState {
                upload: Some(upload),
                parts: FuturesUnordered::new(),
            });
        }
        while matches!(&self.state, ObjectWriterState::Multipart(_))
            && self.buffer.len() >= self.part_size
        {
            self.wait_for_part_capacity().await?;
            let payload = self.buffer.take(self.part_size);
            self.start_part(payload)?;
        }
        Ok(())
    }

    fn start_part(&mut self, payload: PutPayload) -> object_store::Result<()> {
        let ObjectWriterState::Multipart(state) = &mut self.state else {
            return Err(object_store::Error::Generic {
                store: "ParquetFileWriter",
                source: Box::new(std::io::Error::other("multipart upload is not active")),
            });
        };
        let upload = state
            .upload
            .as_mut()
            .ok_or_else(|| object_store::Error::Generic {
                store: "ParquetFileWriter",
                source: Box::new(std::io::Error::other("multipart upload handle is missing")),
            })?;
        state.parts.push(upload.put_part(payload));
        Ok(())
    }

    async fn wait_for_part_capacity(&mut self) -> object_store::Result<()> {
        loop {
            let should_wait = matches!(
                &self.state,
                ObjectWriterState::Multipart(state) if state.parts.len() >= self.max_concurrency
            );
            if !should_wait {
                return Ok(());
            }
            self.wait_for_part().await?;
        }
    }

    async fn wait_for_part(&mut self) -> object_store::Result<()> {
        let result = match &mut self.state {
            ObjectWriterState::Multipart(state) => state.parts.next().await,
            ObjectWriterState::Buffered | ObjectWriterState::Complete => None,
        };
        match result {
            Some(Ok(())) | None => Ok(()),
            Some(Err(error)) => {
                let _ = self.abort().await;
                Err(error)
            }
        }
    }

    async fn complete(&mut self) -> object_store::Result<()> {
        if matches!(&self.state, ObjectWriterState::Buffered) {
            let payload = self.buffer.take_all();
            self.state = ObjectWriterState::Complete;
            self.store
                .put_opts(&self.path, payload, PutOptions::default())
                .await?;
            return Ok(());
        }
        if matches!(&self.state, ObjectWriterState::Complete) {
            return Err(object_store::Error::Generic {
                store: "ParquetFileWriter",
                source: Box::new(std::io::Error::other("object writer is already complete")),
            });
        }

        if !self.buffer.is_empty() {
            self.wait_for_part_capacity().await?;
            let payload = self.buffer.take_all();
            self.start_part(payload)?;
        }
        loop {
            let has_parts = matches!(
                &self.state,
                ObjectWriterState::Multipart(state) if !state.parts.is_empty()
            );
            if !has_parts {
                break;
            }
            self.wait_for_part().await?;
        }
        let complete_result = match &mut self.state {
            ObjectWriterState::Multipart(state) => {
                let upload = state
                    .upload
                    .as_mut()
                    .ok_or_else(|| object_store::Error::Generic {
                        store: "ParquetFileWriter",
                        source: Box::new(std::io::Error::other(
                            "multipart upload handle is missing",
                        )),
                    })?;
                upload.complete().await
            }
            ObjectWriterState::Buffered | ObjectWriterState::Complete => unreachable!(),
        };
        match complete_result {
            Ok(_) => {
                self.state = ObjectWriterState::Complete;
                Ok(())
            }
            Err(error) => {
                let _ = self.abort().await;
                Err(error)
            }
        }
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let state = std::mem::replace(&mut self.state, ObjectWriterState::Complete);
        match state {
            ObjectWriterState::Multipart(mut state) => {
                state.parts.clear();
                if let Some(mut upload) = state.upload.take() {
                    upload.abort().await
                } else {
                    Ok(())
                }
            }
            ObjectWriterState::Buffered | ObjectWriterState::Complete => Ok(()),
        }
    }
}

impl AsyncFileWriter for AbortableObjectWriter {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, ParquetResult<()>> {
        Box::pin(async move {
            self.write_bytes(bytes)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, ParquetResult<()>> {
        Box::pin(async move {
            self.complete()
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        })
    }
}

type ObjectParquetWriter = AsyncArrowWriter<AbortableObjectWriter>;

struct AbortOnDrop {
    writer: Option<ObjectParquetWriter>,
}

impl AbortOnDrop {
    fn new(writer: ObjectParquetWriter) -> Self {
        Self {
            writer: Some(writer),
        }
    }

    fn writer(&self) -> Result<&ObjectParquetWriter> {
        self.writer
            .as_ref()
            .ok_or_else(|| DataFusionError::Execution("Parquet writer guard is empty".to_string()))
    }

    fn writer_mut(&mut self) -> Result<&mut ObjectParquetWriter> {
        self.writer
            .as_mut()
            .ok_or_else(|| DataFusionError::Execution("Parquet writer guard is empty".to_string()))
    }

    fn disarm(&mut self) {
        self.writer.take();
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            spawn_abort(writer);
        }
    }
}

struct PublishedObjectGuard {
    store: Arc<dyn ObjectStore>,
    path: Path,
    armed: bool,
}

impl PublishedObjectGuard {
    fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PublishedObjectGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = store.delete(&path).await;
            });
        }
    }
}

async fn abort_writer(writer: ObjectParquetWriter) {
    let mut object_writer = writer.into_inner();
    let _ = object_writer.abort().await;
}

fn spawn_abort(writer: ObjectParquetWriter) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(abort_writer(writer));
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion_common::{DataFusionError, Result};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult, UploadPart,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use parquet::file::metadata::ParquetMetaData;
    use parquet::file::properties::WriterProperties;
    use tokio::sync::Notify;

    use super::ParquetFileWriter;

    fn batch() -> Result<RecordBatch> {
        let values = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        Ok(RecordBatch::try_from_iter([("value", values)])?)
    }

    fn multipart_batch() -> Result<RecordBatch> {
        let values = Arc::new(Int64Array::from_iter_values(0_i64..1024)) as ArrayRef;
        Ok(RecordBatch::try_from_iter([("value", values)])?)
    }

    #[derive(Debug, Default)]
    struct MultipartFailureState {
        upload_active: AtomicBool,
        part_calls: AtomicUsize,
        complete_calls: AtomicUsize,
        abort_calls: AtomicUsize,
        complete_started: Notify,
    }

    #[derive(Debug, Clone, Copy)]
    enum MultipartFailureMode {
        Part,
        PendingComplete,
    }

    #[derive(Debug)]
    struct PartFailureStore {
        objects: InMemory,
        state: Arc<MultipartFailureState>,
        mode: MultipartFailureMode,
    }

    impl PartFailureStore {
        fn new(state: Arc<MultipartFailureState>, mode: MultipartFailureMode) -> Self {
            Self {
                objects: InMemory::new(),
                state,
                mode,
            }
        }
    }

    impl fmt::Display for PartFailureStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("PartFailureStore")
        }
    }

    #[async_trait]
    impl ObjectStore for PartFailureStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.objects.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.state.upload_active.store(true, Ordering::SeqCst);
            Ok(Box::new(PartFailureUpload {
                state: Arc::clone(&self.state),
                mode: self.mode,
            }))
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.objects.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.objects.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.objects.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.objects.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.objects.copy_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    struct PartFailureUpload {
        state: Arc<MultipartFailureState>,
        mode: MultipartFailureMode,
    }

    #[async_trait]
    impl MultipartUpload for PartFailureUpload {
        fn put_part(&mut self, _payload: PutPayload) -> UploadPart {
            let part_index = self.state.part_calls.fetch_add(1, Ordering::SeqCst);
            let mode = self.mode;
            Box::pin(async move {
                if matches!(mode, MultipartFailureMode::Part) && part_index == 0 {
                    Err(object_store::Error::Generic {
                        store: "PartFailureStore",
                        source: Box::new(std::io::Error::other("injected multipart part failure")),
                    })
                } else {
                    Ok(())
                }
            })
        }

        async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
            self.state.complete_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, MultipartFailureMode::PendingComplete) {
                self.state.complete_started.notify_one();
                return futures::future::pending().await;
            }
            Err(object_store::Error::Generic {
                store: "PartFailureStore",
                source: Box::new(std::io::Error::other(
                    "complete must not follow a failed part",
                )),
            })
        }

        async fn abort(&mut self) -> ObjectStoreResult<()> {
            self.state.abort_calls.fetch_add(1, Ordering::SeqCst);
            self.state.upload_active.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    fn has_arrow_schema(metadata: &ParquetMetaData) -> bool {
        metadata
            .file_metadata()
            .key_value_metadata()
            .is_some_and(|items| items.iter().any(|item| item.key == "ARROW:schema"))
    }

    #[tokio::test]
    async fn writes_exact_file_facts_and_readable_object() -> Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = Path::from("data/file.parquet");
        let batch = batch()?;
        let context = TaskContext::default();
        let mut writer = ParquetFileWriter::try_new(
            Arc::clone(&store),
            path.clone(),
            batch.schema(),
            WriterProperties::default(),
            64,
            context.memory_pool(),
        )?;
        writer.write(&batch).await?;
        assert!(writer.estimated_file_size() > 0);

        let written = writer.finish().await?;
        let object = store.head(&path).await?;
        assert_eq!(written.path, path);
        assert_eq!(written.file_size, object.size);
        assert_eq!(written.row_count, 3);
        assert_eq!(written.parquet_metadata.file_metadata().num_rows(), 3);
        assert!(has_arrow_schema(&written.parquet_metadata));

        let bytes = store.get(&written.path).await?.bytes().await?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let actual = reader.next().transpose()?.ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "expected one Parquet record batch".to_string(),
            )
        })?;
        assert_eq!(actual, batch);
        Ok(())
    }

    #[tokio::test]
    async fn abort_does_not_publish_an_object() -> Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = Path::from("data/aborted.parquet");
        let batch = batch()?;
        let context = TaskContext::default();
        let mut writer = ParquetFileWriter::try_new(
            Arc::clone(&store),
            path.clone(),
            batch.schema(),
            WriterProperties::default(),
            1,
            context.memory_pool(),
        )?;
        writer.write(&batch).await?;
        writer.abort().await;
        assert!(store.head(&path).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn finish_aborts_multipart_upload_after_part_failure() -> Result<()> {
        let state = Arc::new(MultipartFailureState::default());
        let store = Arc::new(PartFailureStore::new(
            Arc::clone(&state),
            MultipartFailureMode::Part,
        ));
        let object_store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let path = Path::from("data/failed-finalization.parquet");
        let batch = multipart_batch()?;
        let context = TaskContext::default();
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        let mut writer = ParquetFileWriter::try_new(
            object_store,
            path.clone(),
            batch.schema(),
            properties,
            4096,
            context.memory_pool(),
        )?;
        writer.write(&batch).await?;

        let finish_result = writer.finish().await;
        assert!(
            finish_result.is_err(),
            "injected part failure must fail finish"
        );
        assert!(state.part_calls.load(Ordering::SeqCst) > 0);
        assert_eq!(state.complete_calls.load(Ordering::SeqCst), 0);
        assert!(store.head(&path).await.is_err());
        assert_eq!(
            state.abort_calls.load(Ordering::SeqCst),
            1,
            "failed finish must abort the multipart upload before returning"
        );
        assert!(
            !state.upload_active.load(Ordering::SeqCst),
            "failed finish must not leave multipart state active"
        );
        Ok(())
    }

    #[tokio::test]
    async fn canceled_finish_aborts_multipart_upload() -> Result<()> {
        let state = Arc::new(MultipartFailureState::default());
        let store = Arc::new(PartFailureStore::new(
            Arc::clone(&state),
            MultipartFailureMode::PendingComplete,
        ));
        let object_store = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let path = Path::from("data/canceled-finalization.parquet");
        let batch = multipart_batch()?;
        let context = TaskContext::default();
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        let mut writer = ParquetFileWriter::try_new(
            object_store,
            path.clone(),
            batch.schema(),
            properties,
            4096,
            context.memory_pool(),
        )?;
        writer.write(&batch).await?;

        let mut finish = Box::pin(writer.finish());
        tokio::select! {
            () = state.complete_started.notified() => {}
            _ = &mut finish => {
                return Err(DataFusionError::Execution(
                    "multipart completion unexpectedly finished".to_string()
                ));
            }
        }
        drop(finish);

        let aborted = tokio::time::timeout(Duration::from_secs(1), async {
            while state.abort_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            aborted.is_ok(),
            "canceled finish did not abort its multipart upload"
        );
        assert_eq!(state.abort_calls.load(Ordering::SeqCst), 1);
        assert!(!state.upload_active.load(Ordering::SeqCst));
        assert!(store.head(&path).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn honors_skip_arrow_metadata() -> Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = Path::from("data/without-arrow-schema.parquet");
        let batch = batch()?;
        let context = TaskContext::default();
        let mut writer = ParquetFileWriter::try_new_with_options(
            Arc::clone(&store),
            path,
            batch.schema(),
            WriterProperties::default(),
            true,
            64,
            context.memory_pool(),
        )?;
        writer.write(&batch).await?;

        let written = writer.finish().await?;
        assert!(!has_arrow_schema(&written.parquet_metadata));
        Ok(())
    }
}
