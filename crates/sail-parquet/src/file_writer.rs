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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_common::{DataFusionError, Result};
use futures::FutureExt;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::async_writer::ParquetObjectWriter;
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
    writer: Option<AsyncArrowWriter<ParquetObjectWriter>>,
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
        let object_writer = ParquetObjectWriter::from_buf_writer(BufWriter::with_capacity(
            Arc::clone(&store),
            path.clone(),
            object_store_buffer_size.max(1),
        ));
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
        let parquet_metadata = writer
            .writer_mut()?
            .finish()
            .await
            .map_err(|error| DataFusionError::ParquetError(Box::new(error)))?;
        let mut published = PublishedObjectGuard::new(Arc::clone(&self.store), self.path.clone());
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

type ObjectParquetWriter = AsyncArrowWriter<ParquetObjectWriter>;

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
    let mut buffer = writer.into_inner().into_inner();
    let _ = AssertUnwindSafe(buffer.abort()).catch_unwind().await;
}

fn spawn_abort(writer: ObjectParquetWriter) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(abort_writer(writer));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion_common::Result;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::ParquetMetaData;
    use parquet::file::properties::WriterProperties;

    use super::ParquetFileWriter;

    fn batch() -> Result<RecordBatch> {
        let values = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        Ok(RecordBatch::try_from_iter([("value", values)])?)
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
