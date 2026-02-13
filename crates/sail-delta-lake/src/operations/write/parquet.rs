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

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use tokio::io::AsyncWrite;

use crate::kernel::DeltaTableError;

#[derive(Clone, Debug)]
pub struct SharedBytesCounter(Arc<AtomicU64>);

impl SharedBytesCounter {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn bytes(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// An `AsyncWrite` wrapper that counts bytes written.
pub struct CountingAsyncWrite<W> {
    inner: W,
    counter: SharedBytesCounter,
}

impl<W> CountingAsyncWrite<W> {
    pub fn new(inner: W) -> (Self, SharedBytesCounter) {
        let counter = SharedBytesCounter::new();
        (
            Self {
                inner,
                counter: counter.clone(),
            },
            counter,
        )
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for CountingAsyncWrite<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let p = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &p {
            self.counter.0.fetch_add(*n as u64, Ordering::Relaxed);
        }
        p
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A single parquet file writer that streams directly to `ObjectStore` via `BufWriter`.
pub struct DeltaParquetFileWriter {
    writer: Option<AsyncArrowWriter<CountingAsyncWrite<BufWriter>>>,
    bytes_counter: SharedBytesCounter,
    schema: ArrowSchemaRef,
}

impl DeltaParquetFileWriter {
    pub fn try_new(
        object_store: Arc<dyn ObjectStore>,
        location: &Path,
        schema: ArrowSchemaRef,
        writer_properties: WriterProperties,
        objectstore_writer_buffer_size: usize,
        skip_arrow_metadata: bool,
    ) -> Result<Self, DeltaTableError> {
        let buf_writer = BufWriter::with_capacity(
            object_store,
            location.clone(),
            objectstore_writer_buffer_size,
        );
        let (counting, bytes_counter) = CountingAsyncWrite::new(buf_writer);
        let options = ArrowWriterOptions::new()
            .with_properties(writer_properties)
            .with_skip_arrow_metadata(skip_arrow_metadata);

        let writer = AsyncArrowWriter::try_new_with_options(counting, schema.clone(), options)
            .map_err(|e| {
                DeltaTableError::generic(format!("Failed to create parquet writer: {e}"))
            })?;

        Ok(Self {
            writer: Some(writer),
            bytes_counter,
            schema,
        })
    }

    pub fn schema(&self) -> &ArrowSchemaRef {
        &self.schema
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_counter.bytes()
    }

    pub fn bytes_counter(&self) -> SharedBytesCounter {
        self.bytes_counter.clone()
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), DeltaTableError> {
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| DeltaTableError::generic("Parquet writer not available".to_string()))?;

        w.write(batch)
            .await
            .map_err(|e| DeltaTableError::generic(format!("Failed to write RecordBatch: {e}")))?;
        Ok(())
    }

    pub fn in_progress_size(&self) -> u64 {
        self.writer
            .as_ref()
            .map(|w| w.in_progress_size() as u64)
            .unwrap_or_default()
    }

    pub async fn close(mut self) -> Result<ParquetMetaData, DeltaTableError> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| DeltaTableError::generic("Parquet writer not available".to_string()))?;

        writer
            .close()
            .await
            .map_err(|e| DeltaTableError::generic(format!("Failed to close parquet writer: {e}")))
    }
}
