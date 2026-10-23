use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::async_writer::{AsyncArrowWriter, AsyncFileWriter};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ParquetMetaData;

/// Metadata is returned only after the file footer and upload have completed.
#[derive(Debug)]
pub struct ParquetFileMetadata {
    pub num_rows: u64,
    pub file_size: u64,
    pub parquet_metadata: ParquetMetaData,
}

pub struct ParquetWriter<W: AsyncFileWriter> {
    writer: AsyncArrowWriter<W>,
}

impl<W: AsyncFileWriter> ParquetWriter<W> {
    pub fn try_new(output: W, schema: SchemaRef, options: ArrowWriterOptions) -> Result<Self> {
        Ok(Self {
            writer: AsyncArrowWriter::try_new_with_options(output, schema, options)?,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).await
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await
    }

    /// Memory held by the Parquet encoder, excluding the output's own buffers.
    pub fn memory_size(&self) -> usize {
        self.writer.memory_size()
    }

    pub fn estimated_size(&self) -> u64 {
        self.writer
            .bytes_written()
            .saturating_add(self.writer.in_progress_size()) as u64
    }

    pub async fn finish(mut self) -> Result<(W, ParquetFileMetadata)> {
        let parquet_metadata = self.writer.finish().await?;
        let metadata = ParquetFileMetadata {
            num_rows: parquet_metadata.file_metadata().num_rows() as u64,
            file_size: self.writer.bytes_written() as u64,
            parquet_metadata,
        };
        Ok((self.writer.into_inner(), metadata))
    }
}

/// Bounded object-store buffering with one upload part in flight.
///
/// The upload remains owned here during writes and completion, so dropping a
/// failed or cancelled writer can abort it even if completion was interrupted.
pub struct ParquetObjectWriter {
    store: Arc<dyn ObjectStore>,
    location: Path,
    capacity: usize,
    buffer: Vec<u8>,
    upload: Option<Box<dyn MultipartUpload>>,
    completed: bool,
}

impl ParquetObjectWriter {
    pub fn new(store: Arc<dyn ObjectStore>, location: Path, capacity: usize) -> Self {
        Self {
            store,
            location,
            capacity: capacity.max(1),
            buffer: Vec::new(),
            upload: None,
            completed: false,
        }
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.upload.is_none() {
            self.upload = Some(
                self.store
                    .put_multipart(&self.location)
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e)))?,
            );
        }
        if let Some(upload) = &mut self.upload {
            upload
                .put_part(std::mem::take(&mut self.buffer).into())
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
        }
        Ok(())
    }
}

impl AsyncFileWriter for ParquetObjectWriter {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if self.completed {
                return Err(ParquetError::General("Parquet upload is closed".into()));
            }
            let mut remaining = bytes.as_ref();
            while !remaining.is_empty() {
                let len = remaining.len().min(self.capacity - self.buffer.len());
                // Own the buffered bytes: a slice of the input would retain the
                // entire encoded row group even for a tiny unfinished part.
                if self.buffer.len() + len > self.buffer.capacity() {
                    let capacity = self.capacity.min(
                        self.buffer
                            .capacity()
                            .saturating_mul(2)
                            .max(self.buffer.len() + len),
                    );
                    self.buffer.reserve_exact(capacity - self.buffer.len());
                }
                self.buffer.extend_from_slice(&remaining[..len]);
                remaining = &remaining[len..];
                if self.buffer.len() == self.capacity {
                    self.flush_buffer().await?;
                }
            }
            Ok(())
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if self.completed {
                return Err(ParquetError::General("Parquet upload is closed".into()));
            }
            if self.upload.is_some() {
                if !self.buffer.is_empty() {
                    self.flush_buffer().await?;
                }
                if let Some(upload) = &mut self.upload {
                    upload
                        .complete()
                        .await
                        .map_err(|e| ParquetError::External(Box::new(e)))?;
                }
                self.upload = None;
            } else {
                self.store
                    .put(&self.location, std::mem::take(&mut self.buffer).into())
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
            }
            self.completed = true;
            Ok(())
        })
    }
}

impl Drop for ParquetObjectWriter {
    fn drop(&mut self) {
        if let Some(mut upload) = self.upload.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                if let Err(error) = upload.abort().await {
                    log::warn!("Failed to abort Parquet upload: {error}");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::FutureExt;
    use object_store::memory::InMemory;
    use object_store::{PutPayload, PutResult, UploadPart};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;

    use super::*;

    #[tokio::test]
    async fn metadata_and_field_ids_survive_buffered_and_multipart_writes()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false)
                .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())].into()),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..11))],
        )?;
        for capacity in [32, 1024 * 1024] {
            let store = Arc::new(InMemory::new());
            let path = Path::from("data.parquet");
            let output = ParquetObjectWriter::new(store.clone(), path.clone(), capacity);
            let mut writer = ParquetWriter::try_new(
                output,
                schema.clone(),
                ArrowWriterOptions::new().with_properties(
                    WriterProperties::builder()
                        .set_max_row_group_row_count(Some(4))
                        .build(),
                ),
            )?;
            writer.write(&batch).await?;
            let (_, metadata) = writer.finish().await?;
            let bytes = store.get(&path).await?.bytes().await?;
            assert_eq!(metadata.file_size, bytes.len() as u64);
            assert_eq!(metadata.num_rows, 11);
            assert_eq!(metadata.parquet_metadata.num_row_groups(), 3);
            assert_eq!(
                metadata
                    .parquet_metadata
                    .file_metadata()
                    .schema_descr()
                    .column(0)
                    .self_type()
                    .get_basic_info()
                    .id(),
                7
            );
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
            let mut offset = 0;
            for actual in reader {
                let actual = actual?;
                assert_eq!(actual, batch.slice(offset, actual.num_rows()));
                offset += actual.num_rows();
            }
            assert_eq!(offset, batch.num_rows());
        }
        Ok(())
    }

    #[derive(Debug)]
    struct TrackedBytes {
        data: Vec<u8>,
        released: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for TrackedBytes {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for TrackedBytes {
        fn drop(&mut self) {
            self.released.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn buffered_tail_does_not_retain_the_encoded_row_group()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let released = Arc::new(AtomicBool::new(false));
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        let capacity = 64;
        let mut output = ParquetObjectWriter::new(store.clone(), path.clone(), capacity);
        let bytes = Bytes::from_owner(TrackedBytes {
            data: vec![7; 4097],
            released: released.clone(),
        });
        output.write(bytes).await?;
        assert!(released.load(Ordering::SeqCst));
        assert_eq!(output.buffer.len(), 1);
        assert!(output.buffer.capacity() <= capacity);
        for value in 0..63 {
            output.write(Bytes::copy_from_slice(&[value])).await?;
            assert!(output.buffer.capacity() <= capacity);
        }
        output.complete().await?;
        let mut expected = vec![7; 4097];
        expected.extend(0..63);
        assert_eq!(store.get(&path).await?.bytes().await?.as_ref(), expected);
        Ok(())
    }

    #[derive(Debug)]
    struct InterruptedUpload {
        aborted: Arc<AtomicBool>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for InterruptedUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(async { Ok(()) })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            if self.fail {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "injected completion failure".into(),
                });
            }
            futures::future::pending().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborted.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn aborts_upload_after_failed_or_cancelled_completion()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        for fail in [false, true] {
            let aborted = Arc::new(AtomicBool::new(false));
            let mut output =
                ParquetObjectWriter::new(Arc::new(InMemory::new()), Path::from("data.parquet"), 32);
            output.upload = Some(Box::new(InterruptedUpload {
                aborted: aborted.clone(),
                fail,
            }));
            if fail {
                assert!(output.complete().await.is_err());
            } else {
                assert!(output.complete().now_or_never().is_none());
            }
            drop(output);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !aborted.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
        }
        Ok(())
    }
}
