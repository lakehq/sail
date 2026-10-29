use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::buffered::BufWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::ParquetError;
use tokio::io::AsyncWriteExt;

/// Streams encoded row groups and aborts uploads abandoned before completion.
pub(super) struct ParquetObjectSink {
    output: Option<BufWriter>,
}

impl ParquetObjectSink {
    pub(super) fn new(output: BufWriter) -> Self {
        Self {
            output: Some(output),
        }
    }
}

impl AsyncFileWriter for ParquetObjectSink {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            let output = self
                .output
                .as_mut()
                .ok_or_else(|| ParquetError::General("Delta Parquet sink is closed".to_string()))?;
            output
                .put(bytes)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            let mut output = self
                .output
                .take()
                .ok_or_else(|| ParquetError::General("Delta Parquet sink is closed".to_string()))?;
            output.shutdown().await?;
            Ok(())
        })
    }
}

impl Drop for ParquetObjectSink {
    fn drop(&mut self) {
        if let Some(mut output) = self.output.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                if let Err(error) = output.abort().await {
                    log::warn!("Failed to abort Delta Parquet upload: {error}");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;

    #[tokio::test]
    async fn buffered_and_multipart_uploads_preserve_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        for capacity in [4, 1024] {
            let path = Path::from(format!("file-{capacity}"));
            let mut sink = ParquetObjectSink::new(BufWriter::with_capacity(
                store.clone(),
                path.clone(),
                capacity,
            ));
            sink.write(Bytes::from_static(b"first")).await?;
            sink.write(Bytes::from_static(b"second")).await?;
            assert!(matches!(
                store.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ));
            sink.complete().await?;
            assert_eq!(store.get(&path).await?.bytes().await?, "firstsecond");
            assert!(sink.write(Bytes::from_static(b"closed")).await.is_err());
        }
        Ok(())
    }
}
