use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;

pub struct ParquetFileMeta {
    pub num_rows: u64,
    pub file_size: u64,
}

pub struct ArrowParquetWriter {
    writer: Option<AsyncArrowWriter<Vec<u8>>>,
}

impl ArrowParquetWriter {
    pub fn try_new(
        schema: &datafusion::arrow::datatypes::Schema,
        props: WriterProperties,
    ) -> Result<Self, String> {
        let buffer = Vec::new();
        let writer = AsyncArrowWriter::try_new(buffer, Arc::new(schema.clone()), Some(props))
            .map_err(|e| format!("parquet writer error: {e}"))?;
        Ok(Self {
            writer: Some(writer),
        })
    }

    pub async fn write_batch(
        &mut self,
        batch: &datafusion::arrow::array::RecordBatch,
    ) -> Result<(), String> {
        let writer = self.writer.as_mut().ok_or("writer closed")?;
        writer
            .write(batch)
            .await
            .map_err(|e| format!("parquet write: {e}"))
    }

    pub async fn close(mut self) -> Result<(Bytes, ParquetFileMeta), String> {
        let mut writer = self.writer.take().ok_or("writer already closed")?;
        let metadata = writer
            .finish()
            .await
            .map_err(|e| format!("parquet finish: {e}"))?;
        let buf = writer.into_inner();
        let file_size = buf.len() as u64;
        let bytes = Bytes::from(buf);
        let num_rows = metadata.num_rows as u64;
        Ok((
            bytes,
            ParquetFileMeta {
                num_rows,
                file_size,
            },
        ))
    }
}
