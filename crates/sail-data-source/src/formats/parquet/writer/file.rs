// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion_common::Result;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;

use crate::listing::commit::WrittenFile;

/// The maximum number of concurrent part uploads for one file.
/// Many files can be written at the same time, so this is lower than
/// the default of the object store writer.
const MAX_UPLOAD_CONCURRENCY: usize = 4;

/// A Parquet file that is being written to an object store.
///
/// The file becomes visible in the object store only when [`Self::close`] succeeds.
/// Dropping the writer before that abandons the upload.
pub struct ParquetFileWriter {
    inner: FileEncoder,
    location: Path,
    rows: u64,
}

enum FileEncoder {
    Arrow(AsyncArrowWriter<BufWriter>),
}

impl ParquetFileWriter {
    pub fn try_new(
        store: Arc<dyn ObjectStore>,
        location: Path,
        schema: SchemaRef,
        props: WriterProperties,
        skip_arrow_metadata: bool,
        buffer_size: usize,
    ) -> Result<Self> {
        let writer = BufWriter::with_capacity(store, location.clone(), buffer_size)
            .with_max_concurrency(MAX_UPLOAD_CONCURRENCY);
        let options = ArrowWriterOptions::new()
            .with_properties(props)
            .with_skip_arrow_metadata(skip_arrow_metadata);
        let writer = AsyncArrowWriter::try_new_with_options(writer, schema, options)?;
        Ok(Self {
            inner: FileEncoder::Arrow(writer),
            location,
            rows: 0,
        })
    }

    /// Writes a batch to the file.
    /// Returns whether a row group was flushed during the call.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<bool> {
        let flushed = match &mut self.inner {
            FileEncoder::Arrow(writer) => {
                let row_groups = writer.flushed_row_groups().len();
                writer.write(batch).await?;
                writer.flushed_row_groups().len() > row_groups
            }
        };
        self.rows += batch.num_rows() as u64;
        Ok(flushed)
    }

    /// The number of rows written to the file so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The memory used by the data that has not been flushed as a row group.
    pub fn memory_size(&self) -> usize {
        match &self.inner {
            FileEncoder::Arrow(writer) => writer.memory_size(),
        }
    }

    /// Writes the remaining data and the footer, and completes the upload.
    pub async fn close(self) -> Result<WrittenFile> {
        let size = match self.inner {
            FileEncoder::Arrow(mut writer) => {
                writer.finish().await?;
                writer.bytes_written()
            }
        };
        Ok(WrittenFile {
            location: self.location,
            size: size as u64,
            num_rows: self.rows,
        })
    }
}
