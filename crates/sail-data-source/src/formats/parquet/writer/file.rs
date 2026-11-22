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

use crate::formats::parquet::writer::encoder::ColumnEncoder;
use crate::formats::parquet::writer::resources::EncodeResources;
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
    resources: Arc<EncodeResources>,
}

enum FileEncoder {
    /// Encodes the file with content-defined chunking.
    Arrow(Box<AsyncArrowWriter<BufWriter>>),
    /// Encodes the columns of the file in the writer task or in other tasks.
    Columns(Box<ColumnEncoder>),
}

impl ParquetFileWriter {
    pub fn try_new(
        store: Arc<dyn ObjectStore>,
        location: Path,
        schema: SchemaRef,
        props: WriterProperties,
        skip_arrow_metadata: bool,
        buffer_size: usize,
        resources: Arc<EncodeResources>,
    ) -> Result<Self> {
        let writer = BufWriter::with_capacity(store, location.clone(), buffer_size)
            .with_max_concurrency(MAX_UPLOAD_CONCURRENCY);
        // Content-defined chunking keeps state across the row groups of a column,
        // which is only available in the Arrow writer.
        let inner = if props.content_defined_chunking().is_none() {
            FileEncoder::Columns(Box::new(ColumnEncoder::try_new(
                writer,
                buffer_size,
                schema,
                props,
                skip_arrow_metadata,
                Arc::clone(&resources),
            )?))
        } else {
            let options = ArrowWriterOptions::new()
                .with_properties(props)
                .with_skip_arrow_metadata(skip_arrow_metadata);
            FileEncoder::Arrow(Box::new(AsyncArrowWriter::try_new_with_options(
                writer, schema, options,
            )?))
        };
        Ok(Self {
            inner,
            location,
            rows: 0,
            resources,
        })
    }

    /// Writes a batch to the file.
    /// Returns whether a row group received its last row during the call.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<bool> {
        let flushed = match &mut self.inner {
            FileEncoder::Arrow(writer) => {
                let _timer = self.resources.elapsed_compute().timer();
                let row_groups = writer.flushed_row_groups().len();
                writer.write(batch).await?;
                writer.flushed_row_groups().len() > row_groups
            }
            FileEncoder::Columns(writer) => {
                let row_groups = writer.row_groups();
                writer.write_batch(batch).await?;
                writer.row_groups() > row_groups
            }
        };
        self.rows += batch.num_rows() as u64;
        Ok(flushed)
    }

    /// The number of rows written to the file so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The number of row groups that are being encoded.
    #[cfg(test)]
    pub fn row_groups_in_flight(&self) -> usize {
        match &self.inner {
            FileEncoder::Arrow(writer) => usize::from(writer.in_progress_rows() > 0),
            FileEncoder::Columns(writer) => writer.row_groups_in_flight(),
        }
    }

    /// The memory used by the data that has not been handed to the upload.
    /// This does not include the input that waits to be encoded.
    pub fn memory_size(&self) -> usize {
        match &self.inner {
            FileEncoder::Arrow(writer) => writer.memory_size(),
            FileEncoder::Columns(writer) => writer.memory_size(),
        }
    }

    /// Writes the remaining data and the footer, and completes the upload.
    pub async fn close(self) -> Result<WrittenFile> {
        let size = match self.inner {
            FileEncoder::Arrow(mut writer) => {
                let _timer = self.resources.elapsed_compute().timer();
                writer.finish().await?;
                writer.bytes_written()
            }
            FileEncoder::Columns(mut writer) => writer.finish().await?,
        };
        Ok(WrittenFile {
            location: self.location,
            size: size as u64,
            num_rows: self.rows,
        })
    }
}
