// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion_common::Result;
use object_store::path::Path;

use crate::formats::parquet::writer::encoder::ColumnEncoder;
use crate::formats::parquet::writer::resources::EncodeResources;
use crate::listing::staging::{StagedFile, StagedUpload};

/// The maximum number of concurrent part uploads for one file.
/// Many files can be written at the same time, so this is lower than
/// the default of the object store writer.
pub(super) const MAX_UPLOAD_CONCURRENCY: usize = 4;

/// A Parquet file that is being written to an object store.
///
/// The file is not visible in the object store when [`Self::close`] succeeds.
/// It becomes visible when the staged file that is returned is published.
/// Dropping the writer before that abandons the upload.
pub struct ParquetFileWriter {
    inner: FileEncoder,
    /// The size of the data that is handed to the upload and not stored.
    uploading: Arc<AtomicUsize>,
    location: Path,
    rows: u64,
    resources: Arc<EncodeResources>,
}

enum FileEncoder {
    /// Encodes the file with content-defined chunking.
    Arrow(Box<AsyncArrowWriter<StagedUpload>>),
    /// Encodes the columns of the file in the writer task or in other tasks.
    Columns(Box<ColumnEncoder>),
}

impl ParquetFileWriter {
    /// Creates the writer of the file at the location, with the upload
    /// that was started for it. The encoded data is handed to the upload
    /// in pieces of at most `buffer_size` bytes.
    pub fn try_new(
        writer: StagedUpload,
        location: Path,
        schema: SchemaRef,
        props: WriterProperties,
        skip_arrow_metadata: bool,
        buffer_size: usize,
        resources: Arc<EncodeResources>,
    ) -> Result<Self> {
        let uploading = writer.buffered();
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
            uploading,
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

    /// The size in bytes that the file is expected to have with the rows written
    /// to it so far. The size is not known before the file has a row group.
    ///
    /// The row groups that are being encoded are expected to have the same size
    /// for each row as the row groups that are in the file. The result depends
    /// on how many row groups have been encoded when this is called.
    pub fn expected_size(&self) -> Option<u64> {
        let (bytes, rows) = match &self.inner {
            FileEncoder::Arrow(writer) => (
                writer.bytes_written(),
                writer
                    .flushed_row_groups()
                    .iter()
                    .map(|x| x.num_rows())
                    .sum::<i64>(),
            ),
            FileEncoder::Columns(writer) => writer.written(),
        };
        let rows = u64::try_from(rows).ok().filter(|rows| *rows > 0)?;
        Some((bytes as u128 * self.rows as u128 / rows as u128).min(u64::MAX as u128) as u64)
    }

    /// The number of tasks that encode the row group that receives rows.
    #[cfg(test)]
    pub fn lanes(&self) -> usize {
        match &self.inner {
            FileEncoder::Arrow(_) => 0,
            FileEncoder::Columns(writer) => writer.lanes(),
        }
    }

    /// The number of row groups that are being encoded.
    #[cfg(test)]
    pub fn row_groups_in_flight(&self) -> usize {
        match &self.inner {
            FileEncoder::Arrow(writer) => usize::from(writer.in_progress_rows() > 0),
            FileEncoder::Columns(writer) => writer.row_groups_in_flight(),
        }
    }

    /// The memory used by the data that the object store has not stored.
    /// This does not include the input that waits to be encoded.
    pub fn memory_size(&self) -> usize {
        let encoding = match &self.inner {
            FileEncoder::Arrow(writer) => writer.memory_size(),
            FileEncoder::Columns(writer) => writer.memory_size(),
        };
        encoding + self.uploading.load(Ordering::Relaxed)
    }

    /// Writes the remaining data and the footer, and uploads what remains.
    pub async fn close(self) -> Result<StagedFile> {
        let (size, publish) = match self.inner {
            FileEncoder::Arrow(mut writer) => {
                let _timer = self.resources.elapsed_compute().timer();
                writer.finish().await?;
                let size = writer.bytes_written();
                (size, writer.into_inner().finish().await?)
            }
            FileEncoder::Columns(mut writer) => writer.finish().await?,
        };
        Ok(StagedFile {
            location: self.location,
            size: size as u64,
            num_rows: self.rows,
            publish,
        })
    }
}
