// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{FieldRef, SchemaRef};
use datafusion::common::runtime::SpawnedTask;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, ArrowWriterOptions,
    compute_leaves,
};
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::writer::SerializedFileWriter;
use datafusion_datasource::write::SharedBuffer;
use futures::future::BoxFuture;
use object_store::buffered::BufWriter;
use tokio::io::AsyncWriteExt;

/// The size of the encoded data of a file that is kept in memory
/// before the data is handed to the upload.
const UPLOAD_THRESHOLD: usize = 1024 * 1024;

/// The number of rows below which a batch is encoded without using other tasks,
/// since the cost of starting the tasks would exceed the benefit.
const MIN_ROWS_FOR_LANES: usize = 1024;

/// Encodes a Parquet file, with the columns of each batch encoded in parallel
/// when there is more than one lane.
///
/// The columns of a batch are divided into lanes, and each lane is encoded by
/// one task. All tasks are joined before the next batch is accepted. With one
/// lane, the columns are encoded one after another without other tasks. Each
/// column receives the same data in the same order regardless of the number
/// of lanes, so the content of the file does not depend on it, and it is the
/// same as the content that [`ArrowWriter`] produces.
pub(super) struct ColumnEncoder {
    writer: SerializedFileWriter<SharedBuffer>,
    /// The encoded data that has not been handed to the upload.
    buffer: SharedBuffer,
    upload: BufWriter,
    /// The maximum size of the data that is handed to the upload at a time.
    upload_chunk_size: usize,
    schema: SchemaRef,
    in_progress: Option<ArrowRowGroupWriter>,
    row_group_writer_factory: ArrowRowGroupWriterFactory,
    max_row_group_row_count: Option<usize>,
    max_row_group_bytes: Option<usize>,
    /// The number of leaf columns of each top-level column.
    leaf_counts: Arc<[usize]>,
    /// The maximum number of lanes.
    lanes: usize,
}

impl ColumnEncoder {
    pub fn try_new(
        upload: BufWriter,
        upload_chunk_size: usize,
        schema: SchemaRef,
        props: WriterProperties,
        skip_arrow_metadata: bool,
        lanes: usize,
    ) -> Result<Self> {
        let max_row_group_row_count = props.max_row_group_row_count();
        let max_row_group_bytes = props.max_row_group_bytes();
        let buffer = SharedBuffer::new(UPLOAD_THRESHOLD);
        let options = ArrowWriterOptions::new()
            .with_properties(props)
            .with_skip_arrow_metadata(skip_arrow_metadata);
        let writer =
            ArrowWriter::try_new_with_options(buffer.clone(), Arc::clone(&schema), options)?;
        let (writer, row_group_writer_factory) = writer.into_serialized_writer()?;
        let mut leaf_counts = vec![0; schema.fields().len()];
        for leaf in 0..writer.schema_descr().num_columns() {
            leaf_counts[writer.schema_descr().get_column_root_idx(leaf)] += 1;
        }
        Ok(Self {
            writer,
            buffer,
            upload,
            upload_chunk_size: upload_chunk_size.max(1),
            schema,
            in_progress: None,
            row_group_writer_factory,
            max_row_group_row_count,
            max_row_group_bytes,
            leaf_counts: leaf_counts.into(),
            lanes,
        })
    }

    /// The number of row groups that have been completed.
    pub fn flushed_row_groups(&self) -> usize {
        self.writer.flushed_row_groups().len()
    }

    /// The memory used by the data that has not been handed to the upload.
    pub fn memory_size(&self) -> usize {
        let in_progress: usize = match &self.in_progress {
            Some(in_progress) => in_progress.writers.iter().map(|x| x.memory_size()).sum(),
            None => 0,
        };
        let buffered = self.buffer.buffer.try_lock().map_or(0, |x| x.len());
        in_progress + buffered
    }

    /// Writes the remaining data and the footer, and completes the upload.
    /// Returns the size of the file.
    pub async fn finish(&mut self) -> Result<usize> {
        self.flush().await?;
        self.writer.finish()?;
        upload_buffer(&self.buffer, &mut self.upload, self.upload_chunk_size, 0).await?;
        self.upload
            .shutdown()
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        Ok(self.writer.bytes_written())
    }

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/arrow-rs/blob/59.2.0/parquet/src/arrow/arrow_writer/mod.rs#L347-L431
    /// Encodes the provided [`RecordBatch`]
    ///
    /// If this would cause the current row group to exceed [`WriterProperties::max_row_group_row_count`]
    /// rows or [`WriterProperties::max_row_group_bytes`] bytes, the contents of `batch` will be
    /// written to one or more row groups such that limits are respected.
    ///
    /// If both limits are `None`, all data is written to a single row group.
    /// If one limit is set, that limit is respected.
    /// If both limits are set, the lower bound (whichever triggers first) is respected.
    ///
    /// This will fail if the `batch`'s schema does not match the writer's schema.
    #[expect(clippy::collapsible_if)]
    pub fn write<'a>(&'a mut self, batch: &'a RecordBatch) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return Ok(());
            }

            let in_progress = match &mut self.in_progress {
                Some(in_progress) => in_progress,
                x => x.insert(ArrowRowGroupWriter::new(
                    self.row_group_writer_factory
                        .create_column_writers(self.writer.flushed_row_groups().len())?,
                    &self.schema,
                )),
            };

            if let Some(max_rows) = self.max_row_group_row_count {
                if in_progress.buffered_rows + batch.num_rows() > max_rows {
                    let to_write = max_rows - in_progress.buffered_rows;
                    let a = batch.slice(0, to_write);
                    let b = batch.slice(to_write, batch.num_rows() - to_write);
                    self.write(&a).await?;
                    return self.write(&b).await;
                }
            }

            // Check byte limit: if we have buffered data, use measured average row size
            // to split batch proactively before exceeding byte limit
            if let Some(max_bytes) = self.max_row_group_bytes {
                if in_progress.buffered_rows > 0 {
                    let current_bytes = in_progress.get_estimated_total_bytes();

                    if current_bytes >= max_bytes {
                        self.flush().await?;
                        return self.write(batch).await;
                    }

                    if let Some(avg_row_bytes) = current_bytes
                        .checked_div(in_progress.buffered_rows)
                        .filter(|avg_row_bytes| *avg_row_bytes > 0)
                    {
                        // At this point, `current_bytes < max_bytes` (checked above)
                        let remaining_bytes = max_bytes - current_bytes;
                        let rows_that_fit = remaining_bytes.checked_div(avg_row_bytes).unwrap_or(0);

                        if batch.num_rows() > rows_that_fit {
                            if rows_that_fit > 0 {
                                let a = batch.slice(0, rows_that_fit);
                                let b =
                                    batch.slice(rows_that_fit, batch.num_rows() - rows_that_fit);
                                self.write(&a).await?;
                                return self.write(&b).await;
                            } else {
                                self.flush().await?;
                                return self.write(batch).await;
                            }
                        }
                    }
                }
            }

            in_progress
                .write(batch, &self.leaf_counts, self.lanes)
                .await?;

            let should_flush = self
                .max_row_group_row_count
                .is_some_and(|max| in_progress.buffered_rows >= max)
                || self
                    .max_row_group_bytes
                    .is_some_and(|max| in_progress.get_estimated_total_bytes() >= max);

            if should_flush {
                self.flush().await?
            }
            Ok(())
        })
    }
    // SPDX-SnippetEnd

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/arrow-rs/blob/59.2.0/parquet/src/arrow/arrow_writer/mod.rs#L446-L463
    /// Flushes all buffered rows into a new row group
    ///
    /// Note the underlying writer is not flushed with this call.
    /// If this is a desired behavior, please call [`ArrowWriter::sync`].
    async fn flush(&mut self) -> Result<()> {
        let in_progress = match self.in_progress.take() {
            Some(in_progress) => in_progress,
            None => return Ok(()),
        };

        let mut row_group_writer = self.writer.next_row_group()?;
        for chunk in in_progress.close()? {
            chunk.append_to_row_group(&mut row_group_writer)?;
            upload_buffer(
                &self.buffer,
                &mut self.upload,
                self.upload_chunk_size,
                UPLOAD_THRESHOLD,
            )
            .await?;
        }
        row_group_writer.close()?;
        Ok(())
    }
    // SPDX-SnippetEnd
}

/// Hands the encoded data to the upload if the size of the data exceeds the threshold.
async fn upload_buffer(
    buffer: &SharedBuffer,
    upload: &mut BufWriter,
    chunk_size: usize,
    threshold: usize,
) -> Result<()> {
    let data = {
        let mut buffer = buffer
            .buffer
            .try_lock()
            .ok_or_else(|| ParquetError::General("the file buffer is in use".to_string()))?;
        if buffer.len() <= threshold {
            return Ok(());
        }
        let capacity = buffer.capacity();
        Bytes::from(std::mem::replace(
            &mut *buffer,
            Vec::with_capacity(capacity),
        ))
    };
    let mut offset = 0;
    while offset < data.len() {
        let end = data.len().min(offset + chunk_size);
        // This waits when too many parts of the file are being uploaded.
        upload
            .put(data.slice(offset..end))
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        offset = end;
    }
    Ok(())
}

/// A top-level column of a batch with the writers of its leaf columns.
struct LaneColumn {
    /// The index of the column in the schema.
    index: usize,
    field: FieldRef,
    array: ArrayRef,
    writers: Vec<ArrowColumnWriter>,
}

impl LaneColumn {
    fn estimated_bytes(&self) -> usize {
        self.writers
            .iter()
            .map(|x| x.get_estimated_total_bytes())
            .sum()
    }
}

/// Divides the columns into lanes of similar size, using the size of
/// the data that each column has encoded so far in the row group.
fn assign_lanes(mut columns: Vec<LaneColumn>, lanes: usize) -> Vec<Vec<LaneColumn>> {
    let mut assigned: Vec<(usize, Vec<LaneColumn>)> = (0..lanes).map(|_| (0, vec![])).collect();
    columns.sort_by_key(|column| std::cmp::Reverse(column.estimated_bytes()));
    for column in columns {
        if let Some((size, lane)) = assigned
            .iter_mut()
            .min_by_key(|(size, lane)| (*size, lane.len()))
        {
            *size += column.estimated_bytes();
            lane.push(column);
        }
    }
    assigned.into_iter().map(|(_, lane)| lane).collect()
}

/// Encodes the columns of a lane and returns the columns.
fn encode_lane(mut lane: Vec<LaneColumn>) -> Result<Vec<LaneColumn>> {
    for column in &mut lane {
        let leaves = compute_leaves(column.field.as_ref(), &column.array)?;
        for (leaf, writer) in leaves.iter().zip(column.writers.iter_mut()) {
            writer.write(leaf)?;
        }
    }
    Ok(lane)
}

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/arrow-rs/blob/59.2.0/parquet/src/arrow/arrow_writer/mod.rs#L1204-L1269
/// Encodes [`RecordBatch`] to a parquet row group
///
/// Note: this structure is created by [`ArrowRowGroupWriterFactory`] internally used to
/// create [`ArrowRowGroupWriter`]s, but it is not exposed publicly.
///
/// See the example on [`ArrowColumnWriter`] for how to encode columns in parallel
#[derive(Debug)]
struct ArrowRowGroupWriter {
    writers: Vec<ArrowColumnWriter>,
    schema: SchemaRef,
    buffered_rows: usize,
}

impl ArrowRowGroupWriter {
    fn new(writers: Vec<ArrowColumnWriter>, arrow: &SchemaRef) -> Self {
        Self {
            writers,
            schema: arrow.clone(),
            buffered_rows: 0,
        }
    }

    #[expect(clippy::unwrap_used)]
    async fn write(
        &mut self,
        batch: &RecordBatch,
        leaf_counts: &[usize],
        lanes: usize,
    ) -> Result<()> {
        self.buffered_rows += batch.num_rows();
        let lanes = if batch.num_rows() < MIN_ROWS_FOR_LANES {
            1
        } else {
            lanes.clamp(1, batch.num_columns().max(1))
        };
        if lanes == 1 {
            let mut writers = self.writers.iter_mut();
            for (field, column) in self.schema.fields().iter().zip(batch.columns()) {
                for leaf in compute_leaves(field.as_ref(), column)? {
                    writers.next().unwrap().write(&leaf)?;
                }
            }
            return Ok(());
        }
        let mut writers = std::mem::take(&mut self.writers).into_iter();
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (index, (field, column)) in self.schema.fields().iter().zip(batch.columns()).enumerate()
        {
            let leaves = leaf_counts.get(index).copied().unwrap_or(0);
            columns.push(LaneColumn {
                index,
                field: Arc::clone(field),
                array: Arc::clone(column),
                writers: writers.by_ref().take(leaves).collect(),
            });
        }
        let mut lanes = assign_lanes(columns, lanes).into_iter();
        let first = lanes.next().unwrap_or_default();
        let tasks: Vec<_> = lanes
            .map(|lane| SpawnedTask::spawn(async move { encode_lane(lane) }))
            .collect();
        let mut columns = encode_lane(first)?;
        for task in tasks {
            let lane = task
                .join()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))??;
            columns.extend(lane);
        }
        columns.sort_by_key(|column| column.index);
        self.writers = columns
            .into_iter()
            .flat_map(|column| column.writers)
            .collect();
        Ok(())
    }

    /// Returns the estimated total encoded bytes for this row group
    fn get_estimated_total_bytes(&self) -> usize {
        self.writers
            .iter()
            .map(|x| x.get_estimated_total_bytes())
            .sum()
    }

    fn close(self) -> Result<Vec<ArrowColumnChunk>> {
        self.writers
            .into_iter()
            .map(|writer| writer.close())
            .collect()
    }
}
// SPDX-SnippetEnd
