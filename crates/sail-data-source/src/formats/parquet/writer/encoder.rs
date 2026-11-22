// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
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
use tokio::sync::oneshot;

use crate::formats::parquet::writer::lane::{Lane, LaneInput, assign_lanes, logical_size};
use crate::formats::parquet::writer::resources::{EncodeResources, OpenFile};

/// The size of the encoded data of a file that is kept in memory
/// before the data is handed to the upload.
const UPLOAD_THRESHOLD: usize = 1024 * 1024;

/// Encodes a Parquet file.
///
/// The writer task either encodes the columns of a row group itself, one after
/// another, or it divides the columns into lanes. Each lane is a task that
/// encodes its columns from a queue, so the writer task does not wait for
/// the columns of a batch before it accepts the next batch. When a row group
/// has all its rows, the writer task continues with the next row group while
/// the lanes of the previous row groups complete their columns. The row groups
/// are added to the file in order as they are completed.
///
/// The input that waits in the queues is limited by the read-ahead budget of
/// the writer task, and the number of row groups that are encoded at the same
/// time is limited by the window of the file.
///
/// Each column writer receives the same data in the same order in all cases,
/// so the content of the file does not depend on how the file is encoded,
/// and it is the same as the content that [`ArrowWriter`] produces.
pub(super) struct ColumnEncoder {
    writer: SerializedFileWriter<SharedBuffer>,
    /// The encoded data that has not been handed to the upload.
    buffer: SharedBuffer,
    upload: BufWriter,
    /// The maximum size of the data that is handed to the upload at a time.
    upload_chunk_size: usize,
    schema: SchemaRef,
    /// The row group that receives rows.
    in_progress: Option<ArrowRowGroupWriter>,
    /// The row groups that have all their rows and are not in the file, in file order.
    sealed: VecDeque<SealedRowGroup>,
    /// The number of row groups that are encoded at the same time,
    /// as decided for the row group that receives rows.
    window: usize,
    row_group_writer_factory: ArrowRowGroupWriterFactory,
    max_row_group_row_count: Option<usize>,
    max_row_group_bytes: Option<usize>,
    /// The number of leaf columns of each top-level column.
    leaf_counts: Arc<[usize]>,
    /// The weight of each top-level column for dividing the columns into lanes.
    /// This is the size of the encoded data of the column in the row group
    /// that was last added to the file, or the number of leaf columns before that.
    column_weights: Vec<usize>,
    resources: Arc<EncodeResources>,
    _open_file: OpenFile,
}

/// A row group that has all its rows.
enum SealedRowGroup {
    Encoded(Vec<ArrowColumnChunk>),
    /// The lanes are encoding the input in their queues.
    Encoding(Vec<Lane>),
}

impl ColumnEncoder {
    pub fn try_new(
        upload: BufWriter,
        upload_chunk_size: usize,
        schema: SchemaRef,
        props: WriterProperties,
        skip_arrow_metadata: bool,
        resources: Arc<EncodeResources>,
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
            let column = writer.schema_descr().get_column_root_idx(leaf);
            if let Some(count) = leaf_counts.get_mut(column) {
                *count += 1;
            }
        }
        Ok(Self {
            writer,
            buffer,
            upload,
            upload_chunk_size: upload_chunk_size.max(1),
            schema,
            in_progress: None,
            sealed: VecDeque::new(),
            window: 1,
            row_group_writer_factory,
            max_row_group_row_count,
            max_row_group_bytes,
            column_weights: leaf_counts.clone(),
            leaf_counts: leaf_counts.into(),
            _open_file: resources.open_file(),
            resources,
        })
    }

    /// The number of row groups that have all their rows.
    pub fn row_groups(&self) -> usize {
        self.writer.flushed_row_groups().len() + self.sealed.len()
    }

    /// The number of lanes of the row group that receives rows.
    #[cfg(test)]
    pub fn lanes(&self) -> usize {
        self.in_progress.as_ref().map_or(0, |x| x.lanes.len())
    }

    /// The number of row groups that are being encoded,
    /// including the row group that receives rows.
    #[cfg(test)]
    pub fn row_groups_in_flight(&self) -> usize {
        self.sealed.len() + usize::from(self.in_progress.is_some())
    }

    /// The memory used by the data that has not been handed to the upload.
    /// This does not include the memory that the lanes reserve themselves,
    /// and the input that waits to be encoded.
    pub fn memory_size(&self) -> usize {
        let in_progress: usize = match &self.in_progress {
            Some(in_progress) => in_progress.writers.iter().map(|x| x.memory_size()).sum(),
            None => 0,
        };
        let sealed: usize = self
            .sealed
            .iter()
            .map(|row_group| match row_group {
                SealedRowGroup::Encoded(chunks) => chunks
                    .iter()
                    .map(|chunk| chunk.close().bytes_written as usize)
                    .sum(),
                SealedRowGroup::Encoding(_) => 0,
            })
            .sum();
        let buffered = self.buffer.buffer.try_lock().map_or(0, |x| x.len());
        in_progress + sealed + buffered
    }

    /// Writes a batch to the file.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // The row groups that have been completed since the last batch are added
        // to the file here, so that they do not wait for the next row group
        // to have all its rows.
        self.append_completed().await?;
        self.write(batch).await
    }

    /// Writes the remaining data and the footer, and completes the upload.
    /// Returns the size of the file.
    pub async fn finish(&mut self) -> Result<usize> {
        self.flush().await?;
        while !self.sealed.is_empty() {
            self.append_next().await?;
        }
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
    fn write<'a>(&'a mut self, batch: &'a RecordBatch) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return Ok(());
            }

            let in_progress = match &mut self.in_progress {
                Some(in_progress) => in_progress,
                x => {
                    let plan = self.resources.plan_row_group(self.schema.fields().len());
                    self.window = plan.window;
                    x.insert(
                        ArrowRowGroupWriter::new(
                            self.row_group_writer_factory.create_column_writers(
                                self.writer.flushed_row_groups().len() + self.sealed.len(),
                            )?,
                            &self.schema,
                        )
                        .with_lanes(
                            assign_lanes(&self.column_weights, plan.lanes),
                            &self.leaf_counts,
                            &self.resources,
                        ),
                    )
                }
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
                .write(batch, &self.resources, self.max_row_group_bytes.is_some())
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

    /// Ends the row group that receives rows. The row group is added to the file
    /// when its columns are completed and all row groups before it are in the file.
    /// This waits for row groups to be completed only as far as the window requires.
    async fn flush(&mut self) -> Result<()> {
        let in_progress = match self.in_progress.take() {
            Some(in_progress) => in_progress,
            None => return Ok(()),
        };
        self.sealed.push_back(in_progress.seal(&self.resources)?);
        self.append_completed().await?;
        while self.sealed.len() >= self.window {
            self.append_next().await?;
        }
        Ok(())
    }

    /// Adds the row groups to the file that can be added without waiting for a lane.
    async fn append_completed(&mut self) -> Result<()> {
        while self.sealed.front().is_some_and(|x| x.is_completed()) {
            self.append_next().await?;
        }
        Ok(())
    }

    /// Adds the first of the row groups that are not in the file to the file,
    /// after waiting for its columns to be completed.
    async fn append_next(&mut self) -> Result<()> {
        // The lanes keep their memory reserved until the row group is in the file.
        let mut reservations = vec![];
        let chunks = match self.sealed.pop_front() {
            Some(SealedRowGroup::Encoded(chunks)) => chunks,
            Some(SealedRowGroup::Encoding(lanes)) => {
                // The lanes have the columns in any order,
                // and the file has the columns in the order of the schema.
                let mut columns: Vec<Vec<ArrowColumnChunk>> =
                    self.leaf_counts.iter().map(|_| vec![]).collect();
                for lane in lanes {
                    let lane_columns = lane.columns().to_vec();
                    let output = lane.join().await?;
                    reservations.push(output.reservation);
                    let mut chunks = output.chunks.into_iter();
                    for column in lane_columns {
                        let (Some(leaves), Some(column)) =
                            (self.leaf_counts.get(column), columns.get_mut(column))
                        else {
                            return Err(ParquetError::General(format!(
                                "a column encoder has completed column {column}, which is not in the file"
                            )));
                        };
                        *column = chunks.by_ref().take(*leaves).collect();
                    }
                }
                columns.into_iter().flatten().collect()
            }
            None => return Ok(()),
        };
        {
            let mut sizes = chunks
                .iter()
                .map(|chunk| chunk.close().bytes_written as usize);
            for (weight, leaves) in self.column_weights.iter_mut().zip(self.leaf_counts.iter()) {
                *weight = sizes.by_ref().take(*leaves).sum();
            }
        }
        self.append_row_group(chunks).await?;
        drop(reservations);
        Ok(())
    }

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/arrow-rs/blob/59.2.0/parquet/src/arrow/arrow_writer/mod.rs#L446-L463
    /// Flushes all buffered rows into a new row group
    ///
    /// Note the underlying writer is not flushed with this call.
    /// If this is a desired behavior, please call [`ArrowWriter::sync`].
    async fn append_row_group(&mut self, chunks: Vec<ArrowColumnChunk>) -> Result<()> {
        let mut row_group_writer = self.writer.next_row_group()?;
        for chunk in chunks {
            {
                let _timer = self.resources.elapsed_compute().timer();
                chunk.append_to_row_group(&mut row_group_writer)?;
            }
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

impl SealedRowGroup {
    /// Whether the columns of the row group can be added to the file without waiting.
    fn is_completed(&self) -> bool {
        match self {
            Self::Encoded(_) => true,
            Self::Encoding(lanes) => lanes.iter().all(|x| x.has_ended()),
        }
    }
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

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/arrow-rs/blob/59.2.0/parquet/src/arrow/arrow_writer/mod.rs#L1204-L1269
/// Encodes [`RecordBatch`] to a parquet row group
///
/// Note: this structure is created by [`ArrowRowGroupWriterFactory`] internally used to
/// create [`ArrowRowGroupWriter`]s, but it is not exposed publicly.
///
/// See the example on [`ArrowColumnWriter`] for how to encode columns in parallel
struct ArrowRowGroupWriter {
    writers: Vec<ArrowColumnWriter>,
    schema: SchemaRef,
    buffered_rows: usize,
    /// The lanes that have the writers when the columns are encoded by other tasks.
    lanes: Vec<Lane>,
    /// The estimated size of the encoded data of the lanes
    /// when the lanes report it for each part of a batch.
    lanes_estimated_bytes: usize,
}

impl ArrowRowGroupWriter {
    fn new(writers: Vec<ArrowColumnWriter>, arrow: &SchemaRef) -> Self {
        Self {
            writers,
            schema: arrow.clone(),
            buffered_rows: 0,
            lanes: vec![],
            lanes_estimated_bytes: 0,
        }
    }

    async fn write(
        &mut self,
        batch: &RecordBatch,
        resources: &EncodeResources,
        report_estimated_bytes: bool,
    ) -> Result<()> {
        self.buffered_rows += batch.num_rows();
        if !self.lanes.is_empty() {
            return self
                .write_to_lanes(batch, resources, report_estimated_bytes)
                .await;
        }
        let _timer = resources.elapsed_compute().timer();
        let mut writers = self.writers.iter_mut();
        for (field, column) in self.schema.fields().iter().zip(batch.columns()) {
            for leaf in compute_leaves(field.as_ref(), column)? {
                writers
                    .next()
                    .ok_or_else(|| {
                        ParquetError::General("missing column writer for leaf column".to_string())
                    })?
                    .write(&leaf)?;
            }
        }
        Ok(())
    }

    /// Returns the estimated total encoded bytes for this row group
    fn get_estimated_total_bytes(&self) -> usize {
        self.writers
            .iter()
            .map(|x| x.get_estimated_total_bytes())
            .sum::<usize>()
            + self.lanes_estimated_bytes
    }

    fn close(self) -> Result<Vec<ArrowColumnChunk>> {
        self.writers
            .into_iter()
            .map(|writer| writer.close())
            .collect()
    }
}
// SPDX-SnippetEnd

impl ArrowRowGroupWriter {
    /// Moves the column writers to lanes, each of which is encoded by one task.
    /// The writer task keeps the column writers when there are no lanes.
    fn with_lanes(
        mut self,
        lanes: Vec<Vec<usize>>,
        leaf_counts: &[usize],
        resources: &EncodeResources,
    ) -> Self {
        if lanes.is_empty() {
            return self;
        }
        let mut writers = std::mem::take(&mut self.writers).into_iter();
        let mut writers: Vec<Vec<ArrowColumnWriter>> = leaf_counts
            .iter()
            .map(|leaves| writers.by_ref().take(*leaves).collect())
            .collect();
        self.lanes = lanes
            .into_iter()
            .map(|columns| {
                let fields = columns
                    .iter()
                    .filter_map(|&column| self.schema.fields().get(column).cloned())
                    .collect();
                let writers = columns
                    .iter()
                    .filter_map(|&column| writers.get_mut(column).map(std::mem::take))
                    .flatten()
                    .collect();
                Lane::spawn(
                    columns,
                    fields,
                    writers,
                    resources.new_reservation(),
                    resources.elapsed_compute().clone(),
                )
            })
            .collect();
        self
    }

    /// Adds the columns of the batch to the queues of the lanes.
    ///
    /// This waits while the input that waits to be encoded exceeds the read-ahead
    /// budget. It waits for the lanes to encode the batch only when the lanes
    /// report the estimated size of the encoded data, which is needed when
    /// the size of a row group is limited in bytes.
    async fn write_to_lanes(
        &mut self,
        batch: &RecordBatch,
        resources: &EncodeResources,
        report_estimated_bytes: bool,
    ) -> Result<()> {
        let mut reports = vec![];
        for lane in &mut self.lanes {
            let columns = lane
                .columns()
                .iter()
                .map(|&column| {
                    batch.columns().get(column).cloned().ok_or_else(|| {
                        ParquetError::General(format!("the batch does not have column {column}"))
                    })
                })
                .collect::<Result<Vec<ArrayRef>>>()?;
            let bytes = columns.iter().map(|x| logical_size(x.as_ref())).sum();
            let admission = resources.admit(bytes).await?;
            let encoded = if report_estimated_bytes {
                let (encoded, report) = oneshot::channel();
                reports.push(report);
                Some(encoded)
            } else {
                None
            };
            lane.send(LaneInput {
                columns,
                encoded,
                admission,
            })
            .await?;
        }
        if report_estimated_bytes {
            let mut estimated_bytes = 0;
            for (lane, report) in self.lanes.iter_mut().zip(reports) {
                match report.await {
                    Ok(bytes) => estimated_bytes += bytes,
                    Err(_) => return Err(lane.failure().await),
                }
            }
            self.lanes_estimated_bytes = estimated_bytes;
        }
        Ok(())
    }

    /// Ends the row group. The writer task completes the columns here
    /// when it has the column writers, and the lanes complete their columns
    /// after they have encoded the input in their queues.
    fn seal(mut self, resources: &EncodeResources) -> Result<SealedRowGroup> {
        if self.lanes.is_empty() {
            let _timer = resources.elapsed_compute().timer();
            return Ok(SealedRowGroup::Encoded(self.close()?));
        }
        for lane in &mut self.lanes {
            lane.seal();
        }
        Ok(SealedRowGroup::Encoding(self.lanes))
    }
}
