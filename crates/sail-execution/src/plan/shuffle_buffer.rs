// SPDX-License-Identifier: Apache-2.0

//! Holds the rows of a shuffle write until there are enough of them to write, and copies
//! them into one batch per output channel.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, ArrowPrimitiveType, AsArray, BooleanBufferBuilder, ByteView,
    GenericByteViewArray, MAX_INLINE_VIEW_LEN, PrimitiveArray, UInt32Array, downcast_primitive,
};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use datafusion::arrow::compute::{concat, take};
use datafusion::arrow::datatypes::{
    BinaryViewType, ByteViewType, DataType, Int16Type, Int32Type, Int64Type, Schema, SchemaRef,
    StringViewType,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{HashMap, Result};

use crate::plan::shuffle_compact::{
    allocated_bytes, contains_dictionary, contains_list_view, copy, dictionary_capacities,
    dictionary_sizes, oversized, trim,
};

macro_rules! primitive_runs {
    ($t:ty, $held:ident, $column:ident, $runs:ident, $rows:ident) => {
        copy_primitives::<$t>($held, $column, $runs, $rows)
    };
}

/// The batches of one write to the sink, each with its channel.
pub(super) type Round = Vec<(usize, RecordBatch)>;

/// The rows of a held batch that go to one channel. They end where the next piece of
/// the batch starts.
#[derive(Clone, Copy)]
struct Piece {
    channel: u32,
    start: u32,
}

struct Held {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
    /// Where the pieces of the next held batch start.
    pieces_end: usize,
    /// For each column, how many of its first data buffers have the strings of one
    /// piece each. It is empty for a batch that is written as it is.
    own: Vec<u32>,
}

impl Held {
    /// Returns the bytes of the batch apart from the buffers of its columns.
    fn bytes(&self) -> usize {
        let columns: usize = self.columns.iter().map(array_own_bytes).sum();
        size_of::<Self>() + columns + self.own.len() * size_of::<u32>()
    }
}

/// Returns the bytes of an array and of the arrays inside it, apart from their buffers.
/// An array that several arrays share is counted for each of them.
fn array_own_bytes(array: &ArrayRef) -> usize {
    // An array is behind a pointer, and is allocated with two counters.
    let own = size_of::<[usize; 4]>() + size_of_val(&**array);
    let inside: usize = match array.data_type() {
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .map(array_own_bytes)
            .sum(),
        DataType::List(_) => array_own_bytes(array.as_list::<i32>().values()),
        DataType::LargeList(_) => array_own_bytes(array.as_list::<i64>().values()),
        DataType::ListView(_) => array_own_bytes(array.as_list_view::<i32>().values()),
        DataType::LargeListView(_) => array_own_bytes(array.as_list_view::<i64>().values()),
        DataType::FixedSizeList(_, _) => array_own_bytes(array.as_fixed_size_list().values()),
        // The entries of a map are in the map itself, and have its keys and values.
        DataType::Map(_, _) => {
            let entries = array.as_map().entries();
            entries.columns().iter().map(array_own_bytes).sum()
        }
        DataType::Dictionary(_, _) => array_own_bytes(array.as_any_dictionary().values()),
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => array_own_bytes(array.as_run::<Int16Type>().values()),
            DataType::Int32 => array_own_bytes(array.as_run::<Int32Type>().values()),
            DataType::Int64 => array_own_bytes(array.as_run::<Int64Type>().values()),
            _ => 0,
        },
        DataType::Union(fields, _) => {
            let union = array.as_union();
            // A union has a pointer for every type id up to its largest.
            let ids = fields.iter().map(|(id, _)| id as usize + 1).max();
            let unused = ids.unwrap_or(0).saturating_sub(fields.len());
            let children = fields
                .iter()
                .map(|(id, _)| array_own_bytes(union.child(id)));
            unused * size_of::<Option<ArrayRef>>() + children.sum::<usize>()
        }
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => 0,
    };
    own + inside
}

/// Rows of a held batch that are copied into one output batch.
#[derive(Clone, Copy, Default)]
struct Run {
    batch: u32,
    start: u32,
    rows: u32,
    /// Whether these are only some of the rows of a piece.
    cut: bool,
}

impl Run {
    fn range(&self) -> Range<usize> {
        self.start as usize..(self.start + self.rows) as usize
    }
}

/// What the views of a view column of the held batches point at.
#[derive(Clone, Copy, Default)]
struct ViewBytes {
    /// The bytes of the data buffers, each counted once.
    distinct: usize,
    /// The bytes of the strings too long to inline, counted for every view.
    referenced: usize,
}

/// Returns the bytes of a buffer unless it is one of `shared` already, and adds it to them.
fn distinct_bytes(shared: &mut HashSet<usize>, buffer: &Buffer) -> usize {
    if shared.insert(buffer.data_ptr().as_ptr() as usize) {
        buffer.capacity()
    } else {
        0
    }
}

/// The rows of a shuffle write that have not been written yet.
///
/// Rows are held until their memory reaches the budget or there is nearly a batch of them
/// for every channel. All channels are then written at once, each as batches that own
/// their memory, so that what is held does not depend on the number of channels. A budget
/// of zero is no limit on the memory.
pub(super) struct ShuffleBuffer {
    channels: usize,
    batch_size: usize,
    budget: usize,
    /// Whether each held batch goes to one channel as a whole. Otherwise its rows have
    /// been reordered by channel.
    whole: bool,
    held: Vec<Held>,
    pieces: Vec<Piece>,
    rows: usize,
    /// The bytes of the buffers of the held batches, each counted once, and of the
    /// batches apart from their buffers.
    bytes: usize,
    /// What each view column holds, and `None` for the columns of other types.
    views: Vec<Option<ViewBytes>>,
    /// The buffers that the held batches share with each other or with their input.
    shared: HashSet<usize>,
    /// One counter per channel, for grouping the pieces by channel.
    counts: Vec<u32>,
    /// The columns with dictionaries or list views, whose pieces keep the values of
    /// the whole batch.
    compacted: Vec<usize>,
    compacted_schema: SchemaRef,
    /// How many values each dictionary of the compacted columns can hold.
    capacities: Vec<usize>,
}

impl ShuffleBuffer {
    pub(super) fn try_new(
        schema: &Schema,
        channels: usize,
        batch_size: usize,
        budget: usize,
        whole: bool,
    ) -> Result<Self> {
        let compacted: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                contains_dictionary(field.data_type()) || contains_list_view(field.data_type())
            })
            .map(|(column, _)| column)
            .collect();
        let compacted_schema = Arc::new(schema.project(&compacted)?);
        let capacities = dictionary_capacities(&compacted_schema);
        let views = schema
            .fields()
            .iter()
            .map(|field| {
                matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView)
                    .then(ViewBytes::default)
            })
            .collect();
        Ok(Self {
            channels,
            batch_size,
            budget: match budget {
                0 => usize::MAX,
                budget => budget,
            },
            whole,
            held: vec![],
            pieces: vec![],
            rows: 0,
            bytes: 0,
            views,
            shared: HashSet::new(),
            counts: vec![],
            compacted,
            compacted_schema,
            capacities,
        })
    }

    /// Holds a batch whose rows all go to `channel`, and returns what is to be written.
    ///
    /// The batch is not copied while it is held, and a batch of more than half the batch
    /// size is written at once, as it is.
    pub(super) fn push_batch(
        &mut self,
        channel: usize,
        batch: RecordBatch,
        flush: bool,
    ) -> Result<Vec<Round>> {
        let (schema, mut columns, rows) = batch.into_parts();
        self.pieces.push(Piece {
            channel: channel as u32,
            start: 0,
        });
        let large = rows > self.batch_size / 2;
        let mut own = vec![];
        if !large {
            // A slice of a larger batch would fill the budget by itself.
            let piece = &self.pieces[self.pieces.len() - 1..];
            (columns, own) = columns
                .into_iter()
                .map(|column| match oversized(&column) {
                    true => Ok((copy(&column)?, 0)),
                    false => detach(column, piece),
                })
                .collect::<Result<_>>()?;
        }
        self.push(schema, columns, own, rows, large, flush)
    }

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/cce6374e6256b3b8892ec60674578cf32bc3a32c/datafusion/physical-plan/src/repartition/mod.rs#L1345-L1392
    /// Holds a batch with the rows of each channel, and returns what is to be written.
    /// The lists of rows are left empty.
    ///
    /// The rows are reordered by channel with one `take`. The lists keep their memory for
    /// the next batch, unless they have more of it than four times what a batch needs.
    pub(super) fn push_rows(
        &mut self,
        batch: RecordBatch,
        rows: &mut [Vec<u32>],
        flush: bool,
    ) -> Result<Vec<Round>> {
        let mut order: Vec<u32> = Vec::with_capacity(batch.num_rows());
        let first = self.pieces.len();
        let mut capacity = 0;
        for (channel, rows) in rows.iter_mut().enumerate() {
            capacity += rows.capacity();
            if !rows.is_empty() {
                self.pieces.push(Piece {
                    channel: channel as u32,
                    start: order.len() as u32,
                });
                order.extend_from_slice(rows);
                rows.clear();
            }
        }
        // Each list keeps the capacity of the most rows its channel has had in one batch, so
        // when batches go to different channels the lists together can keep a batch per
        // channel. Release them once they keep over four entries per row of a batch: a batch
        // needs one entry per row, a list that grows by doubling can keep two, and rows moving
        // between channels from batch to batch can double that again, so ordinary input stays
        // under four.
        if capacity > 4 * (self.batch_size.max(batch.num_rows()) + rows.len()) {
            rows.fill_with(Vec::new);
        }
        let pieces = &self.pieces[first..];
        let order = UInt32Array::from(order);
        let (columns, own) = batch
            .columns()
            .iter()
            .map(|column| reorder(column, &order, pieces))
            .collect::<Result<_>>()?;
        self.push(batch.schema(), columns, own, batch.num_rows(), false, flush)
    }
    // SPDX-SnippetEnd

    fn push(
        &mut self,
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        own: Vec<u32>,
        rows: usize,
        large: bool,
        flush: bool,
    ) -> Result<Vec<Round>> {
        let before = self.held_bytes();
        // A large batch is written at once, whatever it holds.
        if !large {
            self.count(&columns);
        }
        let first = self.held.len();
        let held = Held {
            schema,
            columns,
            rows,
            pieces_end: self.pieces.len(),
            own,
        };
        // Many batches of few rows hold more memory apart from their buffers than in them.
        if !large {
            self.bytes += held.bytes();
        }
        self.held.push(held);
        self.rows += rows;
        let bytes = self.held_bytes();
        // Channels that receive the same share of the rows do not hold exactly as many. At
        // a full batch for each, half of them would hold a few rows more than a batch and
        // be written as two batches of half the size. So the rows are written a little
        // earlier: a 32nd of a batch is about three standard deviations of the rows of an
        // evenly hashed channel at the default batch size.
        let rows_per_channel = self.batch_size - self.batch_size / 32;
        let full =
            bytes >= self.budget || self.rows >= self.channels.saturating_mul(rows_per_channel);
        if !(flush || large || full) {
            return Ok(vec![]);
        }
        let mut rounds = vec![];
        // A batch that fills the budget by itself is written apart from the rows held
        // before it. An output batch then has at most the bytes of one input batch or of
        // two budgets, so its offsets overflow only if those of its input did.
        if first > 0 && (large || bytes - before >= self.budget) {
            self.build(0..first, &mut rounds)?;
            self.build(first..first + 1, &mut rounds)?;
        } else {
            self.build(0..first + 1, &mut rounds)?;
        }
        self.clear();
        Ok(rounds)
    }

    /// Returns everything that is held, to be written.
    pub(super) fn flush(&mut self) -> Result<Vec<Round>> {
        let mut rounds = vec![];
        self.build(0..self.held.len(), &mut rounds)?;
        self.clear();
        Ok(rounds)
    }

    fn clear(&mut self) {
        self.held.clear();
        self.pieces.clear();
        self.rows = 0;
        self.bytes = 0;
        for views in self.views.iter_mut().flatten() {
            *views = ViewBytes::default();
        }
        self.shared.clear();
    }

    /// Returns the bytes that the held batches keep alive.
    fn held_bytes(&self) -> usize {
        self.bytes + self.pieces.len() * size_of::<Piece>()
    }

    /// Adds the memory of the columns of a batch to what is held.
    fn count(&mut self, columns: &[ArrayRef]) {
        for (column, views) in columns.iter().zip(self.views.iter_mut()) {
            let Some(views) = views else {
                // A whole input batch keeps all its buffers alive, however few of them
                // its rows use, and `take` gives a primitive column buffers of its own.
                self.bytes += if self.whole || column.data_type().is_primitive() {
                    column.get_buffer_memory_size()
                } else {
                    array_bytes(&column.to_data(), false, &mut self.shared)
                };
                continue;
            };
            let data = column.to_data();
            let nulls = data.nulls().map_or(0, |nulls| nulls.buffer().capacity());
            self.bytes += data.buffers()[0].capacity() + nulls;
            for buffer in &data.buffers()[1..] {
                let distinct = if self.whole {
                    buffer.capacity()
                } else {
                    distinct_bytes(&mut self.shared, buffer)
                };
                self.bytes += distinct;
                views.distinct += distinct;
            }
            views.referenced += referenced_bytes(&data);
        }
    }

    /// Copies the rows of the held batches in `batches` into the batches of each channel,
    /// and adds them to `rounds` after the batches already there.
    fn build(&mut self, batches: Range<usize>, rounds: &mut Vec<Round>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let first = match batches.start {
            0 => 0,
            start => self.held[start - 1].pieces_end,
        };
        let last = self.held[batches.end - 1].pieces_end;
        // The pieces are grouped by channel, keeping the order of their batches.
        self.counts.clear();
        self.counts.resize(self.channels, 0);
        for piece in &self.pieces[first..last] {
            self.counts[piece.channel as usize] += 1;
        }
        let mut total = 0;
        for count in self.counts.iter_mut() {
            let pieces = *count;
            *count = total;
            total += pieces;
        }
        let mut runs = vec![Run::default(); total as usize];
        let mut start = first;
        for batch in batches {
            let held = &self.held[batch];
            let pieces = &self.pieces[start..held.pieces_end];
            for (i, piece) in pieces.iter().enumerate() {
                let end = pieces
                    .get(i + 1)
                    .map_or(held.rows as u32, |next| next.start);
                let position = &mut self.counts[piece.channel as usize];
                runs[*position as usize] = Run {
                    batch: batch as u32,
                    start: piece.start,
                    rows: end - piece.start,
                    cut: false,
                };
                *position += 1;
            }
            start = held.pieces_end;
        }
        // Strings that several views point at would be copied once for each view, so
        // the strings of a view column are copied without looking for them among those
        // already copied only if no more bytes are referenced than the buffers have.
        let direct: Vec<bool> = self
            .views
            .iter()
            .map(|views| views.is_some_and(|views| views.referenced <= views.distinct))
            .collect();
        let mut built = vec![];
        let mut part = vec![];
        let mut start = 0;
        for channel in 0..self.channels {
            let end = self.counts[channel] as usize;
            if end > start {
                self.build_channel(channel, &runs[start..end], &direct, &mut part, &mut built)?;
            }
            start = end;
        }
        rounds.extend(built);
        Ok(())
    }

    /// Builds the batches of a channel from its runs, one in each of the first `rounds`.
    fn build_channel(
        &self,
        channel: usize,
        runs: &[Run],
        direct: &[bool],
        part: &mut Vec<Run>,
        rounds: &mut Vec<Round>,
    ) -> Result<()> {
        let mut round = 0;
        if let (true, [run]) = (self.whole, runs) {
            // A whole input batch is passed on rather than copied or split.
            let held = &self.held[run.batch as usize];
            let columns = held
                .columns
                .iter()
                .map(compact)
                .collect::<Result<Vec<_>>>()?;
            let batch = record_batch(&held.schema, columns, held.rows)?;
            place(rounds, &mut round, channel, batch);
            return Ok(());
        }
        let rows: usize = runs.iter().map(|run| run.rows as usize).sum();
        // Rows that do not fit in one batch are split evenly, rather than into full
        // batches and a small one.
        let limit = rows.div_ceil(rows.div_ceil(self.batch_size));
        if rows <= limit {
            return self.build_part(channel, runs, direct, &mut round, rounds);
        }
        part.clear();
        let mut part_rows = 0;
        for &run in runs {
            let mut run = run;
            while part_rows + run.rows as usize > limit {
                let head = (limit - part_rows) as u32;
                if head > 0 {
                    run.cut = true;
                    part.push(Run { rows: head, ..run });
                }
                run.start += head;
                run.rows -= head;
                self.build_part(channel, part, direct, &mut round, rounds)?;
                part.clear();
                part_rows = 0;
            }
            if run.rows > 0 {
                part_rows += run.rows as usize;
                part.push(run);
            }
        }
        if !part.is_empty() {
            self.build_part(channel, part, direct, &mut round, rounds)?;
        }
        Ok(())
    }

    /// Builds one batch from the runs, or several if the dictionaries of all the runs
    /// together would overflow their keys.
    fn build_part(
        &self,
        channel: usize,
        runs: &[Run],
        direct: &[bool],
        round: &mut usize,
        rounds: &mut Vec<Round>,
    ) -> Result<()> {
        if runs.len() == 1 || self.compacted.is_empty() {
            let batch = self.build_batch(runs, &[], direct)?;
            place(rounds, round, channel, batch);
            return Ok(());
        }
        // A piece of a column with dictionaries or list views still has the values of
        // its whole batch, so each piece is compacted before the pieces are concatenated.
        // A piece's dictionaries then hold only the values its keys use, which always fit.
        let pieces = runs
            .iter()
            .map(|run| {
                let columns = self
                    .compacted
                    .iter()
                    .map(|&column| trim(self.slice(column, run), false))
                    .collect::<Result<Vec<_>>>()?;
                record_batch(&self.compacted_schema, columns, run.rows as usize)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut sizes = vec![0; self.capacities.len()];
        let mut start = 0;
        for (i, piece) in pieces.iter().enumerate() {
            let incoming = dictionary_sizes(piece);
            let overflows = sizes
                .iter()
                .zip(&incoming)
                .zip(&self.capacities)
                .any(|((held, incoming), capacity)| held + incoming > *capacity);
            if overflows && i > start {
                let batch = self.build_batch(&runs[start..i], &pieces[start..i], direct)?;
                place(rounds, round, channel, batch);
                start = i;
                sizes.fill(0);
            }
            for (held, incoming) in sizes.iter_mut().zip(incoming) {
                *held += incoming;
            }
        }
        let batch = self.build_batch(&runs[start..], &pieces[start..], direct)?;
        place(rounds, round, channel, batch);
        Ok(())
    }

    /// Copies the rows of the runs into one batch that owns its memory. `pieces` has the
    /// compacted columns of each run if they have been compacted.
    fn build_batch(
        &self,
        runs: &[Run],
        pieces: &[RecordBatch],
        direct: &[bool],
    ) -> Result<RecordBatch> {
        let held = self.held.as_slice();
        let schema = &held[runs[0].batch as usize].schema;
        let rows = runs.iter().map(|run| run.rows as usize).sum();
        let columns = (0..schema.fields().len())
            .map(|column| {
                downcast_primitive! {
                    schema.field(column).data_type() => (primitive_runs, held, column, runs, rows),
                    DataType::Utf8View if direct[column] => {
                        copy_views::<StringViewType>(held, column, runs, rows)
                    }
                    DataType::Utf8View => {
                        copy_shared_views::<StringViewType>(held, column, runs, rows)
                    }
                    DataType::BinaryView if direct[column] => {
                        copy_views::<BinaryViewType>(held, column, runs, rows)
                    }
                    DataType::BinaryView => {
                        copy_shared_views::<BinaryViewType>(held, column, runs, rows)
                    }
                    _ => {
                        let compacted = self.compacted.iter().position(|&c| c == column);
                        let compacted = compacted.filter(|_| !pieces.is_empty());
                        let prepared = compacted.map(|compacted| {
                            pieces
                                .iter()
                                .map(|piece| Arc::clone(piece.column(compacted)))
                                .collect::<Vec<_>>()
                        });
                        concat_runs(held, column, runs, prepared)
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;
        record_batch(schema, columns, rows)
    }

    fn slice(&self, column: usize, run: &Run) -> ArrayRef {
        run_column(&self.held, column, run).slice(run.start as usize, run.rows as usize)
    }
}

fn record_batch(schema: &SchemaRef, columns: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &options,
    )?)
}

/// Adds a batch of a channel to the round after its previous batch.
fn place(rounds: &mut Vec<Round>, round: &mut usize, channel: usize, batch: RecordBatch) {
    if rounds.len() <= *round {
        rounds.resize_with(*round + 1, Vec::new);
    }
    rounds[*round].push((channel, batch));
    *round += 1;
}

/// The bytes of strings from which a piece of a held view column has a buffer of its own.
const OWN_BUFFER_BYTES: usize = 4096;

/// Compacts a held column that keeps alive more than twice what its rows use, as the
/// rows of a slice of a larger batch do. Holding it as it is would fill the budget with
/// a few rows. Returns the column, and how many of its first data buffers have the
/// strings of one of the `pieces` each.
fn detach(column: ArrayRef, pieces: &[Piece]) -> Result<(ArrayRef, u32)> {
    let data = column.to_data();
    if column.data_type().is_primitive() || !sparse(&data) {
        return Ok((column, 0));
    }
    match column.data_type() {
        DataType::Utf8View if fits(&data) => Ok(split_views::<StringViewType>(&column, pieces)),
        DataType::BinaryView if fits(&data) => Ok(split_views::<BinaryViewType>(&column, pieces)),
        _ => Ok((trim(column, false)?, 0)),
    }
}

/// Returns whether the strings that the views of an array point at fit in one buffer,
/// as the strings of a piece have to.
fn fits(data: &ArrayData) -> bool {
    referenced_bytes(data) <= i32::MAX as usize
}

/// Returns the rows of a column of an input batch in the given order, in which the rows
/// of each of the `pieces` follow each other, compacted like `detach` compacts a held
/// column. Also returns how many of the first data buffers of the column have the
/// strings of one piece each.
fn reorder(column: &ArrayRef, order: &UInt32Array, pieces: &[Piece]) -> Result<(ArrayRef, u32)> {
    let views = matches!(
        column.data_type(),
        DataType::Utf8View | DataType::BinaryView
    );
    if views && order.len() == column.len() {
        // The rows of a column of views use the same strings in any order.
        let data = column.to_data();
        if !sparse(&data) {
            return Ok((take(column, order, None)?, 0));
        }
        let placed = match column.data_type() {
            _ if !fits(&data) => None,
            DataType::Utf8View => place_views::<StringViewType>(column, order, pieces),
            _ => place_views::<BinaryViewType>(column, order, pieces),
        };
        if let Some(placed) = placed {
            return Ok(placed);
        }
    }
    detach(take(column, order, None)?, pieces)
}

/// Where the next string of a piece goes: to the end of the buffer of the piece, or to
/// an offset in the buffer that the pieces with few strings share.
#[derive(Clone, Copy)]
enum Cursor {
    Own(u32),
    Shared(u32),
}

/// Where a row of an input batch goes: the piece it is in, and its place in the order.
#[derive(Clone, Copy)]
struct Place {
    piece: u32,
    row: u32,
}

/// Does what `take` and then `split_views` do, for a column whose rows in each piece point
/// at strings that follow each other in the buffers without overlapping, so that none of
/// them is shared. Returns `None` for any other column, or if `order` does not have
/// every row of the column once.
///
/// The strings are copied in the order of the rows of the input rather than piece by
/// piece. That is the order in which they are in the buffers of a column whose rows have
/// only been filtered or sliced, and reading a large buffer in order is several times
/// faster than visiting it once for each piece.
fn place_views<T: ByteViewType>(
    column: &ArrayRef,
    order: &UInt32Array,
    pieces: &[Piece],
) -> Option<(ArrayRef, u32)> {
    const NONE: Place = Place {
        piece: u32::MAX,
        row: 0,
    };
    let order = order.values();
    let array = column.as_byte_view::<T>();
    let buffers = array.data_buffers();
    let source = array.views();
    let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
    let mut places = vec![NONE; array.len()];
    let mut cursors: Vec<Cursor> = Vec::with_capacity(pieces.len());
    let mut own: Vec<Vec<u8>> = vec![];
    // The bytes of the strings of the pieces that have no buffer of their own.
    let mut rest = 0;
    for (i, piece) in pieces.iter().enumerate() {
        let start = piece.start as usize;
        let end = pieces
            .get(i + 1)
            .map_or(order.len(), |next| next.start as usize);
        let mut bytes = 0;
        // The buffer and offset after the last string of the piece so far.
        let mut last = 0;
        for (row, &source_row) in order[start..end].iter().enumerate() {
            let place = places.get_mut(source_row as usize)?;
            // Each row of the input is placed once.
            let mut ordered = place.piece == u32::MAX;
            *place = Place {
                piece: i as u32,
                row: (start + row) as u32,
            };
            let view = source[source_row as usize];
            let len = view as u32;
            if len > MAX_INLINE_VIEW_LEN && nulls.is_none_or(|n| n.is_valid(source_row as usize)) {
                let view = ByteView::from(view);
                let at = (view.buffer_index as u64) << 32 | view.offset as u64;
                ordered &= at >= last;
                last = at + len as u64;
                bytes += len as usize;
            }
            if !ordered {
                return None;
            }
        }
        if bytes >= OWN_BUFFER_BYTES {
            cursors.push(Cursor::Own(own.len() as u32));
            own.push(Vec::with_capacity(bytes));
        } else {
            cursors.push(Cursor::Shared(rest as u32));
            rest += bytes;
        }
    }
    let owned = own.len() as u32;
    let mut shared = vec![0u8; rest];
    let mut views = vec![0u128; order.len()];
    for (row, (&view, place)) in source.iter().zip(&places).enumerate() {
        if view as u32 <= MAX_INLINE_VIEW_LEN {
            views[place.row as usize] = view;
        } else if nulls.is_none_or(|nulls| nulls.is_valid(row)) {
            let mut view = ByteView::from(view);
            let from = view.offset as usize;
            let len = view.length as usize;
            let string = &buffers[view.buffer_index as usize][from..from + len];
            match &mut cursors[place.piece as usize] {
                Cursor::Own(buffer) => {
                    let data = &mut own[*buffer as usize];
                    view.buffer_index = *buffer;
                    view.offset = data.len() as u32;
                    data.extend_from_slice(string);
                }
                Cursor::Shared(offset) => {
                    let to = *offset as usize;
                    shared[to..to + len].copy_from_slice(string);
                    view.buffer_index = owned;
                    view.offset = *offset;
                    *offset += len as u32;
                }
            }
            views[place.row as usize] = view.as_u128();
        }
    }
    let nulls = nulls.map(|nulls| {
        NullBuffer::new(BooleanBuffer::collect_bool(order.len(), |row| {
            nulls.is_valid(order[row] as usize)
        }))
    });
    let mut own: Vec<Buffer> = own.into_iter().map(Buffer::from_vec).collect();
    if !shared.is_empty() {
        own.push(Buffer::from_vec(shared));
    }
    // SAFETY: Each view is an inline view of a valid array, the view of a null row, or
    // the view of a whole string of a valid array, with its length and prefix, in the
    // buffer and at the offset that the string was copied to.
    let array =
        unsafe { GenericByteViewArray::<T>::new_unchecked(views.into(), own.into(), nulls) };
    Some((Arc::new(array), owned))
}

/// Copies the views of a column, and each of the strings that the views of a piece point
/// at once. A piece with enough strings has them in a buffer of its own, which an output
/// batch with all the rows of the piece keeps rather than copies. The strings of the
/// other pieces follow in one buffer. Returns the array and the number of buffers that
/// are of one piece.
fn split_views<T: ByteViewType>(column: &ArrayRef, pieces: &[Piece]) -> (ArrayRef, u32) {
    let array = column.as_byte_view::<T>();
    let buffers = array.data_buffers();
    let mut views: Vec<u128> = Vec::with_capacity(array.len());
    let mut own: Vec<Buffer> = vec![];
    // The strings of the pieces without a buffer of their own, and where the views and
    // the strings of each of these pieces start.
    let mut rest: Vec<&[u8]> = vec![];
    let mut rest_bytes = 0;
    let mut rest_pieces: Vec<(Range<usize>, usize)> = vec![];
    // Where each string of a piece goes among its strings, by the piece and the address
    // and length of the string.
    let mut offsets: HashMap<(usize, usize, u32), usize> = HashMap::with_capacity(array.len());
    let mut strings: Vec<&[u8]> = vec![];
    for (i, piece) in pieces.iter().enumerate() {
        let start = piece.start as usize;
        let end = pieces
            .get(i + 1)
            .map_or(array.len(), |next| next.start as usize);
        strings.clear();
        let mut bytes = 0;
        for (row, &view) in array.views()[start..end].iter().enumerate() {
            if view as u32 <= MAX_INLINE_VIEW_LEN {
                views.push(view);
            } else if array.is_null(start + row) {
                views.push(0);
            } else {
                let mut view = ByteView::from(view);
                let from = view.offset as usize;
                let string =
                    &buffers[view.buffer_index as usize][from..from + view.length as usize];
                let offset = *offsets
                    .entry((i, string.as_ptr() as usize, view.length))
                    .or_insert_with(|| {
                        strings.push(string);
                        bytes += string.len();
                        bytes - string.len()
                    });
                view.buffer_index = own.len() as u32;
                view.offset = offset as u32;
                views.push(view.as_u128());
            }
        }
        if bytes >= OWN_BUFFER_BYTES {
            own.push(Buffer::from_vec(strings.concat()));
        } else if bytes > 0 {
            rest_pieces.push((start..end, rest_bytes));
            rest_bytes += bytes;
            rest.append(&mut strings);
        }
    }
    let owned = own.len() as u32;
    if rest_bytes > 0 {
        for (piece, base) in rest_pieces {
            for view in &mut views[piece] {
                if *view as u32 > MAX_INLINE_VIEW_LEN {
                    let mut moved = ByteView::from(*view);
                    moved.buffer_index = owned;
                    moved.offset += base as u32;
                    *view = moved.as_u128();
                }
            }
        }
        own.push(Buffer::from_vec(rest.concat()));
    }
    let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
    let nulls = nulls.map(|nulls| {
        let mut builder = BooleanBufferBuilder::new(array.len());
        builder.append_buffer(nulls.inner());
        NullBuffer::new(builder.finish())
    });
    // SAFETY: Each view is an inline view of a valid array, the view of a null row, or
    // the view of a whole string of a valid array, with its length and prefix, in the
    // buffer and at the offset that the string was copied to.
    let array =
        unsafe { GenericByteViewArray::<T>::new_unchecked(views.into(), own.into(), nulls) };
    (Arc::new(array), owned)
}

/// Returns whether an array, or an array nested in it, has more than twice the view
/// strings, dictionary values, or list view values that its rows use.
fn sparse(data: &ArrayData) -> bool {
    let values = |used: usize| data.child_data()[0].len() > 2 * used;
    let here = match data.data_type() {
        DataType::Utf8View | DataType::BinaryView => {
            allocated_bytes(&data.buffers()[1..]) > 2 * referenced_bytes(data)
        }
        DataType::Dictionary(_, _) => values(data.len()),
        DataType::ListView(_) => values(
            data.buffer::<i32>(1)[..data.len()]
                .iter()
                .map(|size| *size as usize)
                .sum(),
        ),
        DataType::LargeListView(_) => values(
            data.buffer::<i64>(1)[..data.len()]
                .iter()
                .map(|size| *size as usize)
                .sum(),
        ),
        _ => false,
    };
    here || data.child_data().iter().any(sparse)
}

/// Returns the bytes of the strings of a view array that are too long to inline,
/// counted for every view.
fn referenced_bytes(data: &ArrayData) -> usize {
    data.buffer::<u128>(0)[..data.len()]
        .iter()
        .map(|&view| view as u32)
        .filter(|&len| len > MAX_INLINE_VIEW_LEN)
        .map(|len| len as usize)
        .sum()
}

/// Returns the bytes of the buffers of an array whose rows have been taken from an input
/// batch, counting once the buffers that it shares with other held batches.
///
/// `take` leaves the strings of views and the values of dictionaries and list views in
/// the buffers of its input. `inside` is set for an array inside such values.
fn array_bytes(data: &ArrayData, inside: bool, shared: &mut HashSet<usize>) -> usize {
    let views = matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView);
    let mut bytes = 0;
    let nulls = data.nulls().map(|nulls| nulls.buffer());
    for (i, buffer) in data.buffers().iter().chain(nulls).enumerate() {
        // The nulls come after the buffers, and a view array has them of its own.
        let strings = views && (1..data.buffers().len()).contains(&i);
        bytes += if inside || strings {
            distinct_bytes(shared, buffer)
        } else {
            buffer.capacity()
        };
    }
    let inside = inside
        || matches!(
            data.data_type(),
            DataType::Dictionary(_, _) | DataType::ListView(_) | DataType::LargeListView(_)
        );
    for child in data.child_data() {
        bytes += array_bytes(child, inside, shared);
    }
    bytes
}

/// Copies a column of a whole input batch if it holds much more memory than its rows
/// need, as a slice of a larger batch does, and compacts it otherwise.
fn compact(column: &ArrayRef) -> Result<ArrayRef> {
    if oversized(column) {
        copy(column)
    } else {
        trim(Arc::clone(column), false)
    }
}

/// Returns the column of the held batch that a run is in.
fn run_column<'a>(held: &'a [Held], column: usize, run: &Run) -> &'a ArrayRef {
    &held[run.batch as usize].columns[column]
}

/// Returns the validity of the rows of the runs, if any of them is null.
fn copy_nulls(held: &[Held], column: usize, runs: &[Run], rows: usize) -> Option<NullBuffer> {
    if runs
        .iter()
        .all(|run| run_column(held, column, run).null_count() == 0)
    {
        return None;
    }
    let mut builder = BooleanBufferBuilder::new(rows);
    for run in runs {
        match run_column(held, column, run).nulls() {
            Some(nulls) => {
                builder.append_buffer(&nulls.inner().slice(run.start as usize, run.rows as usize))
            }
            None => builder.append_n(run.rows as usize, true),
        }
    }
    Some(NullBuffer::new(builder.finish()))
}

fn copy_primitives<T: ArrowPrimitiveType>(
    held: &[Held],
    column: usize,
    runs: &[Run],
    rows: usize,
) -> Result<ArrayRef> {
    let mut values: Vec<T::Native> = Vec::with_capacity(rows);
    for run in runs {
        let array = run_column(held, column, run).as_primitive::<T>();
        values.extend_from_slice(&array.values()[run.range()]);
    }
    let nulls = copy_nulls(held, column, runs, rows);
    let data_type = run_column(held, column, &runs[0]).data_type().clone();
    let array = PrimitiveArray::<T>::try_new(values.into(), nulls)?;
    Ok(Arc::new(array.with_data_type(data_type)))
}

/// Returns how many of the first data buffers of the column of a run have only strings
/// of the run, all of which the run has.
fn own_buffers(held: &[Held], column: usize, run: &Run) -> u32 {
    let own = &held[run.batch as usize].own;
    match run.cut {
        true => 0,
        false => own.get(column).copied().unwrap_or(0),
    }
}

/// Copies the views of the runs, and the strings they point at into one new buffer. The
/// strings of a run that are in a buffer of its own stay there, and the buffer is kept.
fn copy_views<T: ByteViewType>(
    held: &[Held],
    column: usize,
    runs: &[Run],
    rows: usize,
) -> Result<ArrayRef> {
    let mut bytes = 0;
    for run in runs {
        let array = run_column(held, column, run).as_byte_view::<T>();
        let own = own_buffers(held, column, run);
        let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
        for (i, &view) in array.views()[run.range()].iter().enumerate() {
            let len = view as u32;
            let valid = nulls.is_none_or(|nulls| nulls.is_valid(run.start as usize + i));
            if len > MAX_INLINE_VIEW_LEN && valid && ByteView::from(view).buffer_index >= own {
                bytes += len as usize;
            }
        }
    }
    if bytes > i32::MAX as usize {
        // The strings do not fit in one buffer.
        return concat_runs(held, column, runs, None);
    }
    let mut views: Vec<u128> = Vec::with_capacity(rows);
    let mut data: Vec<u8> = Vec::with_capacity(bytes);
    // The copied strings are in the first buffer, if there are any.
    let mut kept: Vec<Buffer> = vec![];
    let first_kept = u32::from(bytes > 0);
    for run in runs {
        let array = run_column(held, column, run).as_byte_view::<T>();
        let buffers = array.data_buffers();
        let own = own_buffers(held, column, run);
        // The strings of a run that have a buffer of its own are all in the same one.
        let mut kept_index = None;
        // Strings that follow each other are copied at once, to here.
        let range = match own {
            0 => adjacent_strings(array, run),
            _ => None,
        };
        let moved = data.len();
        if let Some((buffer, range)) = &range {
            data.extend_from_slice(&buffers[*buffer as usize][range.clone()]);
        }
        for (i, &view) in array.views()[run.range()].iter().enumerate() {
            if view as u32 <= MAX_INLINE_VIEW_LEN {
                views.push(view);
            } else if array.is_null(run.start as usize + i) {
                views.push(0);
            } else {
                let mut view = ByteView::from(view);
                if view.buffer_index < own {
                    view.buffer_index = *kept_index.get_or_insert_with(|| {
                        kept.push(buffers[view.buffer_index as usize].clone());
                        first_kept + kept.len() as u32 - 1
                    });
                } else if let Some((_, range)) = &range {
                    view.buffer_index = 0;
                    view.offset = (moved + view.offset as usize - range.start) as u32;
                } else {
                    let start = view.offset as usize;
                    let end = start + view.length as usize;
                    let string = &buffers[view.buffer_index as usize][start..end];
                    view.buffer_index = 0;
                    view.offset = data.len() as u32;
                    data.extend_from_slice(string);
                }
                views.push(view.as_u128());
            }
        }
    }
    let nulls = copy_nulls(held, column, runs, rows);
    Ok(view_array::<T>(views, data, kept, nulls))
}

/// Returns the buffer and the range of the strings of a run if each of them starts where
/// the one before it ends, as the strings that were copied when their batch arrived do.
fn adjacent_strings<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    run: &Run,
) -> Option<(u32, Range<usize>)> {
    let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
    let mut strings: Option<(u32, Range<usize>)> = None;
    for (i, &view) in array.views()[run.range()].iter().enumerate() {
        let valid = nulls.is_none_or(|nulls| nulls.is_valid(run.start as usize + i));
        if view as u32 > MAX_INLINE_VIEW_LEN && valid {
            let view = ByteView::from(view);
            let start = view.offset as usize;
            let (buffer, range) = strings.get_or_insert((view.buffer_index, start..start));
            if view.buffer_index != *buffer || start != range.end {
                return None;
            }
            range.end += view.length as usize;
        }
    }
    strings
}

/// Copies the views of the runs, and each of the strings they point at once, into one
/// new buffer.
fn copy_shared_views<T: ByteViewType>(
    held: &[Held],
    column: usize,
    runs: &[Run],
    rows: usize,
) -> Result<ArrayRef> {
    let mut views: Vec<u128> = Vec::with_capacity(rows);
    // Where each string goes in the new buffer, by its address and length.
    let mut offsets: HashMap<(usize, u32), usize> = HashMap::with_capacity(rows);
    let mut strings: Vec<&[u8]> = vec![];
    let mut bytes = 0;
    for run in runs {
        let array = run_column(held, column, run).as_byte_view::<T>();
        let buffers = array.data_buffers();
        for (i, &view) in array.views()[run.range()].iter().enumerate() {
            if view as u32 <= MAX_INLINE_VIEW_LEN {
                views.push(view);
            } else if array.is_null(run.start as usize + i) {
                views.push(0);
            } else {
                let mut view = ByteView::from(view);
                let start = view.offset as usize;
                let end = start + view.length as usize;
                let string = &buffers[view.buffer_index as usize][start..end];
                let offset = *offsets
                    .entry((string.as_ptr() as usize, view.length))
                    .or_insert_with(|| {
                        strings.push(string);
                        bytes += string.len();
                        bytes - string.len()
                    });
                view.buffer_index = 0;
                view.offset = offset as u32;
                views.push(view.as_u128());
            }
        }
    }
    if bytes > i32::MAX as usize {
        // The strings do not fit in one buffer.
        return concat_runs(held, column, runs, None);
    }
    let mut data: Vec<u8> = Vec::with_capacity(bytes);
    for string in strings {
        data.extend_from_slice(string);
    }
    let nulls = copy_nulls(held, column, runs, rows);
    Ok(view_array::<T>(views, data, vec![], nulls))
}

/// Makes a view array of views that point at the strings in `data`, which is its first
/// buffer unless it is empty, and in the `kept` buffers after it.
fn view_array<T: ByteViewType>(
    views: Vec<u128>,
    data: Vec<u8>,
    kept: Vec<Buffer>,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let mut buffers = kept;
    if !data.is_empty() {
        buffers.insert(0, Buffer::from_vec(data));
    }
    // SAFETY: Each view is an inline view of a valid array, the view of a null row, or
    // the view of a whole string of a valid array, with its length and prefix, either at
    // the offset that the string was copied to in `data` or where it was in a buffer of
    // that array that is kept.
    Arc::new(unsafe {
        GenericByteViewArray::<T>::new_unchecked(views.into(), buffers.into(), nulls)
    })
}

/// Copies the rows of the runs of a column of any type into one array, from the pieces
/// of the runs if they have been made already.
fn concat_runs(
    held: &[Held],
    column: usize,
    runs: &[Run],
    pieces: Option<Vec<ArrayRef>>,
) -> Result<ArrayRef> {
    let pieces = pieces.unwrap_or_else(|| {
        runs.iter()
            .map(|run| run_column(held, column, run).slice(run.start as usize, run.rows as usize))
            .collect()
    });
    if let [piece] = pieces.as_slice() {
        // `concat` returns a single array as it is, still sharing its buffers.
        return copy(piece);
    }
    let pieces: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
    trim(concat(&pieces)?, false)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        Int64Array, StringViewArray, StringViewBuilder, make_view, new_null_array,
    };
    use datafusion::arrow::datatypes::{Field, Fields, UnionFields, UnionMode};
    use datafusion::arrow::ipc::reader::StreamReader;
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::common::DataFusionError;

    use super::*;

    /// The bytes of each string of `shared_views`.
    const STRING_BYTES: usize = 200;

    /// Returns a buffer much larger than the strings that the batches of a test use.
    fn shared_buffer() -> Buffer {
        Buffer::from_vec(
            (0..1 << 20)
                .map(|i| b'a' + (i % 26) as u8)
                .collect::<Vec<u8>>(),
        )
    }

    /// Returns a batch of a view column whose strings are in the shared buffer from
    /// `STRING_BYTES * string(row)` on.
    fn shared_views(
        shared: &Buffer,
        rows: usize,
        string: impl Fn(usize) -> usize,
    ) -> Result<RecordBatch> {
        let views: Vec<u128> = (0..rows)
            .map(|row| {
                let offset = STRING_BYTES * string(row);
                make_view(&shared[offset..offset + STRING_BYTES], 0, offset as u32)
            })
            .collect();
        let s = StringViewArray::try_new(views.into(), vec![shared.clone()], None)?;
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8View, false)]);
        Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(s)])?)
    }

    /// Holds `batches` batches of `rows` rows each, dealt to the first two of six
    /// channels in turn, and returns the held bytes and what a flush writes.
    fn deal_shared_views(
        batches: usize,
        rows: usize,
        batch_size: usize,
        string: impl Fn(usize) -> usize + Copy,
    ) -> Result<(usize, Buffer, Vec<Round>)> {
        let shared = shared_buffer();
        let schema = shared_views(&shared, 0, string)?.schema();
        let mut buffer = ShuffleBuffer::try_new(&schema, 6, batch_size, usize::MAX, false)?;
        let mut channels = vec![vec![]; 6];
        for batch in 0..batches {
            for row in 0..rows as u32 {
                channels[row as usize % 2].push(row);
            }
            let batch = shared_views(&shared, rows, |row| string(batch * rows + row))?;
            assert!(buffer.push_rows(batch, &mut channels, false)?.is_empty());
        }
        let pieces = batches * 2 * size_of::<Piece>();
        let batches: usize = buffer.held.iter().map(Held::bytes).sum();
        Ok((
            buffer.held_bytes() - pieces - batches,
            shared,
            buffer.flush()?,
        ))
    }

    /// Returns the data buffers of the view column of each batch in the rounds, after
    /// checking that no two of them, nor any of them and `shared`, have memory in common
    /// and that each batch holds the bytes of its strings and no more.
    fn owned_strings(shared: &Buffer, rounds: &[Round]) -> Vec<Vec<Buffer>> {
        let batches = rounds.iter().flatten();
        let buffers: Vec<Vec<Buffer>> = batches
            .map(|(_, batch)| {
                let buffers = batch.column(0).as_string_view().data_buffers().to_vec();
                let bytes: usize = buffers.iter().map(Buffer::capacity).sum();
                assert_eq!(bytes, batch.num_rows() * STRING_BYTES);
                buffers
            })
            .collect();
        let range = |buffer: &Buffer| {
            let start = buffer.as_ptr() as usize;
            start..start + buffer.len()
        };
        let mut ranges: Vec<_> = buffers
            .iter()
            .flatten()
            .chain([shared])
            .map(range)
            .collect();
        ranges.sort_unstable_by_key(|range| range.start);
        assert!(ranges.windows(2).all(|pair| pair[0].end <= pair[1].start));
        buffers
    }

    /// Returns the strings of the view column of the batches of a channel.
    fn channel_strings(rounds: &[Round], channel: usize) -> Vec<String> {
        let batches = rounds.iter().flatten().filter(|(c, _)| *c == channel);
        batches
            .flat_map(|(_, batch)| batch.column(0).as_string_view().iter().flatten())
            .map(str::to_string)
            .collect()
    }

    /// Returns the strings that `deal_shared_views` gives a channel.
    fn dealt_strings(
        shared: &Buffer,
        rows: usize,
        channel: usize,
        string: impl Fn(usize) -> usize,
    ) -> Vec<String> {
        (0..rows)
            .filter(|row| row % 2 == channel)
            .map(|row| {
                let offset = STRING_BYTES * string(row);
                String::from_utf8_lossy(&shared[offset..offset + STRING_BYTES]).into_owned()
            })
            .collect()
    }

    /// The strings of a piece that are few in a large buffer are copied when the piece
    /// arrives, into a buffer that the batch of its channel keeps.
    #[test]
    fn sparse_view_strings_are_copied_once() -> Result<()> {
        let (held, shared, rounds) = deal_shared_views(3, 256, 8192, |row| row)?;
        // The views and the strings of each batch.
        assert_eq!(held, 3 * 256 * (16 + STRING_BYTES));
        assert_eq!(rounds.len(), 1);
        for buffers in owned_strings(&shared, &rounds) {
            // One buffer for each of the three pieces of the channel.
            let bytes: Vec<usize> = buffers.iter().map(Buffer::len).collect();
            assert_eq!(bytes, vec![128 * STRING_BYTES; 3]);
        }
        for channel in 0..2 {
            let expected = dealt_strings(&shared, 3 * 256, channel, |row| row);
            assert_eq!(channel_strings(&rounds, channel), expected);
        }
        Ok(())
    }

    /// A batch of some of the rows of a piece has a copy of their strings, so that the
    /// batches of a channel that has more rows than the batch size share no buffers.
    #[test]
    fn sparse_view_strings_of_split_output_are_not_shared() -> Result<()> {
        let (_, shared, rounds) = deal_shared_views(7, 80, 100, |row| row)?;
        // Each channel has 7 pieces of 40 rows, written as batches of 94, 94 and 92 rows.
        assert_eq!(rounds.len(), 3);
        let buffers = owned_strings(&shared, &rounds);
        let bytes: Vec<Vec<usize>> = buffers
            .iter()
            .map(|buffers| {
                buffers
                    .iter()
                    .map(|buffer| buffer.len() / STRING_BYTES)
                    .collect()
            })
            .collect();
        // The copied strings of the pieces that are cut, then the pieces that are whole.
        assert_eq!(bytes[0], [14, 40, 40]);
        assert_eq!(bytes[2], [26 + 28, 40]);
        assert_eq!(bytes[4], [12, 40, 40]);
        for channel in 0..2 {
            let expected = dealt_strings(&shared, 7 * 80, channel, |row| row);
            assert_eq!(channel_strings(&rounds, channel), expected);
        }
        Ok(())
    }

    /// Pieces with few strings have no buffers of their own, so that a batch does not
    /// keep many small buffers.
    #[test]
    fn sparse_view_strings_of_small_pieces_are_in_one_buffer() -> Result<()> {
        // Each piece has 20 strings, less than `OWN_BUFFER_BYTES`.
        let (held, shared, rounds) = deal_shared_views(50, 40, 8192, |row| row)?;
        assert_eq!(held, 50 * 40 * (16 + STRING_BYTES));
        for buffers in owned_strings(&shared, &rounds) {
            assert_eq!(buffers.len(), 1);
        }
        for channel in 0..2 {
            let expected = dealt_strings(&shared, 50 * 40, channel, |row| row);
            assert_eq!(channel_strings(&rounds, channel), expected);
        }
        Ok(())
    }

    /// A string that several rows of a channel share is held and written once.
    #[test]
    fn sparse_view_strings_shared_by_rows_are_in_one_buffer() -> Result<()> {
        // The rows of a channel share each string in fours.
        let string = |row: usize| row / 8 * 2 + row % 2;
        let (held, shared, rounds) = deal_shared_views(3, 256, 8192, string)?;
        assert_eq!(held, 3 * 256 * (16 + STRING_BYTES / 4));
        for (_, batch) in rounds.iter().flatten() {
            let buffers = batch.column(0).as_string_view().data_buffers();
            assert_eq!(buffers.len(), 1);
            assert_eq!(buffers[0].capacity(), batch.num_rows() / 4 * STRING_BYTES);
        }
        for channel in 0..2 {
            let expected = dealt_strings(&shared, 3 * 256, channel, string);
            assert_eq!(channel_strings(&rounds, channel), expected);
        }
        Ok(())
    }

    /// Strings that do not follow each other in the buffer are copied once as well.
    #[test]
    fn sparse_view_strings_in_any_order_are_copied_once() -> Result<()> {
        let string = |row: usize| 5000 - row;
        let (held, shared, rounds) = deal_shared_views(3, 256, 8192, string)?;
        assert_eq!(held, 3 * 256 * (16 + STRING_BYTES));
        for buffers in owned_strings(&shared, &rounds) {
            let bytes: Vec<usize> = buffers.iter().map(Buffer::len).collect();
            assert_eq!(bytes, vec![128 * STRING_BYTES; 3]);
        }
        for channel in 0..2 {
            let expected = dealt_strings(&shared, 3 * 256, channel, string);
            assert_eq!(channel_strings(&rounds, channel), expected);
        }
        Ok(())
    }

    /// Returns a column of views with nulls, short strings, and long strings that are few
    /// in two large buffers and follow each other there.
    fn mixed_views(rows: usize) -> Result<ArrayRef> {
        let buffers = vec![shared_buffer(), shared_buffer()];
        let views: Vec<u128> = (0..rows)
            .map(|row| {
                let buffer = usize::from(row >= rows / 2);
                let offset = STRING_BYTES * row;
                let len = if row % 5 == 1 {
                    7
                } else {
                    STRING_BYTES - row % 3
                };
                let string = &buffers[buffer][offset..offset + len];
                make_view(string, buffer as u32, offset as u32)
            })
            .collect();
        let nulls = NullBuffer::from((0..rows).map(|row| row % 5 != 0).collect::<Vec<_>>());
        let array = StringViewArray::try_new(views.into(), buffers, Some(nulls))?;
        Ok(Arc::new(array))
    }

    /// Returns the channel of a row of `mixed_views`: two channels with many strings, one
    /// with few, and one with short strings only.
    fn mixed_channel(row: usize) -> usize {
        match row % 50 {
            2 => 2,
            1 => 3,
            _ => row % 2,
        }
    }

    /// Returns the rows of each of four channels, the order that has them one channel
    /// after the other, and the pieces of the order.
    fn mixed_order(rows: usize) -> (Vec<Vec<u32>>, UInt32Array, Vec<Piece>) {
        let mut channels = vec![vec![]; 4];
        for row in 0..rows {
            channels[mixed_channel(row)].push(row as u32);
        }
        let mut order = vec![];
        let mut pieces = vec![];
        for (channel, rows) in channels.iter().enumerate() {
            pieces.push(Piece {
                channel: channel as u32,
                start: order.len() as u32,
            });
            order.extend_from_slice(rows);
        }
        (channels, UInt32Array::from(order), pieces)
    }

    fn placed_views<T: ByteViewType>(
        column: &ArrayRef,
        order: &UInt32Array,
        pieces: &[Piece],
    ) -> Result<(ArrayRef, u32)> {
        place_views::<T>(column, order, pieces)
            .ok_or_else(|| DataFusionError::Internal("the strings were not placed".to_string()))
    }

    /// Placing the strings of a column in the order of its rows gives what taking the
    /// rows in the order of the pieces and then copying the strings of each piece does.
    #[test]
    fn views_are_placed_as_they_are_taken_and_split() -> Result<()> {
        let column = mixed_views(600)?;
        let (_, order, pieces) = mixed_order(600);
        let (placed, own) = placed_views::<StringViewType>(&column, &order, &pieces)?;
        let taken = take(&column, &order, None)?;
        let (split, split_own) = split_views::<StringViewType>(&taken, &pieces);
        // The two channels with many strings have a buffer each.
        assert_eq!((own, split_own), (2, 2));
        assert_eq!(placed.to_data(), split.to_data());
        let placed = placed.as_string_view();
        let split = split.as_string_view();
        assert_eq!(placed.views(), split.views());
        assert_eq!(placed.data_buffers(), split.data_buffers());
        assert_eq!(placed.data_buffers().len(), 3);
        let binary = column.as_string_view().clone().to_binary_view();
        let column: ArrayRef = Arc::new(binary);
        let (placed, own) = placed_views::<BinaryViewType>(&column, &order, &pieces)?;
        assert_eq!(own, 2);
        assert_eq!(placed.as_binary_view().views(), split.views());
        assert_eq!(placed.as_binary_view().data_buffers(), split.data_buffers());
        Ok(())
    }

    /// Strings are not placed in the order of the rows if rows of a piece share a string
    /// or have their strings in another order, nor if a row is not in the order once.
    #[test]
    fn views_are_not_placed_unless_each_string_is_copied_once() -> Result<()> {
        let shared = shared_buffer();
        let piece = [Piece {
            channel: 0,
            start: 0,
        }];
        let place = |string: fn(usize) -> usize, order: Vec<u32>| -> Result<bool> {
            let batch = shared_views(&shared, 4, string)?;
            let order = UInt32Array::from(order);
            Ok(place_views::<StringViewType>(batch.column(0), &order, &piece).is_some())
        };
        assert!(place(|row| row, vec![0, 1, 2, 3])?);
        assert!(place(|row| 2 * row, vec![0, 1, 2, 3])?);
        // A shared string, and strings in another order than the rows.
        assert!(!place(|row| row / 2, vec![0, 1, 2, 3])?);
        assert!(!place(|row| 3 - row, vec![0, 1, 2, 3])?);
        assert!(!place(|row| row, vec![0, 2, 1, 3])?);
        // A row twice, and a row that the column does not have.
        assert!(!place(|row| row, vec![0, 1, 1, 3])?);
        assert!(!place(|row| row, vec![0, 1, 2, 4])?);
        Ok(())
    }

    /// Nulls, short strings and strings of several buffers are written to their channels,
    /// in batches that hold no more than their strings.
    #[test]
    fn sparse_view_strings_with_nulls_and_short_strings_are_written() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));
        // The three pieces of each of the first two channels have more rows than a batch.
        let mut buffer = ShuffleBuffer::try_new(&schema, 4, 500, usize::MAX, false)?;
        let mut expected: Vec<Vec<Option<String>>> = vec![vec![]; 4];
        for _ in 0..3 {
            let column = mixed_views(600)?;
            let (mut channels, _, _) = mixed_order(600);
            for (row, value) in column.as_string_view().iter().enumerate() {
                expected[mixed_channel(row)].push(value.map(str::to_string));
            }
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column])?;
            assert!(buffer.push_rows(batch, &mut channels, false)?.is_empty());
        }
        let rounds = buffer.flush()?;
        assert_eq!(rounds.len(), 2);
        let mut written: Vec<Vec<Option<String>>> = vec![vec![]; 4];
        for (channel, batch) in rounds.iter().flatten() {
            let strings = batch.column(0).as_string_view();
            written[*channel].extend(strings.iter().map(|value| value.map(str::to_string)));
            let long = strings.iter().flatten().filter(|value| value.len() > 12);
            let bytes: usize = long.map(str::len).sum();
            let held: usize = strings.data_buffers().iter().map(Buffer::capacity).sum();
            assert_eq!(held, bytes);
        }
        assert_eq!(written, expected);
        Ok(())
    }

    /// Small batches for many channels are held with bookkeeping for their rows, and none
    /// for the channels that they have no rows for.
    #[test]
    fn held_bytes_do_not_grow_with_the_channels() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mut buffer = ShuffleBuffer::try_new(&schema, 4000, 8192, usize::MAX, false)?;
        let mut rows = vec![vec![]; 4000];
        for i in 0..100u32 {
            let k = Int64Array::from_iter_values(0..4);
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k)])?;
            for (row, channel) in [i, 1000 + i, 2000 + i, 3000 + i].into_iter().enumerate() {
                rows[channel as usize].push(row as u32);
            }
            assert!(buffer.push_rows(batch, &mut rows, false)?.is_empty());
        }
        // Each batch has a buffer of 64 bytes for its rows, and 8 bytes for each channel
        // that they go to.
        let batches: usize = buffer.held.iter().map(Held::bytes).sum();
        assert!(buffer.held_bytes() <= 100 * (64 + 4 * 8) + batches);
        let rounds = buffer.flush()?;
        assert_eq!(rounds.len(), 1);
        assert_eq!(rounds[0].len(), 400);
        Ok(())
    }

    /// A whole batch of more than half the batch size is written as it is, after the
    /// batches held before it.
    #[test]
    fn large_whole_batch_is_passed_on_as_it_is() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let batch = |rows: usize| {
            let k = Int64Array::from_iter_values(0..rows as i64);
            let s = StringViewArray::from_iter_values(
                (0..rows).map(|row| format!("a string too long to inline {row}")),
            );
            // The strings are in a buffer of their size.
            let s = s.gc();
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k), Arc::new(s)])
        };
        let mut buffer = ShuffleBuffer::try_new(&schema, 2, 8, usize::MAX, true)?;
        assert!(buffer.push_batch(0, batch(2)?, false)?.is_empty());
        let large = batch(8)?;
        let rounds = buffer.push_batch(1, large.clone(), false)?;
        assert_eq!(buffer.held_bytes(), 0);
        let written: Vec<_> = rounds.into_iter().flatten().collect();
        assert_eq!(written.len(), 2);
        assert_eq!((written[0].0, written[0].1.num_rows()), (0, 2));
        assert_eq!(written[1].0, 1);
        for (column, input) in written[1].1.columns().iter().zip(large.columns()) {
            assert!(Arc::ptr_eq(column, input));
        }
        Ok(())
    }

    /// The lists of rows do not keep the memory of a batch for every channel when each
    /// batch goes to another channel, and keep their memory when the batches are alike.
    #[test]
    fn lists_of_rows_keep_the_memory_of_a_few_batches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = |rows: i64| {
            let k = Int64Array::from_iter_values(0..rows);
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k)])
        };
        let capacity = |rows: &[Vec<u32>]| rows.iter().map(Vec::capacity).sum::<usize>();

        let mut buffer = ShuffleBuffer::try_new(&schema, 1000, 8192, 1024 * 1024, false)?;
        let mut rows = vec![vec![]; 1000];
        for channel in 0..100 {
            rows[channel].extend(0..65536);
            buffer.push_rows(batch(65536)?, &mut rows, false)?;
            assert!(capacity(&rows) <= 4 * (65536 + 1000));
        }

        let mut buffer = ShuffleBuffer::try_new(&schema, 8, 8192, 1024 * 1024, false)?;
        let mut rows = vec![vec![]; 8];
        let mut lists = vec![];
        for _ in 0..100 {
            for row in 0..8192 {
                rows[row % 8].push(row as u32);
            }
            buffer.push_rows(batch(8192)?, &mut rows, false)?;
            lists.push(rows.iter().map(|rows| rows.as_ptr()).collect::<Vec<_>>());
        }
        assert!(lists.iter().all(|list| *list == lists[0]));
        Ok(())
    }

    /// Pushes batches of one row until they are written, and checks that the pointers to
    /// arrays that each batch has, of which there are at least `pointers`, do not exceed
    /// the budget. The columns have no buffers, or small ones.
    fn check_few_rows(types: Vec<DataType>, pointers: usize) -> Result<()> {
        let fields: Vec<_> = types
            .iter()
            .enumerate()
            .map(|(i, data_type)| Field::new(format!("c{i}"), data_type.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let budget = 1024 * 1024;
        let mut buffer = ShuffleBuffer::try_new(&schema, 1000, 8192, budget, false)?;
        let mut rows = vec![vec![]; 1000];
        let mut pushed = 0;
        loop {
            let columns = types
                .iter()
                .map(|data_type| new_null_array(data_type, 1))
                .collect();
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
            rows[pushed % 1000].push(0);
            pushed += 1;
            if !buffer.push_rows(batch, &mut rows, false)?.is_empty() {
                return Ok(());
            }
            assert!(
                pushed * pointers * size_of::<ArrayRef>() <= budget,
                "{types:?}"
            );
        }
    }

    /// Batches of one row are written before the memory that they hold apart from their
    /// buffers exceeds the budget, although their columns have no buffers.
    #[test]
    fn batches_of_few_rows_are_held_up_to_the_budget() -> Result<()> {
        check_few_rows(vec![DataType::Null; 50], 50)
    }

    /// The same holds for the arrays inside the columns, for every type that has them.
    #[test]
    fn batches_of_few_rows_of_nested_types_are_held_up_to_the_budget() -> Result<()> {
        let nulls: Fields = (0..50)
            .map(|i| Field::new(format!("f{i}"), DataType::Null, true))
            .collect();
        let item = || Arc::new(Field::new("item", DataType::Struct(nulls.clone()), true));
        let entries = Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Struct(nulls.clone()), true),
        ]);
        let entries = Arc::new(Field::new("entries", DataType::Struct(entries), false));
        let run_ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
        let union: UnionFields = (0..50).zip(nulls.iter().map(Arc::clone)).collect();
        for data_type in [
            DataType::Struct(nulls.clone()),
            DataType::List(item()),
            DataType::LargeList(item()),
            DataType::ListView(item()),
            DataType::LargeListView(item()),
            DataType::FixedSizeList(item(), 1),
            DataType::Map(entries, false),
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Struct(nulls.clone())),
            ),
            DataType::RunEndEncoded(run_ends, item()),
            DataType::Union(union.clone(), UnionMode::Sparse),
            DataType::Union(union, UnionMode::Dense),
        ] {
            check_few_rows(vec![data_type], 50)?;
        }
        Ok(())
    }

    /// Pushes batches of one megabyte for one of many channels until they are written,
    /// and returns how many were pushed.
    fn megabytes_held(budget: usize) -> Result<usize> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mut buffer = ShuffleBuffer::try_new(&schema, 4000, 8192, budget, false)?;
        let mut rows = vec![vec![]; 4000];
        for pushed in 1.. {
            let k = Int64Array::from_iter_values(0..131072);
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k)])?;
            rows[0].extend(0..131072);
            if !buffer.push_rows(batch, &mut rows, false)?.is_empty() {
                return Ok(pushed);
            }
        }
        unreachable!()
    }

    /// Rows are written when their memory reaches the budget.
    #[test]
    fn rows_are_held_up_to_the_budget() -> Result<()> {
        assert_eq!(megabytes_held(4 * 1024 * 1024)?, 4);
        assert_eq!(megabytes_held(32 * 1024 * 1024)?, 32);
        Ok(())
    }

    /// Without a budget, rows are written when there is nearly a batch of them for every
    /// channel.
    #[test]
    fn rows_are_held_up_to_a_batch_for_every_channel_without_a_budget() -> Result<()> {
        // 4000 channels with batches of 8192 rows, written at 7936 rows for each, in
        // batches of 131072 rows.
        assert_eq!(megabytes_held(0)?, 243);
        Ok(())
    }
    /// Returns the batch as it is read from an IPC message that it has been written to.
    fn read_from_ipc(batch: &RecordBatch) -> Result<RecordBatch> {
        let mut writer = StreamWriter::try_new(vec![], &batch.schema())?;
        writer.write(batch)?;
        let message = writer.into_inner()?;
        let reader = StreamReader::try_new(message.as_slice(), None)?;
        let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(batches[0].clone())
    }

    /// The data buffers of a view column read from an IPC message are all in the memory
    /// of the message, which is not counted once for each of them: the column is held
    /// as it is if its rows use the strings, and compacted if they use few of them.
    #[test]
    fn view_strings_read_from_ipc_are_compacted_only_if_few_are_used() -> Result<()> {
        let mut strings = StringViewBuilder::new().with_fixed_block_size(64 * 1024);
        for row in 0..1000 {
            strings.append_value(format!("{row:0>200}"));
        }
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8View, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(strings.finish())])?;
        let piece = [Piece {
            channel: 0,
            start: 0,
        }];

        let column = Arc::clone(read_from_ipc(&batch)?.column(0));
        let buffers = column.as_string_view().data_buffers();
        assert!(buffers.len() > 2);
        assert!(
            buffers
                .iter()
                .all(|buffer| buffer.data_ptr() == buffers[0].data_ptr())
        );
        let (held, _) = detach(Arc::clone(&column), &piece)?;
        assert!(Arc::ptr_eq(&held, &column));
        assert!(Arc::ptr_eq(&trim(Arc::clone(&column), false)?, &column));

        // A slice is written with all the strings of its array.
        let column = Arc::clone(read_from_ipc(&batch.slice(0, 10))?.column(0));
        assert!(column.as_string_view().data_buffers().len() > 2);
        for held in [detach(Arc::clone(&column), &piece)?.0, trim(column, false)?] {
            let buffers = held.as_string_view().data_buffers();
            let bytes: usize = buffers.iter().map(Buffer::capacity).sum();
            assert_eq!(bytes, 10 * 200);
        }
        Ok(())
    }
}
