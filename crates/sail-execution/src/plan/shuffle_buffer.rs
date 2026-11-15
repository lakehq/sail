//! Holds the rows of a shuffle write until there are enough of them to write, and copies
//! them into one batch per output channel.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, ArrowPrimitiveType, AsArray, BooleanBufferBuilder, ByteView,
    GenericByteViewArray, PrimitiveArray, UInt32Array, downcast_primitive,
};
use datafusion::arrow::buffer::{Buffer, NullBuffer};
use datafusion::arrow::compute::{concat, take_arrays};
use datafusion::arrow::datatypes::{
    BinaryViewType, ByteViewType, DataType, Schema, SchemaRef, StringViewType,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result;

use crate::plan::shuffle_compact::{
    contains_dictionary, contains_list_view, copy, dictionary_capacities, dictionary_sizes,
    oversized, trim,
};

macro_rules! primitive_runs {
    ($t:ty, $held:ident, $column:ident, $runs:ident, $rows:ident) => {
        copy_primitives::<$t>($held, $column, $runs, $rows)
    };
}

/// Views of strings at most this long hold them inline rather than in a data buffer.
const MAX_INLINE_VIEW_LEN: u32 = 12;

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
}

/// Rows of a held batch that are copied into one output batch.
#[derive(Clone, Copy, Default)]
struct Run {
    batch: u32,
    start: u32,
    rows: u32,
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
    /// Those of the buffers that the batches of the previous flush did not have.
    fresh: usize,
    /// The bytes of the strings too long to inline, counted for every view.
    referenced: usize,
}

/// The buffers that held batches share with each other or with their input.
#[derive(Default)]
struct SharedBuffers {
    held: HashSet<usize>,
    /// The buffers of the batches that the previous flush wrote.
    written: HashSet<usize>,
}

impl SharedBuffers {
    /// Returns whether no held batch has the buffer yet, and whether the batches of the
    /// previous flush did not have it either.
    fn add(&mut self, buffer: &Buffer) -> (bool, bool) {
        let pointer = buffer.data_ptr().as_ptr() as usize;
        let distinct = self.held.insert(pointer);
        (distinct, distinct && !self.written.contains(&pointer))
    }

    /// Returns the bytes of a buffer unless the held batches, or those of the previous
    /// flush, have it.
    fn fresh_bytes(&self, buffer: &Buffer) -> usize {
        let pointer = buffer.data_ptr().as_ptr() as usize;
        if self.held.contains(&pointer) || self.written.contains(&pointer) {
            0
        } else {
            buffer.capacity()
        }
    }

    /// Returns the bytes that a buffer adds to the held batches.
    ///
    /// A buffer that the batches of the previous flush had as well belongs to the input
    /// rather than to what is held. Counting it would write every batch on its own once
    /// such a buffer is as large as the budget.
    fn bytes(&mut self, buffer: &Buffer) -> usize {
        match self.add(buffer) {
            (_, true) => buffer.capacity(),
            _ => 0,
        }
    }
}

/// The rows of a shuffle write that have not been written yet.
///
/// Rows are held until their memory reaches the budget or there is a batch of them for
/// every channel. All channels are then written at once, each as batches that own their
/// memory, so that what is held does not depend on the number of channels.
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
    /// The bytes of the held buffers, other than the data buffers of view columns.
    bytes: usize,
    /// What each view column holds, and `None` for the columns of other types.
    views: Vec<Option<ViewBytes>>,
    shared: SharedBuffers,
    /// One counter per channel, for grouping rows and pieces by channel.
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
            budget,
            whole,
            held: vec![],
            pieces: vec![],
            rows: 0,
            bytes: 0,
            views,
            shared: SharedBuffers::default(),
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
        let (schema, columns, rows) = batch.into_parts();
        self.pieces.push(Piece {
            channel: channel as u32,
            start: 0,
        });
        let large = rows > self.batch_size / 2;
        self.push(schema, columns, rows, large, flush)
    }

    /// Holds a batch with the channel of each of its rows, and returns what is to be written.
    ///
    /// The rows are reordered by channel with one `take`, which keeps the order of the
    /// rows of each channel.
    pub(super) fn push_rows(
        &mut self,
        batch: RecordBatch,
        channels: &[u32],
        flush: bool,
    ) -> Result<Vec<Round>> {
        self.counts.clear();
        self.counts.resize(self.channels, 0);
        for &channel in channels {
            self.counts[channel as usize] += 1;
        }
        let mut start = 0;
        for (channel, count) in self.counts.iter_mut().enumerate() {
            let rows = *count;
            if rows > 0 {
                self.pieces.push(Piece {
                    channel: channel as u32,
                    start,
                });
            }
            *count = start;
            start += rows;
        }
        let mut order = vec![0u32; channels.len()];
        for (row, &channel) in channels.iter().enumerate() {
            let position = &mut self.counts[channel as usize];
            order[*position as usize] = row as u32;
            *position += 1;
        }
        let columns = take_arrays(batch.columns(), &UInt32Array::from(order), None)?
            .into_iter()
            .zip(&self.views)
            .map(|(column, views)| self.detach(column, views.is_some()))
            .collect::<Result<Vec<_>>>()?;
        self.push(batch.schema(), columns, batch.num_rows(), false, flush)
    }

    /// Compacts a reordered column that keeps much more of its input alive than its rows
    /// use, as the rows of a slice of a larger batch do.
    ///
    /// Holding such columns as they are would fill the budget with a few rows. Whether
    /// the rows use what they keep alive is only known for the strings of a view column,
    /// so other columns are compacted whenever they share a large part of the budget.
    fn detach(&self, column: ArrayRef, views: bool) -> Result<ArrayRef> {
        if column.data_type().is_primitive() {
            return Ok(column);
        }
        let data = column.to_data();
        let sparse = if views {
            let strings: usize = data.buffers()[1..].iter().map(Buffer::capacity).sum();
            strings > 2 * referenced_bytes(&data)
        } else {
            let mut shared = 0;
            array_bytes(&data, false, &mut |buffer| {
                shared += self.shared.fresh_bytes(buffer);
                0
            });
            shared >= self.budget / 8
        };
        if sparse {
            trim(column, false)
        } else {
            Ok(column)
        }
    }

    fn push(
        &mut self,
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        rows: usize,
        large: bool,
        flush: bool,
    ) -> Result<Vec<Round>> {
        let before = self.held_bytes();
        self.count(&columns);
        let first = self.held.len();
        self.held.push(Held {
            schema,
            columns,
            rows,
            pieces_end: self.pieces.len(),
        });
        self.rows += rows;
        let bytes = self.held_bytes();
        let full =
            bytes >= self.budget || self.rows >= self.channels.saturating_mul(self.batch_size);
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
        std::mem::swap(&mut self.shared.held, &mut self.shared.written);
        self.shared.held.clear();
    }

    /// Returns the bytes that the held batches keep alive, or that writing them copies
    /// if that is more.
    fn held_bytes(&self) -> usize {
        // Views into buffers that are not counted, or that are counted once for many
        // views, are counted by the strings that a flush copies for them.
        let views: usize = self
            .views
            .iter()
            .flatten()
            .map(|views| views.fresh.max(views.referenced.min(views.distinct)))
            .sum();
        self.bytes + views + self.pieces.len() * size_of::<Piece>()
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
                    let shared = &mut self.shared;
                    array_bytes(&column.to_data(), false, &mut |buffer| shared.bytes(buffer))
                };
                continue;
            };
            let data = column.to_data();
            let nulls = data.nulls().map_or(0, |nulls| nulls.buffer().capacity());
            self.bytes += data.buffers()[0].capacity() + nulls;
            for buffer in &data.buffers()[1..] {
                let (distinct, fresh) = if self.whole {
                    (true, true)
                } else {
                    self.shared.add(buffer)
                };
                if distinct {
                    views.distinct += buffer.capacity();
                }
                if fresh {
                    views.fresh += buffer.capacity();
                }
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
                };
                *position += 1;
            }
            start = held.pieces_end;
        }
        // Strings that several views point at would be copied once for each view, so
        // the strings of a view column are copied directly only if no more bytes are
        // referenced than the buffers have.
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
                    DataType::BinaryView if direct[column] => {
                        copy_views::<BinaryViewType>(held, column, runs, rows)
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
/// batch, with `count` giving the bytes of each buffer that it shares with its input.
///
/// `take` leaves the strings of views and the values of dictionaries and list views in
/// the buffers of its input. `shared` is set for an array inside such values.
fn array_bytes(data: &ArrayData, shared: bool, count: &mut dyn FnMut(&Buffer) -> usize) -> usize {
    let views = matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView);
    let mut bytes = 0;
    let nulls = data.nulls().map(|nulls| nulls.buffer());
    for (i, buffer) in data.buffers().iter().chain(nulls).enumerate() {
        // The nulls come after the buffers, and a view array has them of its own.
        let strings = views && (1..data.buffers().len()).contains(&i);
        bytes += if shared || strings {
            count(buffer)
        } else {
            buffer.capacity()
        };
    }
    let shared = shared
        || matches!(
            data.data_type(),
            DataType::Dictionary(_, _) | DataType::ListView(_) | DataType::LargeListView(_)
        );
    for child in data.child_data() {
        bytes += array_bytes(child, shared, count);
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

/// Copies the views of the runs, and the strings they point at into one new buffer.
fn copy_views<T: ByteViewType>(
    held: &[Held],
    column: usize,
    runs: &[Run],
    rows: usize,
) -> Result<ArrayRef> {
    let mut bytes = 0;
    for run in runs {
        let array = run_column(held, column, run).as_byte_view::<T>();
        let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
        for (i, &view) in array.views()[run.range()].iter().enumerate() {
            let len = view as u32;
            let valid = nulls.is_none_or(|nulls| nulls.is_valid(run.start as usize + i));
            if len > MAX_INLINE_VIEW_LEN && valid {
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
                view.buffer_index = 0;
                view.offset = data.len() as u32;
                data.extend_from_slice(string);
                views.push(view.as_u128());
            }
        }
    }
    let nulls = copy_nulls(held, column, runs, rows);
    let buffers = if data.is_empty() {
        vec![]
    } else {
        vec![Buffer::from_vec(data)]
    };
    // SAFETY: Each view is an inline view of a valid array, the view of a null row, or
    // the view of a whole string of a valid array, with its length and prefix, at the
    // offset that the string was copied to.
    let array = unsafe { GenericByteViewArray::<T>::new_unchecked(views.into(), buffers, nulls) };
    Ok(Arc::new(array))
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
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::Field;

    use super::*;

    /// Small batches for many channels are held with bookkeeping for their rows, and none
    /// for the channels that they have no rows for.
    #[test]
    fn held_bytes_do_not_grow_with_the_channels() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let mut buffer = ShuffleBuffer::try_new(&schema, 4000, 8192, usize::MAX, false)?;
        for i in 0..100u32 {
            let k = Int64Array::from_iter_values(0..4);
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k)])?;
            let channels = [i, 1000 + i, 2000 + i, 3000 + i];
            assert!(buffer.push_rows(batch, &channels, false)?.is_empty());
        }
        // Each batch has a buffer of 64 bytes for its rows, and 8 bytes for each channel
        // that they go to.
        assert!(buffer.held_bytes() <= 100 * (64 + 4 * 8));
        let rounds = buffer.flush()?;
        assert_eq!(rounds.len(), 1);
        assert_eq!(rounds[0].len(), 400);
        Ok(())
    }
}
