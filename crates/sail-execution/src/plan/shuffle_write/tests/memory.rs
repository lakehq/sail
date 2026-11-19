// SPDX-License-Identifier: Apache-2.0

//! Checks that a shuffle write with many output channels and few rows for each of them
//! holds memory in proportion to its input rather than to its channels and batch size.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use futures::FutureExt;

use super::probe::{arrays, key_partitioning, output_limit, probed_shuffle};
use super::{
    assert_compact, matrix_column, matrix_input, matrix_list, matrix_rows, shuffle_with, stream,
};
use crate::stream::writer::TaskStreamWriteState;

const BATCH_SIZE: usize = 8192;

/// The bytes that a write is taken to buffer before it writes every channel.
const BUDGET: usize = super::BUFFER_SIZE;

/// The inputs of these tests, by name, with the number of output channels and the batch
/// size for each.
///
/// The wide input has 50 columns of integers, floats, strings, and view strings.
pub(super) const INPUTS: [(&str, usize, usize); 3] = [
    ("narrow", 4000, BATCH_SIZE),
    ("strings", 2000, BATCH_SIZE),
    ("wide", 1000, BATCH_SIZE),
];

/// Inputs for a large batch size with a few hundred channels.
pub(super) const LARGE_BATCH_INPUTS: [(&str, usize, usize); 2] =
    [("narrow", 256, 262144), ("wide", 256, 262144)];

fn input_kinds(name: &str) -> Vec<&'static str> {
    match name {
        "narrow" => vec!["int64"],
        "strings" => vec!["utf8", "utf8"],
        _ => (1..50)
            .map(|i| {
                [
                    "int64",
                    "int64",
                    "int64",
                    "int64",
                    "float64",
                    "int32",
                    "utf8",
                    "utf8_view",
                ][i % 8]
            })
            .collect(),
    }
}

/// Returns batches with a unique Int64 key `k` and the columns of the named input.
fn few_rows_input(name: &str, batches: usize, rows: usize) -> Result<Vec<RecordBatch>> {
    let kinds = input_kinds(name);
    let mut fields = vec![Field::new("k", DataType::Int64, false)];
    for (i, kind) in kinds.iter().enumerate() {
        let data_type = match *kind {
            "int64" => DataType::Int64,
            "float64" => DataType::Float64,
            "int32" => DataType::Int32,
            "utf8" => DataType::Utf8,
            _ => DataType::Utf8View,
        };
        fields.push(Field::new(format!("c{i}"), data_type, false));
    }
    let schema = Arc::new(Schema::new(fields));
    (0..batches)
        .map(|batch| {
            let ids = batch * rows..(batch + 1) * rows;
            let keys = Int64Array::from_iter_values(ids.clone().map(|id| id as i64));
            let mut columns: Vec<ArrayRef> = vec![Arc::new(keys)];
            for (i, kind) in kinds.iter().enumerate() {
                // The strings are too long for views to hold inline.
                let text = |id: usize| format!("value-{id:014}");
                columns.push(match *kind {
                    "int64" => Arc::new(Int64Array::from_iter_values(
                        ids.clone().map(|id| (31 * id + i) as i64),
                    )),
                    "float64" => Arc::new(Float64Array::from_iter_values(
                        ids.clone().map(|id| id as f64 + 0.5),
                    )),
                    "int32" => Arc::new(Int32Array::from_iter_values(
                        ids.clone().map(|id| (id + i) as i32),
                    )),
                    "utf8" => Arc::new(StringArray::from_iter_values(ids.clone().map(text))),
                    _ => Arc::new(StringViewArray::from_iter_values(ids.clone().map(text))),
                });
            }
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        })
        .collect()
}

/// Returns the named input with eight rows for each channel on average.
///
/// Hash, range, and row round-robin partitioning spread each of eight input batches over
/// all the channels. Batch round-robin partitioning gives a whole input batch to one
/// channel, so its input is two batches of four rows for each channel.
pub(super) fn few_rows_per_channel(
    name: &str,
    scheme: &str,
    channels: usize,
) -> Result<Vec<RecordBatch>> {
    if scheme == "round_robin_batch" {
        few_rows_input(name, 2 * channels, 4)
    } else {
        few_rows_input(name, 8, channels)
    }
}

/// Shuffles each input and compares the heap that the write holds as it pulls each input
/// batch, and at most in between, with the bytes of the input pulled so far.
///
/// Returns where it holds more than they account for.
async fn few_rows_per_channel_violations(
    scheme: &str,
    unbounded: bool,
    inputs: &[(&str, usize, usize)],
) -> Result<Vec<String>> {
    let mut violations = vec![];
    for &(name, channels, batch_size) in inputs {
        let input = few_rows_per_channel(name, scheme, channels)?;
        let partitioning = key_partitioning(scheme, &input, channels)?;
        let probe = probed_shuffle(
            &input,
            partitioning,
            unbounded,
            batch_size,
            TaskStreamWriteState::Active,
            false,
        )
        .await?;
        assert!(probe.committed && !probe.aborted);
        assert_eq!(probe.written_rows, 8 * channels);
        if unbounded {
            // Rows of unbounded input are written before more input is pulled.
            assert!(
                probe
                    .samples
                    .iter()
                    .all(|sample| sample.at_write || sample.written_rows == sample.input_rows),
                "a {scheme} shuffle of unbounded input holds rows back"
            );
        }
        let found = probe.heap_violations(channels, arrays(&input[0]), batch_size);
        violations.extend(found.into_iter().map(|violation| {
            format!(
                "{scheme} shuffle (unbounded: {unbounded}), {name} input, {channels} channels, \
                 batch size {batch_size}: {violation}"
            )
        }));
    }
    Ok(violations)
}

async fn check_few_rows_per_channel(scheme: &str, unbounded: bool) -> Result<()> {
    let violations = few_rows_per_channel_violations(scheme, unbounded, &INPUTS).await?;
    assert!(
        violations.is_empty(),
        "the shuffle holds more memory than its input accounts for:\n{}",
        violations.join("\n")
    );
    Ok(())
}

async fn check_large_batch_size(unbounded: bool) -> Result<()> {
    let mut violations = vec![];
    for scheme in ["hash", "range", "round_robin_batch", "round_robin_row"] {
        violations
            .extend(few_rows_per_channel_violations(scheme, unbounded, &LARGE_BATCH_INPUTS).await?);
    }
    assert!(
        violations.is_empty(),
        "the shuffle holds more memory than its input accounts for:\n{}",
        violations.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn hash_shuffle_of_few_rows_per_channel_holds_little_memory() -> Result<()> {
    check_few_rows_per_channel("hash", false).await
}

#[tokio::test]
async fn range_shuffle_of_few_rows_per_channel_holds_little_memory() -> Result<()> {
    check_few_rows_per_channel("range", false).await
}

#[tokio::test]
async fn round_robin_shuffle_of_few_rows_per_channel_holds_little_memory() -> Result<()> {
    check_few_rows_per_channel("round_robin_batch", false).await
}

#[tokio::test]
async fn row_round_robin_shuffle_of_few_rows_per_channel_holds_little_memory() -> Result<()> {
    check_few_rows_per_channel("round_robin_row", false).await
}

#[tokio::test]
async fn hash_shuffle_of_unbounded_few_rows_per_channel_holds_little_memory() -> Result<()> {
    check_few_rows_per_channel("hash", true).await
}

#[tokio::test]
async fn range_shuffle_of_unbounded_few_rows_per_channel_holds_little_memory() -> Result<()> {
    check_few_rows_per_channel("range", true).await
}

#[tokio::test]
async fn round_robin_shuffle_of_unbounded_few_rows_per_channel_holds_little_memory() -> Result<()> {
    check_few_rows_per_channel("round_robin_batch", true).await
}

#[tokio::test]
async fn row_round_robin_shuffle_of_unbounded_few_rows_per_channel_holds_little_memory()
-> Result<()> {
    check_few_rows_per_channel("round_robin_row", true).await
}

/// A large batch size is for large inputs, and must not cost memory when a channel
/// receives a few rows.
#[tokio::test]
async fn shuffle_with_a_large_batch_size_holds_little_memory() -> Result<()> {
    check_large_batch_size(false).await
}

#[tokio::test]
async fn shuffle_of_unbounded_input_with_a_large_batch_size_holds_little_memory() -> Result<()> {
    check_large_batch_size(true).await
}

/// A channel that has received a few rows needs the same memory whatever the batch size is.
#[tokio::test]
async fn shuffle_memory_of_few_rows_per_channel_does_not_grow_with_the_batch_size() -> Result<()> {
    let channels = 1000;
    let mut failures = vec![];
    for scheme in ["hash", "range", "round_robin_batch", "round_robin_row"] {
        let input = few_rows_per_channel("narrow", scheme, channels)?;
        let mut held = vec![];
        for batch_size in [512, 8192, 262144] {
            let partitioning = key_partitioning(scheme, &input, channels)?;
            let probe = probed_shuffle(
                &input,
                partitioning,
                false,
                batch_size,
                TaskStreamWriteState::Active,
                false,
            )
            .await?;
            assert_eq!(probe.written_rows, 8 * channels);
            held.push(probe.max_held());
        }
        if held[1].max(held[2]) > 2 * held[0] + 64 * 1024 {
            failures.push(format!(
                "{scheme} shuffle holds {held:?} bytes with batches of 512, 8192, and 262144 rows"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// A consumer that falls behind holds on to what was written, which for many channels is
/// many small batches.
#[tokio::test]
async fn shuffle_output_of_few_rows_per_channel_holds_little_memory() -> Result<()> {
    let mut failures = vec![];
    for scheme in ["hash", "range", "round_robin_batch", "round_robin_row"] {
        for unbounded in [false, true] {
            for &(name, channels, batch_size) in INPUTS.iter().chain(&LARGE_BATCH_INPUTS) {
                let input = few_rows_per_channel(name, scheme, channels)?;
                let partitioning = key_partitioning(scheme, &input, channels)?;
                let probe = probed_shuffle(
                    &input,
                    partitioning,
                    unbounded,
                    batch_size,
                    TaskStreamWriteState::Active,
                    true,
                )
                .await?;
                assert_eq!(probe.written_rows, 8 * channels);
                let case = format!(
                    "scheme={scheme} unbounded={unbounded} input={name} channels={channels} \
                     batch_size={batch_size}"
                );
                // The first channel that is not compact is reported by its panic.
                let mut compact = true;
                for (channel, output) in probe.channels.iter().enumerate() {
                    let check = AssertUnwindSafe(|| assert_compact(channel, output));
                    match std::panic::catch_unwind(check) {
                        Ok(result) => result?,
                        Err(_) => {
                            compact = false;
                            break;
                        }
                    }
                }
                let input_bytes: usize = input.iter().map(|b| b.get_array_memory_size()).sum();
                let limit = output_limit(input_bytes, probe.written_batches, arrays(&input[0]));
                if !compact {
                    failures.push(format!(
                        "{case}: a channel holds batches that are not compact"
                    ));
                }
                if probe.held_at_end > limit {
                    failures.push(format!(
                        "{case}: the output holds {} bytes in {} batches for {input_bytes} bytes \
                         of input, limit {limit}",
                        probe.held_at_end, probe.written_batches
                    ));
                }
                // Hash partitioning copies every row, so the probe must have seen the copies.
                if scheme == "hash" && name == "narrow" {
                    assert!(
                        probe.held_at_end >= 16 * 8 * channels,
                        "the heap probe saw {} bytes of output, which the write must have \
                         allocated on another thread",
                        probe.held_at_end
                    );
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Each channel receives far more rows than a batch holds, so its batches are few for
/// its rows, however the write groups them, and none is above the batch size.
#[tokio::test]
async fn shuffle_output_of_many_rows_per_channel_has_few_batches() -> Result<()> {
    let channels = 4;
    let input = few_rows_input("narrow", 64, BATCH_SIZE)?;
    let task_rows = 64 * BATCH_SIZE;
    let task_bytes: usize = input.iter().map(|b| b.get_array_memory_size()).sum();
    for scheme in ["hash", "range", "round_robin_batch", "round_robin_row"] {
        let partitioning = key_partitioning(scheme, &input, channels)?;
        let probe = probed_shuffle(
            &input,
            partitioning,
            false,
            BATCH_SIZE,
            TaskStreamWriteState::Active,
            true,
        )
        .await?;
        assert_eq!(probe.written_rows, task_rows);
        for (channel, output) in probe.channels.iter().enumerate() {
            let rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
            // Twice the fewest batches that can hold the rows, and one for each time that
            // the write can write every channel: when it holds nearly a batch of rows for
            // each of them, or as many bytes as it may.
            let limit = 2 * rows.div_ceil(BATCH_SIZE)
                + task_rows.div_ceil(channels * (BATCH_SIZE - BATCH_SIZE / 32))
                + task_bytes.div_ceil(BUDGET)
                + 1;
            assert!(
                output.len() <= limit,
                "channel {channel} of a {scheme} shuffle has {} batches for {rows} rows, limit {limit}",
                output.len()
            );
            assert!(
                output.iter().all(|batch| batch.num_rows() <= BATCH_SIZE),
                "channel {channel} of a {scheme} shuffle has a batch above the batch size"
            );
        }
        let limit = 2 * (task_rows.div_ceil(BATCH_SIZE) + channels * task_bytes.div_ceil(BUDGET))
            + channels;
        assert!(
            probe.written_batches <= limit,
            "a {scheme} shuffle wrote {} batches for {task_rows} rows, limit {limit}",
            probe.written_batches
        );
    }
    Ok(())
}

/// The write stops at the first write that reports every channel closed.
#[tokio::test]
async fn shuffle_of_few_rows_per_channel_stops_when_all_channels_close() -> Result<()> {
    for scheme in ["hash", "range", "round_robin_batch", "round_robin_row"] {
        for unbounded in [false, true] {
            let (name, channels, _) = INPUTS[0];
            let input = few_rows_per_channel(name, scheme, channels)?;
            let partitioning = key_partitioning(scheme, &input, channels)?;
            let probe = probed_shuffle(
                &input,
                partitioning,
                unbounded,
                BATCH_SIZE,
                TaskStreamWriteState::Closed,
                false,
            )
            .await?;
            assert!(
                probe.writes == 1
                    && probe.aborted
                    && !probe.committed
                    && probe.pulled_after_close == 0,
                "a {scheme} shuffle (unbounded: {unbounded}) went on after its channels closed: \
                 {} writes, input pulled {} times after the first",
                probe.writes,
                probe.pulled_after_close
            );
        }
    }
    Ok(())
}

/// Returns batches of two, one, and three rows that are slices of larger batches, with a
/// key and a column of `kind`.
fn sliced_nested_input(kind: &str) -> Result<Vec<RecordBatch>> {
    if kind != "list_of_struct" {
        return matrix_input(kind, "sliced", &[2, 1, 3]);
    }
    let mut base = 0;
    let mut batches = vec![];
    for rows in [2, 1, 3] {
        let full = 4 * rows;
        let keys = Int64Array::from_iter_values((base..base + full).map(|k| k as i64));
        let values = matrix_column("struct", 2 * full, 2 * base, &|_| false)?;
        let column = matrix_list(values, full, None)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", column.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(keys), column])?;
        batches.push(batch.slice(rows, rows));
        base += full;
    }
    Ok(batches)
}

/// A slice of a map, list, or struct keeps all the values of its children, which are
/// not sliced with it.
#[tokio::test]
async fn shuffle_of_sliced_nested_input_writes_its_rows() -> Result<()> {
    let mut failures = vec![];
    for kind in [
        "map",
        "struct",
        "list_of_struct",
        "nested_list",
        "struct_of_list_view",
    ] {
        let input = sliced_nested_input(kind)?;
        for scheme in ["hash", "range", "round_robin_batch", "round_robin_row"] {
            for unbounded in [false, true] {
                let case = format!("{kind} column, {scheme} shuffle (unbounded: {unbounded})");
                let partitioning = key_partitioning(scheme, &input, 3)?;
                let state = TaskStreamWriteState::Active;
                let shuffle = shuffle_with(stream(&input, unbounded)?, partitioning, state);
                let Ok(capture) = AssertUnwindSafe(shuffle).catch_unwind().await else {
                    failures.push(format!("{case}: the write panicked"));
                    continue;
                };
                let output: Vec<RecordBatch> = capture?.channels.into_iter().flatten().collect();
                if output.is_empty() || matrix_rows(&output)? != matrix_rows(&input)? {
                    failures.push(format!("{case}: the output rows differ from the input"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
