// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result;
use object_store::path::Path;

/// A data file that a writer task has completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenFile {
    pub location: Path,
    pub size: u64,
    pub num_rows: u64,
}

/// The schema of the manifest that each writer task returns.
/// The manifest has one row per completed file.
/// A task that completed no file returns a single row with a null path.
pub fn manifest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition", DataType::UInt64, false),
        Field::new("attempt", DataType::UInt64, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("size", DataType::UInt64, false),
        Field::new("num_rows", DataType::UInt64, false),
    ]))
}

/// Builds the manifest of a writer task.
pub fn manifest_batch(
    partition: usize,
    attempt: usize,
    files: &[WrittenFile],
) -> Result<RecordBatch> {
    let rows = files.len().max(1);
    let mut paths: Vec<Option<String>> = files
        .iter()
        .map(|file| Some(file.location.to_string()))
        .collect();
    paths.resize(rows, None);
    let mut sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
    sizes.resize(rows, 0);
    let mut num_rows: Vec<u64> = files.iter().map(|file| file.num_rows).collect();
    num_rows.resize(rows, 0);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![partition as u64; rows])),
        Arc::new(UInt64Array::from(vec![attempt as u64; rows])),
        Arc::new(StringArray::from(paths)),
        Arc::new(UInt64Array::from(sizes)),
        Arc::new(UInt64Array::from(num_rows)),
    ];
    Ok(RecordBatch::try_new(manifest_schema(), columns)?)
}
