// SPDX-License-Identifier: Apache-2.0

pub(crate) mod copy_on_write;
pub mod expressions;
pub mod imported_source;
pub(crate) mod parquet;
pub(crate) mod partition_defaults;
pub(crate) mod predicate;
pub mod pruning;
pub mod scan;
pub mod type_converter;

pub use scan::*;

/// The last modified time to use for an Iceberg data file, given the ID of the snapshot that
/// added the file.
///
/// The Parquet metadata cache reuses a footer for the same path when the size and last_modified
/// match. Iceberg data files are immutable, but a path can be reused for a new file after the old
/// one is deleted, and that happens in a later snapshot. So the ID of the snapshot that added the
/// file, stored as nanoseconds, tells the two files apart while letting every scan of a file
/// reuse its footer. Without the ID, the current time is used, which disables reuse across scans.
pub(crate) fn data_file_last_modified(
    added_snapshot_id: Option<i64>,
) -> chrono::DateTime<chrono::Utc> {
    match added_snapshot_id {
        Some(id) => chrono::TimeZone::timestamp_nanos(&chrono::Utc, id),
        None => chrono::Utc::now(),
    }
}
