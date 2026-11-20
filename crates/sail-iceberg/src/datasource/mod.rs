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

/// The last modified time of every Iceberg data file in a scan.
///
/// Iceberg data files are immutable, so a fixed timestamp is safe and lets the Parquet metadata
/// cache, which checks size and last_modified, reuse footers.
pub(crate) const DATA_FILE_LAST_MODIFIED: chrono::DateTime<chrono::Utc> =
    chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
