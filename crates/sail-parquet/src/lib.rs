//! Shared Parquet I/O for Sail's file and table-format writers.
//!
//! Callers own partitioning, file names, schema adaptation, rolling and commits.
//! Custom Parquet schemas and field IDs can be supplied through
//! [`parquet::arrow::arrow_writer::ArrowWriterOptions`]; the completed footer
//! contains the statistics needed by Delta Lake and Iceberg.

mod writer;

pub use writer::{ParquetFileMetadata, ParquetObjectWriter, ParquetWriter};
