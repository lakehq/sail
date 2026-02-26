//! Python data source support for Sail.
//!
//! This module provides the infrastructure for Python-defined data sources,
//! enabling users to implement custom data sources in Python while leveraging
//! Sail's distributed execution.
//!
//! # Architecture
//!
//! The implementation follows a trait-based abstraction for future-proofing:
//!
//! - `PythonExecutor` trait: Abstracts Python execution (in-process or subprocess)
//! - `InProcessExecutor`: MVP implementation using PyO3 directly
//! - `RemoteExecutor`: Future implementation for subprocess isolation (PR #3)
//!
//! # Components
//!
//! - `discovery`: Entry point discovery and registry
//! - `filter`: Filter pushdown conversion (DataFusion → Python)
//! - `executor`: Python execution abstraction
//! - `stream`: RecordBatch streaming with RAII cleanup
//! - `arrow_utils`: Arrow ↔ Python conversion utilities
pub mod arrow_utils;
mod discovery;
mod error;
mod exec;
mod executor;
mod filter;
#[allow(clippy::module_inception)]
mod python_datasource;
mod python_table_provider;
mod stream;
mod table_format;

// Public exports - always available
// Public exports - require python feature
pub use discovery::{
    discover_data_sources, validate_datasource_class, DataSourceEntry, PythonDataSourceRegistry,
    DATA_SOURCE_REGISTRY,
};
pub use error::PythonDataSourceError;
pub use exec::PythonDataSourceExec;
pub use executor::{InProcessExecutor, InputPartition, PythonExecutor};
pub use filter::{exprs_to_python_filters, ColumnPath, FilterValue, PythonFilter};
pub use python_datasource::PythonDataSource;
pub use python_table_provider::PythonTableProvider;
pub use stream::{PythonDataSourceStream, RowBatchCollector, DEFAULT_BATCH_SIZE};
pub use table_format::PythonTableFormat;
