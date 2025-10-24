// SPDX-License-Identifier: Apache-2.0

//! Generic Python data source for Lakesail.
//!
//! This crate provides a GENERIC bridge to Python-based data sources.
//! Any Python class that returns Arrow data can be used as a Lakesail data source!
//!
//! # Architecture
//!
//! ```text
//! Lakesail (Rust)                    Python
//! ┌──────────────────────┐          ┌────────────────────┐
//! │ PythonDataSource     │          │ User's Python      │
//! │                      │  PyO3    │ Class              │
//! │ - infer_schema()     │ ──────>  │                    │
//! │ - plan_partitions()  │          │ Returns Arrow      │
//! │ - read_partition()   │ <──────  │ RecordBatches      │
//! │                      │   FFI    │                    │
//! └──────────────────────┘          └────────────────────┘
//! ```
//!
//! # Python Interface
//!
//! Python classes must implement:
//! ```python
//! class MyDataSource:
//!     def infer_schema(self, options: dict) -> pa.Schema:
//!         """Return Arrow schema for the data"""
//!
//!     def plan_partitions(self, options: dict) -> List[dict]:
//!         """Return list of partition specs (JSON-serializable dicts)"""
//!
//!     def read_partition(self, partition_spec: dict, options: dict) -> Iterator[pa.RecordBatch]:
//!         """Read one partition, yield Arrow RecordBatches"""
//! ```
//!
//! # Usage
//!
//! ```python
//! # Use existing JDBC datasource
//! spark.read.format("python")
//!   .option("python_module", "pysail.read.arrow_datasource")
//!   .option("python_class", "JDBCArrowDataSource")
//!   .option("url", "jdbc:postgresql://...")
//!   .option("dbtable", "orders")
//!   .load()
//!
//! # Or any custom datasource
//! spark.read.format("python")
//!   .option("python_module", "my_package.my_source")
//!   .option("python_class", "MyCustomDataSource")
//!   .option("custom_param", "value")
//!   .load()
//! ```

pub mod error;
pub mod exec;
pub mod format;
pub mod provider;

// Re-exports
pub use error::{PythonDataSourceError, Result};
pub use format::PythonDataSourceFormat;
pub use provider::PythonTableProvider;
