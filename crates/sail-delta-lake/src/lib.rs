// SPDX-License-Identifier: Apache-2.0

mod catalog;
mod catalog_managed;
mod change_data_feed;
mod checkpoint;
mod clone;
pub mod conversion;
pub mod datasource;
mod ddl;
pub mod deletion_vector;
mod delta_log;
pub mod error;
mod inspection;
pub mod lake_source;
pub mod logical;
pub mod options;
pub mod physical;
pub mod physical_plan;
mod procedure;
pub mod schema;
mod schema_coercion;
pub mod session_extension;
mod snapshot;
pub mod spec;
pub mod table;
mod transaction;
mod writer;

pub use lake_source::DeltaLakeSource;
pub use logical::DeltaTableSource;
pub use table::create_delta_source;

pub use crate::delta_log::{
    CommitOrBytes, LogStore, LogStoreConfig, LogStoreRef, ObjectStoreRef, StorageConfig,
};
pub use crate::snapshot::DeltaSnapshotConfig;
pub use crate::spec::{DeltaError, DeltaError as DeltaTableError, DeltaResult};
