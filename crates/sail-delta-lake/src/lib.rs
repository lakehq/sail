pub mod data_catalog;
pub mod delta_datafusion;
pub mod operations;
pub mod table;

// Re-export the main functions for convenience
// Re-export key types from delta_datafusion for public API
pub use delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
// Re-export transaction and kernel types
pub use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
pub use deltalake::kernel::{Action, Add, Remove};
pub use deltalake::logstore::StorageConfig;
pub use deltalake::parquet::file::properties::WriterProperties;
pub use deltalake::protocol::DeltaOperation;
// Re-export common types from deltalake
pub use deltalake::protocol::SaveMode;
pub use deltalake::DeltaTable;
pub use operations::{LoadBuilder, SailDeltaOps, WriteBuilder};
pub use table::{
    create_delta_table_provider_with_object_store, create_delta_table_with_object_store,
    open_table_with_object_store, open_table_with_object_store_simple,
};
