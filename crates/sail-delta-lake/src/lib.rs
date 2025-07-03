pub mod data_catalog;
pub mod delta_datafusion;
pub mod operations;
pub mod table;

// Re-export the main functions for convenience
pub use table::{
    create_delta_table_provider_with_object_store,
    open_table_with_object_store,
    open_table_with_object_store_simple,
};

// Re-export key types from delta_datafusion for public API
pub use delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
