pub mod datasource;
mod kernel;
pub mod operations;
pub mod options;
pub mod physical_plan;
pub mod table;

mod column_mapping;
pub mod schema_manager;

pub use table::create_delta_provider;
