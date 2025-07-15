mod delta_datafusion;
pub mod delta_format;
mod kernel;
mod operations;
mod table;

pub use table::create_delta_provider;
