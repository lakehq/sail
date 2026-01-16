pub mod logical_plan;
pub mod logical_rewriter;
pub mod physical_plan;
pub mod planner;
pub mod provider;
pub mod querier;
pub mod table_source;

use lazy_static::lazy_static;
pub use provider::{SystemCatalogProvider, SYSTEM_CATALOG_NAME};
use serde_arrow::schema::TracingOptions;

pub(crate) mod gen {
    pub mod catalog {
        include!(concat!(env!("OUT_DIR"), "/catalog.rs"));
        include!(concat!(env!("OUT_DIR"), "/database.rs"));
        include!(concat!(env!("OUT_DIR"), "/table.rs"));
        include!(concat!(env!("OUT_DIR"), "/row.rs"));
    }
}

lazy_static! {
    pub static ref SYSTEM_TRACING_OPTIONS: TracingOptions = TracingOptions::default()
        .sequence_as_large_list(false)
        .strings_as_large_utf8(false)
        .bytes_as_large_binary(false);
}
