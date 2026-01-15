pub mod logical_plan;
pub mod logical_rewriter;
pub mod physical_plan;
pub mod planner;
pub mod provider;
pub mod table_source;

pub use provider::{SystemCatalogProvider, SYSTEM_CATALOG_NAME};

pub(crate) mod gen {
    pub mod catalog {
        include!(concat!(env!("OUT_DIR"), "/catalog.rs"));
        include!(concat!(env!("OUT_DIR"), "/database.rs"));
        include!(concat!(env!("OUT_DIR"), "/table.rs"));
        include!(concat!(env!("OUT_DIR"), "/row.rs"));
    }
}
