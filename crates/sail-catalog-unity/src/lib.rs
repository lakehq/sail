mod config;
mod data_type;
mod provider;

pub mod unity {
    include!(concat!(env!("OUT_DIR"), "/unity_catalog.rs"));
}

pub use provider::UnityCatalogProvider;
