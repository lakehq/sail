mod config;
mod credential;
mod data_type;
mod provider;
mod token;

pub mod unity {
    include!(concat!(env!("OUT_DIR"), "/unity_catalog.rs"));
}

pub use provider::UnityCatalogProvider;
