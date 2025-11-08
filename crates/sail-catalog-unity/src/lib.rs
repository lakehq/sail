// CHECK HERE: I don't like having to allow unwrap.
#![allow(clippy::unwrap_used)]

mod config;
mod data_type;
mod provider;

pub mod unity {
    include!(concat!(env!("OUT_DIR"), "/unity_catalog.rs"));
}

pub use provider::{UnityCatalogProvider, UNITY_CATALOG_PROP_URI};
