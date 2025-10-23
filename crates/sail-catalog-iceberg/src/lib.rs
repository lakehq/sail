#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::needless_return)]
#![allow(clippy::empty_docs)]

pub mod apis;
mod models;
mod provider;

pub use provider::{
    IcebergRestCatalogProvider, REST_CATALOG_PROP_PREFIX, REST_CATALOG_PROP_URI,
    REST_CATALOG_PROP_WAREHOUSE,
};
