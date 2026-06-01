mod convert;
mod data_type;
mod managed_table;
mod provider;
mod security;

pub mod hms {
    #[expect(clippy::allow_attributes)]
    mod internal {
        include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));
    }

    pub use internal::volo_gen::hive_metastore::*;
}

pub use provider::{HmsCatalogConfig, HmsCatalogProvider};
