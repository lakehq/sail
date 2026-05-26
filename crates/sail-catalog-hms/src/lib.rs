mod convert;
mod data_type;
mod provider;
mod security;

#[expect(clippy::allow_attributes)]
pub mod hms {
    include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));

    pub use self::volo_gen::hms::*;
}

pub use provider::{HmsCatalogConfig, HmsCatalogProvider};
