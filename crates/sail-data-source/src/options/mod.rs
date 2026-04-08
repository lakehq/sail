mod loader;
pub mod parsers;
mod serde;

pub use internal::{DeltaReadOptions, DeltaWriteOptions, IcebergReadOptions, IcebergWriteOptions};
#[cfg(test)]
pub use loader::{build_option_layer, build_options};
pub use loader::{load_default_options, load_options, merge_options};

use crate::error::DataSourceResult;

pub(crate) mod internal {
    include!(concat!(env!("OUT_DIR"), "/options/delta_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/delta_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/iceberg_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/iceberg_write.rs"));
}

pub mod gen {
    include!(concat!(env!("OUT_DIR"), "/options/binary.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/csv.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/socket.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/rate.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/text.rs"));
}

pub trait DataSourceOptions: for<'de> serde::Deserialize<'de> {
    /// A list of allowed keys or aliases for the options.
    /// All values must be lowercased.
    const ALLOWED_KEYS: &'static [&'static str];
    /// A list of default values for the options.
    /// Each entry is a tuple of the key and the default value.
    const DEFAULT_VALUES: &'static [(&'static str, &'static str)];
}

/// A trait for partially loaded options.
pub trait PartialOptions {
    /// The complete options type.
    type Options;

    /// Initializes the partial options with configured default values.
    fn initialize() -> Self;

    /// Merges another set of partial options into this one,
    /// overriding any values that are set in the other.
    fn merge(&mut self, other: Self);

    /// Finalizes the partial options into a complete set of options,
    /// validating that all required values are set and returning an error if not.
    fn finalize(self) -> DataSourceResult<Self::Options>;
}

/// A trait for building partial options.
pub trait BuildPartialOptions<T> {
    /// Builds the partial options from data.
    fn build_partial_options(self) -> DataSourceResult<T>;
}
