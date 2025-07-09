mod loader;
mod resolver;
mod serde;

pub use internal::{
    CsvReadOptions, CsvWriteOptions, DeltaReadOptions, DeltaWriteOptions, JsonReadOptions,
    JsonWriteOptions, ParquetReadOptions, ParquetWriteOptions,
};
pub use loader::{load_default_options, load_options};
pub use resolver::DataSourceOptionsResolver;

mod internal {
    include!(concat!(env!("OUT_DIR"), "/options/csv_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/csv_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/delta_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/delta_write.rs"));
}

pub trait DataSourceOptions: for<'de> serde::Deserialize<'de> {
    /// A list of allowed keys or aliases for the options.
    /// All values must be lowercased.
    const ALLOWED_KEYS: &'static [&'static str];
    /// A list of default values for the options.
    /// Each entry is a tuple of the key and the default value.
    const DEFAULT_VALUES: &'static [(&'static str, &'static str)];
}
