mod loader;
mod serde;

pub use internal::{
    BinaryReadOptions, CsvReadOptions, CsvWriteOptions, DeltaReadOptions, DeltaWriteOptions,
    IcebergReadOptions, IcebergWriteOptions, JsonReadOptions, JsonWriteOptions, ParquetReadOptions,
    ParquetWriteOptions, TextReadOptions, TextWriteOptions,
};
#[cfg(test)]
pub use loader::build_options;
pub use loader::{load_default_options, load_options, merge_options};

pub(crate) mod internal {
    include!(concat!(env!("OUT_DIR"), "/options/binary_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/csv_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/csv_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/delta_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/delta_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/iceberg_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/iceberg_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/text_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/text_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/socket_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/rate_read.rs"));
}

pub trait DataSourceOptions: for<'de> serde::Deserialize<'de> {
    /// A list of allowed keys or aliases for the options.
    /// All values must be lowercased.
    const ALLOWED_KEYS: &'static [&'static str];
    /// A list of default values for the options.
    /// Each entry is a tuple of the key and the default value.
    const DEFAULT_VALUES: &'static [(&'static str, &'static str)];
}
