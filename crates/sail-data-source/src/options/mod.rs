mod loader;
mod serde;

pub use internal::{
    CsvReadOptions, CsvWriteOptions, JsonReadOptions, JsonWriteOptions, ParquetReadOptions,
    ParquetWriteOptions,
};
pub use loader::{load_default_options, load_options};

mod internal {
    include!(concat!(env!("OUT_DIR"), "/options/csv_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/csv_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json_write.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet_read.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet_write.rs"));
}

pub trait DataSourceOptions: for<'de> serde::Deserialize<'de> {
    const ALLOWED_KEYS: &'static [&'static str];
    const DEFAULT_VALUES: &'static [(&'static str, &'static str)];
}
