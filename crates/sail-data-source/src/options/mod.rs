pub mod parsers;
pub mod types;

use crate::error::DataSourceResult;

pub mod gen {
    include!(concat!(env!("OUT_DIR"), "/options/socket.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/rate.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/delta.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/iceberg.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/parquet.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/csv.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/json.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/text.rs"));
    include!(concat!(env!("OUT_DIR"), "/options/binary.rs"));
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

#[cfg(test)]
pub(crate) fn option_list(
    items: &[(&str, &str)],
) -> sail_common_datafusion::datasource::OptionLayer {
    sail_common_datafusion::datasource::OptionLayer::OptionList {
        items: items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}
