use datafusion::catalog::Session;
use sail_common_datafusion::datasource::OptionLayer;
use sail_data_source::error::DataSourceResult;
use sail_data_source::options::ResolveOptions;
pub use sail_data_source::options::{parsers, BuildPartialOptions, PartialOptions};

pub mod gen {
    include!(concat!(env!("OUT_DIR"), "/options/iceberg.rs"));
}

impl ResolveOptions for gen::IcebergReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = gen::IcebergReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

impl ResolveOptions for gen::IcebergWriteOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = gen::IcebergWritePartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}
