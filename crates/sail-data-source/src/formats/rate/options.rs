use sail_common_datafusion::datasource::OptionLayer;

use crate::error::DataSourceResult;
use crate::options::internal::RateReadPartialOptions;
use crate::options::{BuildPartialOptions, PartialOptions, RateReadOptions};

pub fn resolve_rate_read_options(options: Vec<OptionLayer>) -> DataSourceResult<RateReadOptions> {
    let mut partial = RateReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}
