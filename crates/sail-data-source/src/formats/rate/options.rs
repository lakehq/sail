use datafusion::catalog::Session;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::DataSourceResult;
use crate::options::r#gen::{RateReadOptions, RateReadPartialOptions};
use crate::options::{BuildPartialOptions, PartialOptions, ResolveOptions};

impl ResolveOptions for RateReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = RateReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}
