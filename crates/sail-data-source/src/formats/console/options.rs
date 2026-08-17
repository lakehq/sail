use datafusion::catalog::Session;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::DataSourceResult;
use crate::options::r#gen::{ConsoleWriteOptions, ConsoleWritePartialOptions};
use crate::options::{BuildPartialOptions, PartialOptions, ResolveOptions};

impl ResolveOptions for ConsoleWriteOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = ConsoleWritePartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}
