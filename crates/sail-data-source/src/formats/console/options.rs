use sail_common_datafusion::datasource::OptionLayer;

use crate::error::DataSourceResult;
use crate::options::gen::{ConsoleWriteOptions, ConsoleWritePartialOptions};
use crate::options::{BuildPartialOptions, PartialOptions};

pub fn resolve_console_write_options(
    options: Vec<OptionLayer>,
) -> DataSourceResult<ConsoleWriteOptions> {
    let mut partial = ConsoleWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}
