use sail_common_datafusion::datasource::OptionLayer;

use crate::error::DataSourceResult;
use crate::options::gen::{SocketReadOptions, SocketReadPartialOptions};
use crate::options::{BuildPartialOptions, PartialOptions};

pub fn resolve_socket_read_options(
    options: Vec<OptionLayer>,
) -> DataSourceResult<SocketReadOptions> {
    let mut partial = SocketReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}
