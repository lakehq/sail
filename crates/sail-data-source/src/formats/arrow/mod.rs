use datafusion::catalog::Session;
use datafusion_common::Result;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingTableFormat};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `ArrowFormat` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-arrow/src/file_format.rs

mod read;
mod write;

pub use read::ArrowReadFormat;
pub use write::ArrowWriteFormat;

pub type ArrowTableFormat = ListingTableFormat<ArrowFormatFactory>;

#[derive(Debug, Default)]
pub struct ArrowFormatFactory;

impl FormatFactory for ArrowFormatFactory {
    type Read = ArrowReadFormat;
    type Write = ArrowWriteFormat;

    fn name() -> &'static str {
        "arrow"
    }

    fn read(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Read> {
        Ok(ArrowReadFormat)
    }

    fn write(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Write> {
        Ok(ArrowWriteFormat)
    }
}
