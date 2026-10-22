use datafusion::catalog::Session;
use datafusion_common::Result;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingDataSource};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `AvroFormat` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-avro/src/file_format.rs

mod read;
mod write;

pub use read::AvroReadFormat;
pub use write::AvroWriteFormat;

pub type AvroDataSource = ListingDataSource<AvroFormatFactory>;

#[derive(Debug, Default)]
pub struct AvroFormatFactory;

impl FormatFactory for AvroFormatFactory {
    type Read = AvroReadFormat;
    type Write = AvroWriteFormat;

    fn name() -> &'static str {
        "avro"
    }

    fn read(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Read> {
        Ok(AvroReadFormat)
    }

    fn write(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Write> {
        Ok(AvroWriteFormat)
    }
}
