use datafusion::catalog::Session;
use datafusion_common::Result;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingTableFormat};

mod read;
mod write;

pub use read::AvroReadFormat;
pub use write::AvroWriteFormat;

pub type AvroTableFormat = ListingTableFormat<AvroFormatFactory>;

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
