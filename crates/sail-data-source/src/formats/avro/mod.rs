// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/datasource-avro/src/file_format.rs

use datafusion::catalog::Session;
use datafusion_common::Result;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingDataSource};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `AvroFormat` implementation.

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
