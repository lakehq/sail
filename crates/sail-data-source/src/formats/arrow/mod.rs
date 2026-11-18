// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/datasource-arrow/src/file_format.rs

use datafusion::catalog::Session;
use datafusion_common::Result;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingDataSource};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `ArrowFormat` implementation.

mod read;
mod write;

pub use read::ArrowReadFormat;
pub use write::ArrowWriteFormat;

pub type ArrowDataSource = ListingDataSource<ArrowFormatFactory>;

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
