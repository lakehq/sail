// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/datasource-json/src/file_format.rs

use datafusion::catalog::Session;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingDataSource};
use crate::options::ResolveOptions;
use crate::options::r#gen::{JsonReadOptions, JsonWriteOptions};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `JsonFormat` implementation.

mod options;
mod read;
mod write;

pub use read::JsonReadFormat;
pub use write::JsonWriteFormat;

pub type JsonDataSource = ListingDataSource<JsonFormatFactory>;

#[derive(Debug, Default)]
pub struct JsonFormatFactory;

impl FormatFactory for JsonFormatFactory {
    type Read = JsonReadFormat;
    type Write = JsonWriteFormat;

    fn name() -> &'static str {
        "json"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let options = JsonReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(JsonReadFormat { options })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = JsonWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(JsonWriteFormat { options })
    }
}
