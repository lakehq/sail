use datafusion::catalog::Session;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingTableFormat};
use crate::options::gen::{JsonReadOptions, JsonWriteOptions};
use crate::options::ResolveOptions;

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `JsonFormat` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-json/src/file_format.rs

mod options;
mod read;
mod write;

pub use read::JsonReadFormat;
pub use write::JsonWriteFormat;

pub type JsonTableFormat = ListingTableFormat<JsonFormatFactory>;

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
