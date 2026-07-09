use datafusion::catalog::Session;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingTableFormat};
use crate::options::ResolveOptions;
use crate::options::r#gen::{ParquetReadOptions, ParquetWriteOptions};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `ParquetFormat` implementation.
// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-parquet/src/file_format.rs

mod options;
mod read;
mod write;

pub use read::ParquetReadFormat;
pub use write::ParquetWriteFormat;

pub type ParquetTableFormat = ListingTableFormat<ParquetFormatFactory>;

#[derive(Debug, Default)]
pub struct ParquetFormatFactory;

impl FormatFactory for ParquetFormatFactory {
    type Read = ParquetReadFormat;
    type Write = ParquetWriteFormat;

    fn name() -> &'static str {
        "parquet"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let options = ParquetReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetReadFormat { options })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = ParquetWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetWriteFormat { options })
    }
}
