// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/datasource-parquet/src/file_format.rs

use datafusion::catalog::Session;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingDataSource};
use crate::options::ResolveOptions;
use crate::options::r#gen::{ParquetReadOptions, ParquetWriteOptions};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `ParquetFormat` implementation.

mod options;
mod read;
mod write;

pub use read::ParquetReadFormat;
pub use write::ParquetWriteFormat;

pub type ParquetDataSource = ListingDataSource<ParquetFormatFactory>;

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
