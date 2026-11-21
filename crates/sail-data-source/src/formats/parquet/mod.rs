// SPDX-License-Identifier: Apache-2.0

use datafusion::catalog::Session;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::schema_evolution::StructFieldMatching;
use sail_common_datafusion::session::config::SessionConfigService;

use crate::listing::source::{FormatFactory, ListingDataSource};
use crate::options::ResolveOptions;
use crate::options::r#gen::{ParquetReadOptions, ParquetWriteOptions};

// Some of the code in the `read` and `write` modules is adapted from the DataFusion `ParquetFormat` implementation.

mod options;
mod read;
mod write;

pub use read::{ParquetReadFormat, parse_coerce_int96_string};
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
        let case_sensitive = ctx
            .config()
            .get_extension::<SessionConfigService>()
            .map(|service| service.get_option("spark.sql.caseSensitive"))
            .transpose()?
            .flatten()
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let matching = if options.merge_schema == Some(true) && !case_sensitive {
            StructFieldMatching::CaseInsensitiveName
        } else {
            StructFieldMatching::Name
        };
        Ok(ParquetReadFormat { options, matching })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = ParquetWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetWriteFormat { options })
    }
}
