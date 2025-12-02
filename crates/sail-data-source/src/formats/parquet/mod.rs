use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::Result;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::listing::{ListingFormat, ListingTableFormat};
use crate::formats::parquet::options::{
    resolve_parquet_read_options, resolve_parquet_write_options,
};

mod options;

pub type ParquetTableFormat = ListingTableFormat<ParquetListingFormat>;

#[derive(Debug, Default)]
pub struct ParquetListingFormat;

impl ListingFormat for ParquetListingFormat {
    fn name(&self) -> &'static str {
        "parquet"
    }

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = resolve_parquet_read_options(ctx, options)?;
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = resolve_parquet_write_options(ctx, options)?;
        let compression = options.global.compression.clone();
        Ok((
            Arc::new(ParquetFormat::default().with_options(options)),
            compression,
        ))
    }
}
