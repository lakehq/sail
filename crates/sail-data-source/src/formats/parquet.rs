use std::sync::Arc;

use datafusion::datasource::file_format::parquet::{ParquetFormat, ParquetFormatFactory};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo};

use super::listing::{ListingFormat, ListingTableFormat};
use crate::options::DataSourceOptionsResolver;

pub type ParquetTableFormat = ListingTableFormat<ParquetListingFormat>;

#[derive(Debug, Default)]
pub struct ParquetListingFormat;

impl ListingFormat for ParquetListingFormat {
    fn name(&self) -> &'static str {
        "parquet"
    }

    fn create_format(&self, info: &SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(info.ctx);
        let options = resolver.resolve_parquet_read_options(info.options.clone())?;
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn create_format_factory(&self, info: &SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        let resolver = DataSourceOptionsResolver::new(info.ctx);
        let options = resolver.resolve_parquet_write_options(info.options.clone())?;
        Ok(Arc::new(ParquetFormatFactory::new_with_options(options)))
    }
}
