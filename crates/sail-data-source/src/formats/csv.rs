use std::sync::Arc;

use datafusion::datasource::file_format::csv::{CsvFormat, CsvFormatFactory};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo};

use super::listing::{ListingFormat, ListingTableFormat};
use crate::options::DataSourceOptionsResolver;

pub type CsvTableFormat = ListingTableFormat<CsvListingFormat>;

#[derive(Debug, Default)]
pub struct CsvListingFormat;

impl ListingFormat for CsvListingFormat {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn create_format(&self, info: &SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(info.ctx);
        let options = resolver.resolve_csv_read_options(info.options.clone())?;
        Ok(Arc::new(CsvFormat::default().with_options(options)))
    }

    fn create_format_factory(&self, info: &SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        let resolver = DataSourceOptionsResolver::new(info.ctx);
        let options = resolver.resolve_csv_write_options(info.options.clone())?;
        Ok(Arc::new(CsvFormatFactory::new_with_options(options)))
    }
}
