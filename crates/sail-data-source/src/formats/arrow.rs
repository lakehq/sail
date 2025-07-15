use std::sync::Arc;

use datafusion::datasource::file_format::arrow::{ArrowFormat, ArrowFormatFactory};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo};

use super::listing::{ListingFormat, ListingTableFormat};

pub type ArrowTableFormat = ListingTableFormat<ArrowListingFormat>;

#[derive(Debug, Default)]
pub struct ArrowListingFormat;

impl ListingFormat for ArrowListingFormat {
    fn name(&self) -> &'static str {
        "arrow"
    }

    fn create_format(&self, _info: &SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    fn create_format_factory(&self, _info: &SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(ArrowFormatFactory::new()))
    }
}
