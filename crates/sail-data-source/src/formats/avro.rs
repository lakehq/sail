use std::sync::Arc;

use datafusion::datasource::file_format::avro::{AvroFormat, AvroFormatFactory};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo};

use super::listing::{ListingFormat, ListingTableFormat};

pub type AvroTableFormat = ListingTableFormat<AvroListingFormat>;

#[derive(Debug, Default)]
pub struct AvroListingFormat;

impl ListingFormat for AvroListingFormat {
    fn name(&self) -> &'static str {
        "avro"
    }

    fn create_format(&self, _info: &SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn create_format_factory(&self, _info: &SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(AvroFormatFactory::new()))
    }
}
