use std::sync::Arc;

use datafusion::datasource::file_format::json::{JsonFormat, JsonFormatFactory};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo};

use super::listing::{ListingFormat, ListingTableFormat};
use crate::options::DataSourceOptionsResolver;

pub type JsonTableFormat = ListingTableFormat<JsonListingFormat>;

#[derive(Debug, Default)]
pub struct JsonListingFormat;

impl ListingFormat for JsonListingFormat {
    fn name(&self) -> &'static str {
        "json"
    }

    fn create_format(&self, info: &SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(info.ctx);
        let options = resolver.resolve_json_read_options(info.options.clone())?;
        Ok(Arc::new(JsonFormat::default().with_options(options)))
    }

    fn create_format_factory(&self, info: &SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        let resolver = DataSourceOptionsResolver::new(info.ctx);
        let options = resolver.resolve_json_write_options(info.options.clone())?;
        Ok(Arc::new(JsonFormatFactory::new_with_options(options)))
    }
}
