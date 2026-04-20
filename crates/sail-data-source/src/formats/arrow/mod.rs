use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::listing::{DefaultSchemaInfer, ListingFormat, ListingTableFormat, SchemaInfer};

pub type ArrowTableFormat = ListingTableFormat<ArrowListingFormat>;

#[derive(Debug, Default)]
pub struct ArrowListingFormat;

impl ListingFormat for ArrowListingFormat {
    fn name(&self) -> &'static str {
        "arrow"
    }

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<OptionLayer>,
        _compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<OptionLayer>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(ArrowFormat), None))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}
