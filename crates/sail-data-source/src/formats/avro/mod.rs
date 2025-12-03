use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::listing::{ListingFormat, ListingTableFormat};

pub type AvroTableFormat = ListingTableFormat<AvroListingFormat>;

#[derive(Debug, Default)]
pub struct AvroListingFormat;

impl ListingFormat for AvroListingFormat {
    fn name(&self) -> &'static str {
        "avro"
    }

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
        _compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(AvroFormat), None))
    }
}
