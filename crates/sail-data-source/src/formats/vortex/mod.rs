use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;
use vortex_datafusion::VortexFormat;

use crate::formats::listing::{DefaultSchemaInfer, ListingFormat, ListingTableFormat, SchemaInfer};

pub type VortexTableFormat = ListingTableFormat<VortexListingFormat>;

#[derive(Debug, Default)]
pub struct VortexListingFormat;

impl ListingFormat for VortexListingFormat {
    fn name(&self) -> &'static str {
        "vortex"
    }

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
        _compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let session = VortexSession::default();
        Ok(Arc::new(VortexFormat::new(session)))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        let session = VortexSession::default();
        Ok((Arc::new(VortexFormat::new(session)), None))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}
