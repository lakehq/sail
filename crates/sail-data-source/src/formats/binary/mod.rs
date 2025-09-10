use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::binary::file_format::BinaryFileFormat;
use crate::formats::listing::{ListingFormat, ListingTableFormat};

pub mod file_format;
pub mod reader;
pub mod source;

pub const DEFAULT_BINARY_EXTENSION: &str = "";

pub(crate) type BinaryTableFormat = ListingTableFormat<BinaryListingFormat>;

#[derive(Debug, Default)]
pub(crate) struct BinaryListingFormat;

impl ListingFormat for BinaryListingFormat {
    fn name(&self) -> &'static str {
        "binaryFile"
    }

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
        _compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(BinaryFileFormat::new()))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        datafusion_common::not_impl_err!("Binary file format does not support writing")
    }
}
