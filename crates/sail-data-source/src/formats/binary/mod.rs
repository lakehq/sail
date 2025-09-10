use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::binary::file_format::BinaryFileFormat;
use crate::formats::binary::options::resolve_binary_read_options;
use crate::formats::listing::{ListingFormat, ListingTableFormat};

pub mod file_format;
pub mod options;
pub mod reader;
pub mod source;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct TableBinaryOptions {
    pub path_glob_filter: Option<String>,
}

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
        options: Vec<HashMap<String, String>>,
        _compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(BinaryFileFormat::new(
            resolve_binary_read_options(options)?,
        )))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        datafusion_common::not_impl_err!("Binary file format does not support writing")
    }
}
