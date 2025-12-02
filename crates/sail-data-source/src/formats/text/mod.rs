use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::listing::{ListingFormat, ListingTableFormat};
use crate::formats::text::file_format::TextFileFormat;
use crate::formats::text::options::{resolve_text_read_options, resolve_text_write_options};

pub mod file_format;
pub mod options;
pub mod reader;
pub mod source;
pub mod writer;

pub const DEFAULT_TEXT_EXTENSION: &str = ".txt";

#[derive(Debug, Clone, PartialEq)]
pub struct TableTextOptions {
    pub whole_text: bool,
    pub line_sep: Option<char>,
    pub compression: CompressionTypeVariant,
}

impl Default for TableTextOptions {
    fn default() -> Self {
        Self {
            whole_text: false,
            line_sep: None,
            compression: CompressionTypeVariant::UNCOMPRESSED,
        }
    }
}

pub type TextTableFormat = ListingTableFormat<TextListingFormat>;

#[derive(Debug, Default)]
pub struct TextListingFormat;

impl ListingFormat for TextListingFormat {
    fn name(&self) -> &'static str {
        "text"
    }

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
        compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let mut options = resolve_text_read_options(options)?;
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(TextFileFormat::new(options)))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = resolve_text_write_options(options)?;
        Ok((Arc::new(TextFileFormat::new(options)), None))
    }
}
