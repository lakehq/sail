use datafusion::catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingTableFormat};
use crate::options::gen::{TextReadOptions, TextWriteOptions};
use crate::options::ResolveOptions;

pub mod options;
mod read;
pub mod reader;
pub mod source;
mod write;
pub mod writer;

pub use read::TextReadFormat;
pub use write::TextWriteFormat;

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

pub type TextTableFormat = ListingTableFormat<TextFormatFactory>;

#[derive(Debug, Default)]
pub struct TextFormatFactory;

impl FormatFactory for TextFormatFactory {
    type Read = TextReadFormat;
    type Write = TextWriteFormat;

    fn name() -> &'static str {
        "text"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let options = TextReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(TextReadFormat { options })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = TextWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(TextWriteFormat { options })
    }
}
