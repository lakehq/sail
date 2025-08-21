use datafusion_common::parsers::CompressionTypeVariant;

pub mod file_format;
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
