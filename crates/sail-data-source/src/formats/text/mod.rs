use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::text::file_format::TextFileFormat;
use crate::listing::source::{
    DefaultSchemaInfer, FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};
use crate::options::gen::{TextReadOptions, TextWriteOptions};
use crate::options::ResolveOptions;

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

pub type TextTableFormat = ListingTableFormat<TextFormatFactory>;

#[derive(Debug, Default)]
pub struct TextFormatFactory;

#[derive(Debug, Clone)]
pub struct TextReadFormat {
    options: TableTextOptions,
}

#[derive(Debug, Clone)]
pub struct TextWriteFormat {
    options: TableTextOptions,
}

impl FormatFactory for TextFormatFactory {
    type Read = TextReadFormat;
    type Write = TextWriteFormat;

    fn name() -> &'static str {
        "text"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> datafusion_common::Result<Self::Read> {
        let options = TextReadOptions::resolve(ctx, options)
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        Ok(TextReadFormat { options })
    }

    fn write(
        ctx: &dyn Session,
        options: Vec<OptionLayer>,
    ) -> datafusion_common::Result<Self::Write> {
        let options = TextWriteOptions::resolve(ctx, options)
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        Ok(TextWriteFormat { options })
    }
}

impl ReadFormat for TextReadFormat {
    fn create_read_format(
        &self,
        compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let mut options = self.options.clone();
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(TextFileFormat::new(options)))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}

impl WriteFormat for TextWriteFormat {
    fn create_write_format(
        &self,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(TextFileFormat::new(self.options.clone())), None))
    }
}
