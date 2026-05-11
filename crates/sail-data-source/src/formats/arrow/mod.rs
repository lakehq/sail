use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::Result;
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{
    DefaultSchemaInfer, FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};

pub type ArrowTableFormat = ListingTableFormat<ArrowFormatFactory>;

#[derive(Debug, Default)]
pub struct ArrowFormatFactory;

#[derive(Debug, Default, Clone)]
pub struct ArrowReadFormat;

#[derive(Debug, Default, Clone)]
pub struct ArrowWriteFormat;

impl FormatFactory for ArrowFormatFactory {
    type Read = ArrowReadFormat;
    type Write = ArrowWriteFormat;

    fn name() -> &'static str {
        "arrow"
    }

    fn read(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Read> {
        Ok(ArrowReadFormat)
    }

    fn write(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Write> {
        Ok(ArrowWriteFormat)
    }
}

impl ReadFormat for ArrowReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}

impl WriteFormat for ArrowWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(ArrowFormat), None))
    }
}
