use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion_common::Result;
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{FormatFactory, ListingTableFormat, WriteFormat};

mod read;

pub type AvroTableFormat = ListingTableFormat<AvroFormatFactory>;

#[derive(Debug, Default)]
pub struct AvroFormatFactory;

#[derive(Debug, Default, Clone)]
pub struct AvroReadFormat;

#[derive(Debug, Default, Clone)]
pub struct AvroWriteFormat;

impl FormatFactory for AvroFormatFactory {
    type Read = AvroReadFormat;
    type Write = AvroWriteFormat;

    fn name() -> &'static str {
        "avro"
    }

    fn read(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Read> {
        Ok(AvroReadFormat)
    }

    fn write(_ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Write> {
        Ok(AvroWriteFormat)
    }
}

// ReadFormat impl moved to `read.rs`.

impl WriteFormat for AvroWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((Arc::new(AvroFormat), None))
    }
}
