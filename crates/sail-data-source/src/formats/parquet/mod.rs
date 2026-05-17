use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{
    DefaultSchemaInfer, FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};
use crate::options::gen::{ParquetReadOptions, ParquetWriteOptions};
use crate::options::ResolveOptions;

mod options;

pub type ParquetTableFormat = ListingTableFormat<ParquetFormatFactory>;

#[derive(Debug, Default)]
pub struct ParquetFormatFactory;

#[derive(Debug, Clone)]
pub struct ParquetReadFormat {
    options: ParquetReadOptions,
}

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    options: ParquetWriteOptions,
}

impl FormatFactory for ParquetFormatFactory {
    type Read = ParquetReadFormat;
    type Write = ParquetWriteFormat;

    fn name() -> &'static str {
        "parquet"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let options = ParquetReadOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetReadFormat { options })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = ParquetWriteOptions::resolve(ctx, options).map_err(DataFusionError::from)?;
        Ok(ParquetWriteFormat { options })
    }
}

impl ReadFormat for ParquetReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = self.options.clone().into_table_options();
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn file_extension_override(&self) -> Result<Option<String>> {
        Ok(Some(self.options.extension.clone()))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}

impl WriteFormat for ParquetWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        let compression = options.global.compression.clone();
        Ok((
            Arc::new(ParquetFormat::default().with_options(options)),
            compression,
        ))
    }
}
