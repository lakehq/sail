use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::Result;
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
    options: TableParquetOptions,
    extension: String,
}

#[derive(Debug, Clone)]
pub struct ParquetWriteFormat {
    options: TableParquetOptions,
    compression: Option<String>,
}

impl FormatFactory for ParquetFormatFactory {
    type Read = ParquetReadFormat;
    type Write = ParquetWriteFormat;

    fn name() -> &'static str {
        "parquet"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        let read_options = ParquetReadOptions::resolve(ctx, options)
            .map_err(datafusion_common::DataFusionError::from)?;
        let extension = read_options.extension.clone();
        Ok(ParquetReadFormat {
            options: read_options.into_table_options(),
            extension,
        })
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        let options = ParquetWriteOptions::resolve(ctx, options)
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        let compression = options.global.compression.clone();
        Ok(ParquetWriteFormat {
            options,
            compression,
        })
    }
}

impl ReadFormat for ParquetReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(
            ParquetFormat::default().with_options(self.options.clone()),
        ))
    }

    fn file_extension_override(&self) -> Result<Option<String>> {
        Ok(Some(self.extension.clone()))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}

impl WriteFormat for ParquetWriteFormat {
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((
            Arc::new(ParquetFormat::default().with_options(self.options.clone())),
            self.compression.clone(),
        ))
    }
}
