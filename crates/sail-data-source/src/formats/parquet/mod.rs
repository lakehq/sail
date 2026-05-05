use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::Result;
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::listing::{DefaultSchemaInfer, ListingFormat, ListingTableFormat, SchemaInfer};
use crate::formats::parquet::options::{
    resolve_parquet_read_options, resolve_parquet_write_options,
};

mod options;

pub type ParquetTableFormat = ListingTableFormat<ParquetListingFormat>;

#[derive(Debug, Default)]
pub struct ParquetListingFormat;

impl ListingFormat for ParquetListingFormat {
    fn name(&self) -> &'static str {
        "parquet"
    }

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<OptionLayer>,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = resolve_parquet_read_options(ctx, options)
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options();
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<OptionLayer>,
    ) -> Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = resolve_parquet_write_options(ctx, options)
            .map_err(datafusion_common::DataFusionError::from)?
            .into_table_options()
            .map_err(datafusion_common::DataFusionError::from)?;
        let compression = options.global.compression.clone();
        Ok((
            Arc::new(ParquetFormat::default().with_options(options)),
            compression,
        ))
    }

    fn file_extension_override(
        &self,
        ctx: &dyn Session,
        options: &[OptionLayer],
    ) -> Result<Option<String>> {
        let read_options = resolve_parquet_read_options(ctx, options.to_vec())
            .map_err(datafusion_common::DataFusionError::from)?;
        Ok(Some(read_options.extension))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}
