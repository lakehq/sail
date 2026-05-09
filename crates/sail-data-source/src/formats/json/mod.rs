use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion_common::config::JsonOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::listing::source::{
    DefaultSchemaInfer, FormatFactory, ListingTableFormat, ReadFormat, SchemaInfer, WriteFormat,
};
use crate::options::gen::{JsonReadOptions, JsonWriteOptions};
use crate::options::ResolveOptions;

mod options;

pub type JsonTableFormat = ListingTableFormat<JsonFormatFactory>;

#[derive(Debug, Default)]
pub struct JsonFormatFactory;

#[derive(Debug, Clone)]
pub struct JsonReadFormat {
    options: JsonOptions,
}

#[derive(Debug, Clone)]
pub struct JsonWriteFormat {
    options: JsonOptions,
}

impl FormatFactory for JsonFormatFactory {
    type Read = JsonReadFormat;
    type Write = JsonWriteFormat;

    fn name() -> &'static str {
        "json"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> datafusion_common::Result<Self::Read> {
        let options = JsonReadOptions::resolve(ctx, options)
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        Ok(JsonReadFormat { options })
    }

    fn write(
        ctx: &dyn Session,
        options: Vec<OptionLayer>,
    ) -> datafusion_common::Result<Self::Write> {
        let options = JsonWriteOptions::resolve(ctx, options)
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        Ok(JsonWriteFormat { options })
    }
}

impl ReadFormat for JsonReadFormat {
    fn create_read_format(
        &self,
        compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let mut options = self.options.clone();
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(JsonFormat::default().with_options(options)))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}

impl WriteFormat for JsonWriteFormat {
    fn create_write_format(
        &self,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        Ok((
            Arc::new(JsonFormat::default().with_options(self.options.clone())),
            None,
        ))
    }
}
