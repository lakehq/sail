use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::listing::{DefaultSchemaInfer, ListingFormat, ListingTableFormat, SchemaInfer};
use crate::options::gen::{JsonReadOptions, JsonWriteOptions};
use crate::options::ResolveOptions;

mod options;

pub type JsonTableFormat = ListingTableFormat<JsonListingFormat>;

#[derive(Debug, Default)]
pub struct JsonListingFormat;

impl ListingFormat for JsonListingFormat {
    fn name(&self) -> &'static str {
        "json"
    }

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<OptionLayer>,
        compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let mut options = JsonReadOptions::resolve(ctx, options)
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(JsonFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<OptionLayer>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = JsonWriteOptions::resolve(ctx, options)
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        Ok((Arc::new(JsonFormat::default().with_options(options)), None))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}
