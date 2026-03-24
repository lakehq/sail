use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::json::options::{resolve_json_read_options, resolve_json_write_options};
use crate::formats::listing::{DefaultSchemaInfer, ListingFormat, ListingTableFormat, SchemaInfer};

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
        options: Vec<HashMap<String, String>>,
        compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let mut options = resolve_json_read_options(ctx, options)?;
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(JsonFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = resolve_json_write_options(ctx, options)?;
        Ok((Arc::new(JsonFormat::default().with_options(options)), None))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(DefaultSchemaInfer)
    }
}
