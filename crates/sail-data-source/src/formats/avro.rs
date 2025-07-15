use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::avro::{AvroFormat, AvroFormatFactory};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::Result;

use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat, TableWriter};

#[derive(Debug, Default)]
pub struct AvroTableFormat;

#[async_trait]
impl TableFormat for AvroTableFormat {
    fn name(&self) -> &str {
        "avro"
    }

    async fn create_provider(&self, info: SourceInfo<'_>) -> Result<Arc<dyn TableProvider>> {
        let listing_options = ListingOptions::new(Arc::new(AvroFormat::default()));

        let urls = crate::url::resolve_listing_urls(info.ctx, info.paths).await?;

        let schema = match info.schema {
            // ignore empty schema
            Some(x) if !x.fields().is_empty() => Arc::new(x.into()),
            _ => {
                crate::listing::resolve_listing_schema(info.ctx, &urls, &listing_options).await?
            }
        };

        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(schema)
            .infer_partitions_from_path(&info.ctx.state())
            .await?;
        let config = crate::listing::rewrite_listing_partitions(config)?;
        Ok(Arc::new(ListingTable::try_new(config)?))
    }
}

impl TableWriter for AvroTableFormat {
    fn name(&self) -> &str {
        "avro"
    }

    fn create_writer(&self, _info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(AvroFormatFactory::new()))
    }
}
