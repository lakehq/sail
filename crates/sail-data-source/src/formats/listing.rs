use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};

/// A trait for defining the specifics of a listing table format.
pub trait ListingFormat: Debug + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn create_format(&self, info: &SourceInfo<'_>) -> Result<Arc<dyn FileFormat>>;
    fn create_format_factory(&self, info: &SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>>;
}

#[derive(Debug)]
pub struct ListingTableFormat<T: ListingFormat> {
    format_def: T,
}

impl<T: ListingFormat> ListingTableFormat<T> {
    pub fn new(format_def: T) -> Self {
        Self { format_def }
    }
}

impl<T: ListingFormat + Default> Default for ListingTableFormat<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[async_trait]
impl<T: ListingFormat> TableFormat for ListingTableFormat<T> {
    fn name(&self) -> &str {
        self.format_def.name()
    }

    async fn create_provider(&self, info: SourceInfo<'_>) -> Result<Arc<dyn TableProvider>> {
        let file_format = self.format_def.create_format(&info)?;
        let listing_options = ListingOptions::new(file_format);

        let urls = crate::url::resolve_listing_urls(info.ctx, info.paths).await?;

        let schema = match info.schema {
            Some(x) if !x.fields().is_empty() => Arc::new(x.into()),
            _ => crate::listing::resolve_listing_schema(info.ctx, &urls, &listing_options).await?,
        };

        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(schema)
            .infer_partitions_from_path(&info.ctx.state())
            .await?;
        let config = crate::listing::rewrite_listing_partitions(config)?;
        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn create_writer(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        self.format_def.create_format_factory(&info)
    }
}
