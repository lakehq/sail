use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::arrow::{ArrowFormat, ArrowFormatFactory};
use datafusion::datasource::file_format::avro::{AvroFormat, AvroFormatFactory};
use datafusion::datasource::file_format::csv::{CsvFormat, CsvFormatFactory};
use datafusion::datasource::file_format::json::{JsonFormat, JsonFormatFactory};
use datafusion::datasource::file_format::parquet::{ParquetFormat, ParquetFormatFactory};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::Result;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};

use crate::options::DataSourceOptionsResolver;

/// A trait for defining the specifics of a listing table format.
pub(crate) trait ListingFormat: Debug + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn create_format(&self, info: SourceInfo<'_>) -> Result<Arc<dyn FileFormat>>;
    fn create_format_factory(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>>;
}

#[derive(Debug)]
pub(crate) struct ListingTableFormat<T: ListingFormat> {
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
        let SourceInfo {
            ctx,
            paths,
            schema,
            options,
        } = info;

        // Create a new SourceInfo to pass to create_format
        let format_info = SourceInfo {
            ctx,
            paths: paths.clone(),
            schema: schema.clone(),
            options,
        };
        let file_format = self.format_def.create_format(format_info)?;
        let listing_options = ListingOptions::new(file_format);

        let urls = crate::url::resolve_listing_urls(ctx, paths).await?;

        let schema = match schema {
            Some(x) if !x.fields().is_empty() => Arc::new(x),
            _ => crate::listing::resolve_listing_schema(ctx, &urls, &listing_options).await?,
        };

        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(schema)
            .infer_partitions_from_path(&ctx.state())
            .await?;
        let config = crate::listing::rewrite_listing_partitions(config)?;
        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn create_writer(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        self.format_def.create_format_factory(info)
    }
}

// Arrow
pub(crate) type ArrowTableFormat = ListingTableFormat<ArrowListingFormat>;

#[derive(Debug, Default)]
pub(crate) struct ArrowListingFormat;

impl ListingFormat for ArrowListingFormat {
    fn name(&self) -> &'static str {
        "arrow"
    }

    fn create_format(&self, _info: SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    fn create_format_factory(&self, _info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(ArrowFormatFactory::new()))
    }
}

// Avro
pub(crate) type AvroTableFormat = ListingTableFormat<AvroListingFormat>;

#[derive(Debug, Default)]
pub(crate) struct AvroListingFormat;

impl ListingFormat for AvroListingFormat {
    fn name(&self) -> &'static str {
        "avro"
    }

    fn create_format(&self, _info: SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn create_format_factory(&self, _info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(AvroFormatFactory::new()))
    }
}

// Csv
pub(crate) type CsvTableFormat = ListingTableFormat<CsvListingFormat>;

#[derive(Debug, Default)]
pub(crate) struct CsvListingFormat;

impl ListingFormat for CsvListingFormat {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn create_format(&self, info: SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        let SourceInfo { ctx, options, .. } = info;
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_csv_read_options(options)?;
        Ok(Arc::new(CsvFormat::default().with_options(options)))
    }

    fn create_format_factory(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        let SinkInfo { ctx, options, .. } = info;
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_csv_write_options(options)?;
        Ok(Arc::new(CsvFormatFactory::new_with_options(options)))
    }
}

// Json
pub(crate) type JsonTableFormat = ListingTableFormat<JsonListingFormat>;

#[derive(Debug, Default)]
pub(crate) struct JsonListingFormat;

impl ListingFormat for JsonListingFormat {
    fn name(&self) -> &'static str {
        "json"
    }

    fn create_format(&self, info: SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        let SourceInfo { ctx, options, .. } = info;
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_json_read_options(options)?;
        Ok(Arc::new(JsonFormat::default().with_options(options)))
    }

    fn create_format_factory(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        let SinkInfo { ctx, options, .. } = info;
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_json_write_options(options)?;
        Ok(Arc::new(JsonFormatFactory::new_with_options(options)))
    }
}

// Parquet
pub(crate) type ParquetTableFormat = ListingTableFormat<ParquetListingFormat>;

#[derive(Debug, Default)]
pub(crate) struct ParquetListingFormat;

impl ListingFormat for ParquetListingFormat {
    fn name(&self) -> &'static str {
        "parquet"
    }

    fn create_format(&self, info: SourceInfo<'_>) -> Result<Arc<dyn FileFormat>> {
        let SourceInfo { ctx, options, .. } = info;
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_parquet_read_options(options)?;
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn create_format_factory(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        let SinkInfo { ctx, options, .. } = info;
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_parquet_write_options(options)?;
        Ok(Arc::new(ParquetFormatFactory::new_with_options(options)))
    }
}
