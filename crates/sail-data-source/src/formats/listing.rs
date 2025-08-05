use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{internal_err, not_impl_err, Result};
use sail_common_datafusion::datasource::{
    get_partition_columns_and_file_schema, SinkInfo, SourceInfo, TableFormat,
};

use crate::options::DataSourceOptionsResolver;

// TODO: infer compression type from file extension
// TODO: support global configuration to ignore file extension (by setting it to empty)
/// A trait for defining the specifics of a listing table format.
pub(crate) trait ListingFormat: Debug + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>>;
    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>>;
}

#[derive(Debug)]
pub(crate) struct ListingTableFormat<T: ListingFormat> {
    inner: T,
}

impl<T: ListingFormat> ListingTableFormat<T> {
    pub fn new(format_def: T) -> Self {
        Self { inner: format_def }
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
        self.inner.name()
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema,
            constraints,
            partition_by,
            bucket_by: _,
            sort_order,
            options,
        } = info;

        let urls = crate::url::resolve_listing_urls(ctx, paths).await?;
        let file_format = self.inner.create_read_format(ctx, options)?;
        let config = ctx.config();
        let listing_options = ListingOptions::new(file_format)
            .with_target_partitions(config.target_partitions())
            .with_collect_stat(config.collect_statistics());

        let (schema, partition_by) = match schema {
            Some(schema) if !schema.fields().is_empty() => {
                let (partition_by, schema) =
                    get_partition_columns_and_file_schema(&schema, partition_by)?;
                (Arc::new(schema), partition_by)
            }
            _ => {
                let schema =
                    crate::listing::resolve_listing_schema(ctx, &urls, &listing_options).await?;
                let partition_by = partition_by
                    .into_iter()
                    .map(|col| (col, DataType::Utf8))
                    .collect();
                (schema, partition_by)
            }
        };

        let listing_options = listing_options
            .with_file_sort_order(vec![sort_order])
            .with_table_partition_cols(partition_by);

        let config = ListingTableConfig::new_with_multi_paths(urls);
        let config = if listing_options.table_partition_cols.is_empty() {
            config
                .with_listing_options(listing_options)
                .infer_partitions_from_path(ctx)
                .await?
        } else {
            for url in config.table_paths.iter() {
                listing_options.validate_partitions(ctx, url).await?;
            }
            config.with_listing_options(listing_options)
        };
        // The schema must be set after the listing options, otherwise it will panic.
        let config = config.with_schema(schema);
        let config = crate::listing::rewrite_listing_partitions(config)?;
        Ok(Arc::new(
            ListingTable::try_new(config)?.with_constraints(constraints),
        ))
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            path,
            // TODO: sink mode is ignored since the file formats only support append operation
            mode: _,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for writing listing table format");
        }
        // always write multi-file output
        let path = if path.ends_with(object_store::path::DELIMITER) {
            path
        } else {
            format!("{path}{}", object_store::path::DELIMITER)
        };
        let table_paths = crate::url::resolve_listing_urls(ctx, vec![path.clone()]).await?;
        let object_store_url = if let Some(path) = table_paths.first() {
            path.object_store()
        } else {
            return internal_err!("empty listing table path: {path}");
        };
        // We do not need to specify the exact data type for partition columns,
        // since the type is inferred from the record batch during writing.
        // This is how DataFusion handles physical planning for `LogicalPlan::Copy`.
        let table_partition_cols = partition_by
            .iter()
            .map(|s| (s.clone(), DataType::Null))
            .collect::<Vec<_>>();
        let format = self.inner.create_write_format(ctx, options)?;
        let conf = FileSinkConfig {
            original_url: path,
            object_store_url,
            file_group: Default::default(),
            table_paths,
            output_schema: input.schema(),
            table_partition_cols,
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: format.get_ext(),
        };
        format
            .create_writer_physical_plan(input, ctx, conf, sort_order)
            .await
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

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
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

    fn create_read_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn create_write_format(
        &self,
        _ctx: &dyn Session,
        _options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
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

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_csv_read_options(options)?;
        Ok(Arc::new(CsvFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_csv_write_options(options)?;
        Ok(Arc::new(CsvFormat::default().with_options(options)))
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

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_json_read_options(options)?;
        Ok(Arc::new(JsonFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_json_write_options(options)?;
        Ok(Arc::new(JsonFormat::default().with_options(options)))
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

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_parquet_read_options(options)?;
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileFormat>> {
        let resolver = DataSourceOptionsResolver::new(ctx);
        let options = resolver.resolve_parquet_write_options(options)?;
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }
}
