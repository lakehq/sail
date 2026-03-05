use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, not_impl_err, plan_err, GetExt, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::datasource::{
    get_partition_columns_and_file_schema, SinkInfo, SourceInfo, TableFormat,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

use crate::utils::split_parquet_compression_string;

/// Trait for schema inference logic
#[async_trait::async_trait]
pub trait SchemaInfer: Debug + Send + Sync + 'static {
    /// Get schema based on options. Each implementation can handle its own
    /// special cases like inferSchema=false.
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        list_options: &ListingOptions,
        options: &[HashMap<String, String>],
    ) -> Result<Schema>;
}

/// Default schema inferrer that uses DataFusion's built-in inference
#[derive(Debug)]
pub struct DefaultSchemaInfer;

#[async_trait::async_trait]
impl SchemaInfer for DefaultSchemaInfer {
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        list_options: &ListingOptions,
        _options: &[HashMap<String, String>],
    ) -> Result<Schema> {
        Ok(list_options
            .format
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone())
    }
}

// TODO: support global configuration to ignore file extension (by setting it to empty)
/// A trait for defining the specifics of a listing table format.
pub trait ListingFormat: Debug + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
        compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>>;
    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> Result<(Arc<dyn FileFormat>, Option<String>)>;

    /// Get the schema inferrer for this format
    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer>;
}

#[derive(Debug)]
pub struct ListingTableFormat<T: ListingFormat> {
    inner: T,
}

impl<T: ListingFormat> ListingTableFormat<T> {
    pub fn new(format_def: T) -> Self {
        Self { inner: format_def }
    }

    pub fn inner(&self) -> &T {
        &self.inner
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
        let file_format = self.inner.create_read_format(ctx, options.clone(), None)?;
        let extension_with_compression =
            file_format.compression_type().and_then(|compression_type| {
                match file_format.get_ext_with_compression(&compression_type) {
                    // if the extension is the same as the file format, we don't need to add it
                    Ok(ext) if ext != file_format.get_ext() => Some(ext),
                    _ => None,
                }
            });

        let config = ctx.config();
        let mut listing_options = ListingOptions::new(file_format)
            .with_target_partitions(config.target_partitions())
            .with_collect_stat(config.collect_statistics());

        let (schema, partition_by) = match schema {
            Some(schema) if !schema.fields().is_empty() => {
                let (partition_by, schema) =
                    get_partition_columns_and_file_schema(&schema, partition_by)?;
                (Arc::new(schema), partition_by)
            }
            _ => {
                let schema = crate::listing::resolve_listing_schema(
                    ctx,
                    &urls,
                    &mut listing_options,
                    &extension_with_compression,
                    options,
                    self,
                )
                .await?;
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
        if is_flow_event_schema(&input.schema()) {
            return plan_err!("cannot write streaming data to listing table");
        }
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
        let (format, compression) = self.inner.create_write_format(ctx, options)?;
        let file_extension = if let Some(file_compression_type) = format.compression_type() {
            match format.get_ext_with_compression(&file_compression_type) {
                Ok(ext) => ext,
                Err(_) => format.get_ext(),
            }
        } else {
            let ext = format.get_ext();
            if let Some(compression) = compression {
                if matches!(ext.as_str(), ".parquet" | "parquet") {
                    let ext = ext.strip_prefix('.').unwrap_or(&ext);
                    let compression = compression.strip_prefix('.').unwrap_or(&compression);
                    let (compression, _level) =
                        split_parquet_compression_string(&compression.to_lowercase())?;
                    let file_compression_type = FileCompressionType::from_str(compression.as_str());
                    let compression = match file_compression_type {
                        // Parquet has compression types not supported by FileCompressionType
                        Ok(compression) => compression.get_ext(),
                        Err(_) => compression,
                    };
                    let compression = compression.strip_prefix('.').unwrap_or(&compression);
                    let result = format!("{compression}.{ext}");
                    result
                } else {
                    ext
                }
            } else {
                ext
            }
        };
        let conf = FileSinkConfig {
            original_url: path,
            object_store_url,
            file_group: Default::default(),
            table_paths,
            output_schema: input.schema(),
            table_partition_cols,
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension,
        };
        format
            .create_writer_physical_plan(input, ctx, conf, sort_order)
            .await
    }
}
