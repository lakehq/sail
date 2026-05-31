use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::TableSource;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{not_impl_err, plan_err, Result, Statistics};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::{ListingTableUrl, TableSchema};
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::datasource::{
    find_path_in_options, get_partition_columns_and_file_schema, OptionLayer, SinkInfo, SourceInfo,
    TableFormat,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

use datafusion_expr::logical_plan::Extension;
use datafusion_expr::LogicalPlan;

use crate::listing::file_write::{FileWriteNode, FileWriteOptions};
use crate::listing::table::{ListingTableSource, ListingTableSourceConfig};
use crate::listing::utils::{
    infer_partitions, rewrite_utf8view_fields, sample_listing_files, validate_partitions,
};
use crate::resolve_listing_urls;

/// A trait for creating format instances when reading and writing listing files.
pub trait FormatFactory: Debug + Send + Sync + 'static {
    type Read: ReadFormat;
    type Write: WriteFormat;

    /// The name of the format.
    fn name() -> &'static str;

    /// Creates the read format.
    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read>;

    /// Creates the write format.
    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write>;
}

/// A trait for format-specific logic for reading listing files.
#[async_trait]
pub trait ReadFormat: Debug + Send + Sync + 'static {
    async fn infer_compression(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant>;

    /// Infer the file schema from the given files.
    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
        compression: CompressionTypeVariant,
    ) -> Result<SchemaRef>;

    /// Infer file-level metadata needed for planning.
    /// The metadata includes statistics and ordering.
    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        file_schema: SchemaRef,
        compression: CompressionTypeVariant,
    ) -> Result<ListingFileMeta> {
        let _ = (ctx, store, object, compression);
        Ok(ListingFileMeta {
            statistics: Statistics::new_unknown(&file_schema),
            ordering: None,
        })
    }

    /// Build a scan configuration for listing reads.
    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig>;
}

#[derive(Debug)]
pub struct ListingFileSample<'a> {
    pub url: &'a ListingTableUrl,
    pub store: Arc<dyn ObjectStore>,
    pub objects: Vec<ObjectMeta>,
}

#[derive(Debug, Clone)]
pub struct ListingFileMeta {
    pub statistics: Statistics,
    pub ordering: Option<LexOrdering>,
}

#[derive(Debug)]
pub struct ListingScanInput {
    pub object_store_url: ObjectStoreUrl,
    pub file_groups: Vec<FileGroup>,
    pub constraints: datafusion_common::Constraints,
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
    pub preserve_order: bool,
    pub output_ordering: Vec<LexOrdering>,
    pub statistics: Statistics,
    pub partitioned_by_file_group: bool,
    pub schema: TableSchema,
    pub compression: CompressionTypeVariant,
}

/// A trait for format-specific logic for writing listing files.
pub trait WriteFormat: Debug + Send + Sync + 'static {
    fn sink(
        &self,
        input: Arc<dyn ExecutionPlan>,
        ctx: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug, Default)]
pub struct ListingTableFormat<T: FormatFactory> {
    phantom: PhantomData<T>,
}

#[async_trait]
impl<T: FormatFactory> TableFormat for ListingTableFormat<T> {
    fn name(&self) -> &str {
        T::name()
    }

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        let SourceInfo {
            paths,
            schema,
            constraints,
            partition_by,
            bucket_by: _,
            sort_order,
            options,
        } = info;

        let read_format = T::read(ctx, options)?;
        let urls = resolve_listing_urls(ctx, paths).await?;
        let sampled_files = sample_listing_files(ctx, &urls).await?;
        let compression = read_format.infer_compression(ctx, &sampled_files).await?;

        let (schema, partition_fields) = match schema {
            Some(schema) if !schema.fields().is_empty() => {
                // When the partition columns are not specified, auto-discover
                // them from `key=value` segments in the listing paths.
                // Without this, columns that exist only in the directory tree
                // are treated as file columns, and the file reader fails
                // because the file itself does not contain them.
                let partition_by = if partition_by.is_empty() {
                    infer_partitions(&sampled_files)?
                        .into_iter()
                        .filter(|name| {
                            schema
                                .fields()
                                .iter()
                                .any(|f| f.name().eq_ignore_ascii_case(name))
                        })
                        .collect::<Vec<_>>()
                } else {
                    partition_by
                };
                let (partition_fields, schema) =
                    get_partition_columns_and_file_schema(&schema, partition_by)?;
                (Arc::new(schema), partition_fields)
            }
            _ => {
                let schema = read_format
                    .infer_schema(ctx, &sampled_files, compression)
                    .await?;
                let schema = rewrite_utf8view_fields(schema);

                let partition_by = if partition_by.is_empty() {
                    infer_partitions(&sampled_files)?
                } else {
                    partition_by
                };

                // TODO: infer concrete partition types from observed values to match
                //   the `spark.sql.sources.partitionColumnTypeInference.enabled` option.
                let partition_fields = partition_by
                    .into_iter()
                    .map(|col| Arc::new(Field::new(col, DataType::Utf8, false)))
                    .collect();
                (schema, partition_fields)
            }
        };

        validate_partitions(&sampled_files, &partition_fields)?;

        let source = ListingTableSource::try_new(ListingTableSourceConfig {
            table_paths: urls,
            schema: TableSchema::new(schema, partition_fields),
            constraints,
            file_sort_order: vec![sort_order],
            collect_stat: ctx.config().collect_statistics(),
            target_partitions: ctx.config().target_partitions(),
            read_format: Arc::new(read_format),
            compression,
        })?;
        Ok(Arc::new(source))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<LogicalPlan> {
        if find_path_in_options(&info.options).is_none() {
            return plan_err!("missing path in listing table options");
        }

        if is_flow_event_schema(info.input.schema().inner()) {
            return plan_err!("cannot write streaming data to listing table");
        }

        if info.bucket_by.is_some() {
            return not_impl_err!("bucketing for writing listing table format");
        }
        if info
            .partition_by
            .iter()
            .any(|field| field.transform.is_some())
        {
            return not_impl_err!("partition transforms for writing listing table format");
        }

        let options = FileWriteOptions {
            mode: info.mode,
            partition_by: info.partition_by,
            sort_by: info.sort_order,
            bucket_by: info.bucket_by,
            options: info.options,
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileWriteNode::new(
                Arc::new(info.input),
                self.name().to_string(),
                options,
            )),
        }))
    }
}
