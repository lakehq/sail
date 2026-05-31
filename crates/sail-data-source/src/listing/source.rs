use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::TableSource;
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, not_impl_err, plan_err, GetExt, Result, Statistics};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::{ListingTableUrl, TableSchema};
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::datasource::{
    find_path_in_options, get_partition_columns_and_file_schema, OptionLayer, SinkInfo, SourceInfo,
    TableFormat,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

use crate::listing::table::{ListingTableSource, ListingTableSourceConfig};
use crate::listing::utils::{
    infer_partitions, rewrite_utf8view_fields, sample_listing_files, validate_partitions,
};
use crate::resolve_listing_urls;
use crate::utils::split_parquet_compression_string;

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
    fn create_write_format(&self) -> Result<(Arc<dyn FileFormat>, Option<String>)>;
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
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in listing table options");
        };
        let SinkInfo {
            input,
            // TODO: sink mode is ignored since the file formats only support append operation
            mode: _,
            partition_by,
            bucket_by,
            sort_order,
            options,
            logical_schema: _,
        } = info;
        if is_flow_event_schema(&input.schema()) {
            return plan_err!("cannot write streaming data to listing table");
        }
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for writing listing table format");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for writing listing table format");
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
            .map(|field| (field.column.clone(), DataType::Null))
            .collect::<Vec<_>>();
        let write_format = T::write(ctx, options)?;
        let (format, compression) = write_format.create_write_format()?;
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
            file_output_mode: FileOutputMode::Automatic,
        };
        format
            .create_writer_physical_plan(input, ctx, conf, sort_order)
            .await
    }
}
