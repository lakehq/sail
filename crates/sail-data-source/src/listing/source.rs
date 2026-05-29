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
use datafusion_datasource::TableSchema;
use sail_common_datafusion::datasource::{
    find_path_in_options, get_partition_columns_and_file_schema, OptionLayer, SinkInfo, SourceInfo,
    TableFormat,
};
use sail_common_datafusion::streaming::event::schema::is_flow_event_schema;

use crate::listing::table::{ListingTableSource, ListingTableSourceConfig};
use crate::listing::utils::rewrite_utf8view_fields;
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
        store: &Arc<dyn object_store::ObjectStore>,
        objects: &[object_store::ObjectMeta],
    ) -> Result<CompressionTypeVariant>;

    /// Infer the file schema from the given files.
    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        objects: &[object_store::ObjectMeta],
        compression: CompressionTypeVariant,
    ) -> Result<SchemaRef>;

    /// Infer file-level metadata needed for planning.
    /// The metadata includes statistics and ordering.
    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        file_schema: SchemaRef,
        object: &object_store::ObjectMeta,
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
    pub compression: Option<CompressionTypeVariant>,
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
        let urls = crate::url::resolve_listing_urls(ctx, paths).await?;
        let sampled_file_groups = crate::listing::utils::sample_listing_files(ctx, &urls).await?;
        let sampled_empty = sampled_file_groups.iter().all(|(_, files)| files.is_empty());

        let inferred_compression = if sampled_empty {
            None
        } else {
            let mut inferred: Option<CompressionTypeVariant> = None;
            for (store, files) in &sampled_file_groups {
                if files.is_empty() {
                    continue;
                }
                let compression = read_format.infer_compression(ctx, store, files).await?;
                if compression == CompressionTypeVariant::UNCOMPRESSED {
                    continue;
                }
                match inferred {
                    None => inferred = Some(compression),
                    Some(prev) if prev == compression => {}
                    Some(prev) => {
                        return plan_err!(
                            "Found mixed compression types in listing paths: {prev:?} and {compression:?}"
                        );
                    }
                }
            }
            inferred
        };
        let compression_for_inference =
            inferred_compression.unwrap_or(CompressionTypeVariant::UNCOMPRESSED);

        let (schema, partition_by) = match schema {
            Some(schema) if !schema.fields().is_empty() => {
                // When the caller did not supply partition columns, auto-
                // discover them from `key=value` segments in the listing
                // paths (matching the no-schema branch's behavior via
                // `infer_partitions_from_path`). Without this, columns
                // that exist only in the directory tree (e.g. `part=x/`)
                // are treated as file columns, and the parquet/CSV reader
                // fails because the file itself doesn't contain them.
                let partition_by = if partition_by.is_empty() {
                    let mut discovered = vec![];
                    for (idx, (_store, files)) in sampled_file_groups.iter().enumerate() {
                        let url = urls.get(idx).ok_or_else(|| {
                            datafusion_common::internal_datafusion_err!(
                                "sampled file groups and listing URLs should have the same length"
                            )
                        })?;
                        for name in crate::listing::utils::infer_partition_names(url, files)? {
                            if !discovered.contains(&name) {
                                discovered.push(name);
                            }
                        }
                    }
                    discovered
                        .into_iter()
                        .filter(|name| {
                            schema
                                .fields()
                                .iter()
                                .any(|f| f.name().eq_ignore_ascii_case(name))
                        })
                        .collect()
                } else {
                    partition_by
                };
                let (partition_by, schema) =
                    get_partition_columns_and_file_schema(&schema, partition_by)?;
                (Arc::new(schema), partition_by)
            }
            _ => {
                if sampled_empty {
                    let urls = urls
                        .iter()
                        .map(|url| url.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return plan_err!("No files found in the specified paths: {urls}")?;
                }

                let mut schemas = vec![];
                for (store, files) in sampled_file_groups.iter() {
                    if files.is_empty() {
                        continue;
                    }
                    let schema = read_format
                        .infer_schema(ctx, store, files, compression_for_inference)
                        .await?;
                    schemas.push(Arc::unwrap_or_clone(schema));
                }
                let schema = rewrite_utf8view_fields(Arc::new(
                    datafusion::arrow::datatypes::Schema::try_merge(schemas)?,
                ));
                let partition_by = partition_by
                    .into_iter()
                    .map(|col| (col, DataType::Utf8))
                    .collect();
                (schema, partition_by)
            }
        };

        let partition_by = if partition_by.is_empty() {
            sampled_file_groups
                .first()
                .and_then(|(_store, files)| urls.first().map(|url| (url, files)))
                .map(|(url, files)| crate::listing::utils::infer_partitions(url, files))
                .transpose()?
                .unwrap_or_default()
        } else {
            partition_by
        };

        for (idx, (_store, files)) in sampled_file_groups.iter().enumerate() {
            let url = urls.get(idx).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "sampled file groups and listing URLs should have the same length"
                )
            })?;
            crate::listing::utils::validate_partitions(url, files, &partition_by)?;
        }

        let partition_fields = partition_by
            .iter()
            .map(|(col, data_type)| Arc::new(Field::new(col, data_type.clone(), false)))
            .collect::<Vec<_>>();

        let source = ListingTableSource::try_new(ListingTableSourceConfig {
            table_paths: urls,
            schema: TableSchema::new(schema, partition_fields),
            constraints,
            file_sort_order: vec![sort_order],
            collect_stat: ctx.config().collect_statistics(),
            target_partitions: ctx.config().target_partitions(),
            read_format: Arc::new(read_format),
            compression: inferred_compression,
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
