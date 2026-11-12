use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableSource};
use datafusion::physical_expr::{LexRequirement, Partitioning};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{Result, Statistics, not_impl_err, plan_err};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::{ListingTableUrl, TableSchema};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::datasource::{
    DataSource, OptionLayer, SinkInfo, SinkMode, SourceInfo, find_path_in_options,
    get_partition_columns_and_file_schema,
};
use url::Url;

use crate::listing::internal::{ListingPolicy, TableListingStore};
use crate::listing::table::{ListingTableSource, ListingTableSourceConfig};
use crate::listing::utils::{
    infer_partitions, rewrite_unsupported_fields, sample_listing_files, validate_partitions,
};
use crate::listing::write::{FileWriteNode, FileWriteOptions};
use crate::resolve_listing_urls;
use crate::url::{PathGlobFilter, resolve_listing_writer_url};

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

    /// Whether partition paths follow Spark's Hive escaping and null convention.
    fn hive_partition_paths(&self) -> bool {
        false
    }

    /// Whether validating an explicit schema requires the physical file schema.
    fn requires_explicit_schema_validation(&self) -> bool {
        false
    }

    /// Validate a user-provided file schema against the physical file schema.
    fn validate_explicit_schema(&self, _schema: &Schema, _physical: &Schema) -> Result<()> {
        Ok(())
    }

    /// File-name glob restricting which listed files compose the dataset.
    fn path_glob_filter(&self) -> Option<&str> {
        None
    }
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
    pub output_partitioning: Option<Partitioning>,
    pub schema: TableSchema,
    pub compression: CompressionTypeVariant,
}

/// Configuration for creating a listing-file sink execution plan.
pub struct ListingSinkInput {
    pub input: Arc<dyn ExecutionPlan>,
    pub sink: FileSinkConfig,
    pub sort_order: Option<LexRequirement>,
}

/// A trait for format-specific logic for writing listing files.
#[async_trait]
pub trait WriteFormat: Debug + Send + Sync + 'static {
    async fn sink(
        &self,
        ctx: &dyn Session,
        input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug, Default)]
pub struct ListingDataSource<T: FormatFactory> {
    phantom: PhantomData<T>,
}

#[async_trait]
impl<T: FormatFactory> DataSource for ListingDataSource<T> {
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
            lakehouse_table: _,
            schema,
            constraints,
            partition_by,
            bucket_by: _,
            sort_order,
            options,
            read_case_sensitive,
        } = info;

        let read_format = T::read(ctx, options)?;
        let hive_partition_paths = read_format.hive_partition_paths();
        let path_glob_filter = read_format
            .path_glob_filter()
            .map(PathGlobFilter::parse)
            .transpose()?;
        let urls = resolve_listing_urls(ctx, paths).await?;
        let sampled_files = sample_listing_files(ctx, &urls, path_glob_filter.as_ref()).await?;
        let compression = read_format.infer_compression(ctx, &sampled_files).await?;

        let (schema, partition_fields) = match schema {
            Some(schema) if !schema.fields().is_empty() => {
                let physical = if read_format.requires_explicit_schema_validation() {
                    Some(
                        read_format
                            .infer_schema(ctx, &sampled_files, compression)
                            .await?,
                    )
                } else if read_case_sensitive {
                    None
                } else {
                    read_format
                        .infer_schema(ctx, &sampled_files, compression)
                        .await
                        .ok()
                };
                // Spark matches a user-specified schema against the physical file
                // columns case-insensitively by default (`spark.sql.caseSensitive=false`).
                // Reconcile the user column names to the physical names up front so that both
                // the file stats and reader (which resolve columns by exact name) find the data.
                let schema = if read_case_sensitive {
                    schema
                } else if let Some(physical) = &physical {
                    reconcile_schema_names_case_insensitive(schema, physical)?
                } else {
                    // Keeps the user schema if physical schema inference is unavailable.
                    schema
                };
                // When the partition columns are not specified, auto-discover
                // them from `key=value` segments in the listing paths.
                // Without this, columns that exist only in the directory tree
                // are treated as file columns, and the file reader fails
                // because the file itself does not contain them.
                let partition_by = if partition_by.is_empty() {
                    infer_partitions(&sampled_files, hive_partition_paths)?
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
                if let Some(physical) = physical {
                    read_format.validate_explicit_schema(&schema, &physical)?;
                }
                (Arc::new(schema), partition_fields)
            }
            _ => {
                let schema = read_format
                    .infer_schema(ctx, &sampled_files, compression)
                    .await?;
                let schema = rewrite_unsupported_fields(schema);

                let partition_by = if partition_by.is_empty() {
                    infer_partitions(&sampled_files, hive_partition_paths)?
                } else {
                    partition_by
                };

                // TODO: infer concrete partition types from observed values to match
                //   the `spark.sql.sources.partitionColumnTypeInference.enabled` option.
                let partition_fields = partition_by
                    .into_iter()
                    .map(|col| Arc::new(Field::new(col, DataType::Utf8, hive_partition_paths)))
                    .collect();
                (schema, partition_fields)
            }
        };

        validate_partitions(&sampled_files, &partition_fields, hive_partition_paths)?;

        let source = ListingTableSource::try_new(ListingTableSourceConfig {
            table_paths: urls,
            schema: TableSchema::builder(schema)
                .with_table_partition_cols(partition_fields)
                .build(),
            constraints,
            file_sort_order: vec![sort_order],
            collect_stat: ctx.config().collect_statistics(),
            target_partitions: ctx.config().target_partitions(),
            read_format: Arc::new(read_format),
            path_glob_filter,
            compression,
        })?;
        Ok(Arc::new(source))
    }

    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        let Some(path) = find_path_in_options(&info.options) else {
            return plan_err!("missing path in listing table options");
        };
        let SinkInfo {
            input,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
            lakehouse_table,
            sql_insert: _,
            identity_columns_to_track: _,
        } = info;
        let catalog_managed = lakehouse_table.is_some();
        if bucket_by.is_some() {
            return not_impl_err!("bucketing for writing listing data source");
        }
        if partition_by.iter().any(|field| field.transform.is_some()) {
            return not_impl_err!("partition transforms for writing listing data source");
        }
        let url = resolve_listing_writer_url(path.clone())?;
        let parquet = T::name().eq_ignore_ascii_case("parquet");
        let dynamic = parquet && dynamic_partition_overwrite(&options)?;
        let overwrite_partitions = parquet
            && !partition_by.is_empty()
            && (matches!(mode, SinkMode::OverwritePartitions)
                || (matches!(mode, SinkMode::Overwrite) && dynamic));
        let overwrite = match mode {
            SinkMode::ErrorIfExists => {
                if (!catalog_managed && listing_target_exists(ctx, &url).await?)
                    || (catalog_managed && listing_target_nonempty(ctx, &url).await?)
                {
                    return plan_err!("listing table path already exists: {path}");
                }
                false
            }
            SinkMode::IgnoreIfExists => {
                if listing_target_exists(ctx, &url).await? {
                    return LogicalPlanBuilder::empty(false).build();
                }
                false
            }
            SinkMode::Append => false,
            SinkMode::Overwrite => true,
            SinkMode::OverwritePartitions if parquet => true,
            mode => return not_impl_err!("unsupported sink mode for listing table: {mode:?}"),
        };
        let write_format = T::write(ctx, options)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileWriteNode::new(
                Arc::new(input),
                FileWriteOptions {
                    format: Arc::new(write_format),
                    url,
                    overwrite,
                    overwrite_partitions,
                    partition_by,
                    sort_by: sort_order,
                },
            )),
        }))
    }
}
fn dynamic_partition_overwrite(options: &[OptionLayer]) -> Result<bool> {
    let mut mode = None;
    for layer in options {
        let (OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items }) = layer
        else {
            continue;
        };
        for (key, value) in items {
            let key = if matches!(layer, OptionLayer::TablePropertyList { .. }) {
                key.strip_prefix("option.").unwrap_or(key)
            } else {
                key
            };
            if key.eq_ignore_ascii_case("partitionOverwriteMode") {
                mode = Some(value);
            }
        }
    }
    match mode {
        None => Ok(false),
        Some(mode) if mode.eq_ignore_ascii_case("static") => Ok(false),
        Some(mode) if mode.eq_ignore_ascii_case("dynamic") => Ok(true),
        Some(mode) => {
            plan_err!("invalid partitionOverwriteMode: {mode}; expected STATIC or DYNAMIC")
        }
    }
}

async fn listing_target_exists(ctx: &dyn Session, url: &Url) -> Result<bool> {
    // For file systems, treat the target as existing even if it is an empty directory.
    if url.scheme() == "file"
        && let Ok(path) = url.to_file_path()
        && path.exists()
    {
        return Ok(true);
    }
    listing_target_nonempty(ctx, url).await
}
async fn listing_target_nonempty(ctx: &dyn Session, url: &Url) -> Result<bool> {
    let path = ListingTableUrl::try_new(url.clone(), None)?;
    let store = ctx.runtime_env().object_store(&path)?;
    let listing =
        TableListingStore::new(store, path.prefix().clone(), ListingPolicy::from_url(url));
    Ok(listing
        .list(Some(path.prefix()))
        .try_next()
        .await?
        .is_some())
}

// Reconciles a user-specified schema's field names with the physical file schema
// case-insensitively, matching Spark's default `spark.sql.caseSensitive=false`.
fn reconcile_schema_names_case_insensitive(schema: Schema, physical: &Schema) -> Result<Schema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        let mut matches = physical
            .fields()
            .iter()
            .filter(|f| f.name().eq_ignore_ascii_case(name));
        let reconciled = match matches.next() {
            None => Arc::clone(field),
            Some(first) => {
                if let Some(second) = matches.next() {
                    let mut names = vec![first.name().as_str(), second.name().as_str()];
                    names.extend(matches.map(|f| f.name().as_str()));
                    return plan_err!(
                        "Ambiguous case-insensitive column match for `{name}`: [{}]",
                        names.join(", ")
                    );
                }
                if first.name() == name {
                    Arc::clone(field)
                } else {
                    Arc::new(field.as_ref().clone().with_name(first.name().as_str()))
                }
            }
        };
        fields.push(reconciled);
    }
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

#[cfg(test)]
mod overwrite_option_tests {
    use super::*;

    #[tokio::test]
    async fn reserved_only_is_logically_empty_but_ordinary_hidden_objects_count() -> Result<()> {
        use datafusion::prelude::SessionContext;
        use object_store::ObjectStoreExt;
        use object_store::path::Path;

        use crate::listing::internal::tests::RecordingStore;
        let store = Arc::new(RecordingStore::default());
        let ctx = SessionContext::new();
        let url = ListingTableUrl::parse("memory://bucket/table/")?
            .get_url()
            .clone();
        ctx.runtime_env().register_object_store(&url, store.clone());
        store
            .put(
                &Path::from("table/.sail-staging/operation/complete"),
                bytes::Bytes::from_static(b"complete").into(),
            )
            .await?;
        assert!(!listing_target_nonempty(&ctx.state(), &url).await?);
        assert!(!listing_target_exists(&ctx.state(), &url).await?);
        for file in [".ordinary", "_SUCCESS", "%2Esail-staging/file"] {
            let path = Path::parse(format!("table/{file}"))?;
            store.put(&path, bytes::Bytes::new().into()).await?;
            assert!(listing_target_nonempty(&ctx.state(), &url).await?);
            store.delete(&path).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn flat_nonempty_check_stops_at_first_visible_object() -> Result<()> {
        use std::sync::atomic::Ordering;

        use object_store::ObjectStoreExt;
        use object_store::path::Path;

        use crate::listing::internal::tests::{Event, RecordingStore};

        let store = Arc::new(RecordingStore {
            delimiter_not_implemented: true,
            ..Default::default()
        });
        store
            .put(
                &Path::from("table/.sail-staging/operation/file"),
                bytes::Bytes::from_static(b"stage").into(),
            )
            .await?;
        for index in 0..500 {
            store
                .put(
                    &Path::from(format!("table/file-{index:04}")),
                    bytes::Bytes::from_static(b"data").into(),
                )
                .await?;
        }
        let ctx = datafusion::prelude::SessionContext::new();
        let url = ListingTableUrl::parse("hf://datasets/table/")?;
        ctx.runtime_env()
            .register_object_store(url.get_url(), store.clone());
        assert!(listing_target_nonempty(&ctx.state(), url.get_url()).await?);
        assert_eq!(store.yielded.load(Ordering::SeqCst), 2);
        assert_eq!(
            *store
                .events
                .lock()
                .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?,
            vec![Event::List(url.prefix().clone())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn existing_empty_local_directory_still_exists() -> Result<()> {
        let ctx = datafusion::prelude::SessionContext::new();
        let directory = std::env::temp_dir().join(format!("sail-listing-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory)?;
        let url = Url::from_directory_path(&directory)
            .map_err(|()| datafusion_common::exec_datafusion_err!("invalid local directory"))?;
        let exists = listing_target_exists(&ctx.state(), &url).await;
        let nonempty = listing_target_nonempty(&ctx.state(), &url).await;
        std::fs::remove_dir(&directory)?;
        assert!(exists?);
        assert!(!nonempty?);
        Ok(())
    }

    #[test]
    fn layers_and_keys_are_case_insensitive() -> Result<()> {
        let session = OptionLayer::OptionList {
            items: vec![("partitionOverwriteMode".into(), "DYNAMIC".into())],
        };
        let write = OptionLayer::OptionList {
            items: vec![("PARTITIONOVERWRITEMODE".into(), "static".into())],
        };
        assert!(!dynamic_partition_overwrite(&[session.clone(), write])?);
        assert!(dynamic_partition_overwrite(&[session])?);
        assert!(
            dynamic_partition_overwrite(&[OptionLayer::OptionList {
                items: vec![("partitionOverwriteMode".into(), "invalid".into())]
            }])
            .is_err()
        );
        Ok(())
    }
}
