// SPDX-License-Identifier: Apache-2.0

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
use sail_common::utils::string::spark_case_insensitive_name_eq;
use sail_common_datafusion::datasource::{
    DataSource, OptionLayer, RegisteredPartition, SinkInfo, SinkMode, SourceInfo,
    find_path_in_options, get_partition_columns_and_file_schema,
};
use sail_common_datafusion::schema_evolution::StructFieldMatching;
use url::Url;

use crate::listing::partition::{
    boolean_option, read_option, resolve_base_path, schema_names_match, split_discovered_partitions,
};
use crate::listing::table::{ListingTableSource, ListingTableSourceConfig};
use crate::listing::utils::{SchemaSampling, rewrite_unsupported_fields, sample_listing_files};
use crate::listing::write::{FileWriteNode, FileWriteOptions};
use crate::url::{PathGlobFilter, resolve_listing_writer_url, resolve_read_urls};

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
    fn configure_schema_inference(
        &mut self,
        _ctx: &dyn Session,
        _hadoop_paths: bool,
        _case_sensitive: bool,
    ) -> Result<SchemaSampling> {
        Ok(SchemaSampling::Sample)
    }

    /// Per-file column identity used to calculate and cache statistics.
    fn statistics_field_matching(&self) -> StructFieldMatching {
        StructFieldMatching::Name
    }

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
        object_store_url: &ObjectStoreUrl,
        object: &ObjectMeta,
        file_schema: SchemaRef,
        compression: CompressionTypeVariant,
    ) -> Result<ListingFileMeta> {
        let _ = (ctx, store, object_store_url, object, compression);
        Ok(ListingFileMeta {
            statistics: Statistics::new_unknown(&file_schema),
            ordering: None,
        })
    }

    /// Build a scan configuration for listing reads.
    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig>;

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
    pub(super) has_listed_file: bool,
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
        self.create_listing_source(ctx, info, None).await
    }

    async fn create_partitioned_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
        partitions: Vec<RegisteredPartition>,
    ) -> Result<Arc<dyn TableSource>> {
        self.create_listing_source(ctx, info, Some(partitions))
            .await
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
                    partition_by,
                    sort_by: sort_order,
                },
            )),
        }))
    }
}

impl<T: FormatFactory> ListingDataSource<T> {
    async fn create_listing_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
        partitions: Option<Vec<RegisteredPartition>>,
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

        let recursive = boolean_option(
            "recursiveFileLookup",
            read_option(&options, "recursiveFileLookup"),
            false,
        )?;
        if recursive && !partition_by.is_empty() {
            return plan_err!("recursiveFileLookup is not supported for a partitioned table");
        }
        let path = read_option(&options, "basePath");
        let mut read_format = T::read(ctx, options.clone())?;
        let hadoop_paths = path.is_some() || recursive;
        let sampling =
            read_format.configure_schema_inference(ctx, hadoop_paths, read_case_sensitive)?;
        let hadoop_paths = hadoop_paths
            || matches!(
                sampling,
                SchemaSampling::SingleParquet | SchemaSampling::MergeParquet { .. }
            );
        let registered = partitions.is_some();
        let (urls, values) = match partitions {
            Some(partitions) => partitions
                .into_iter()
                .map(|p| {
                    let url = ListingTableUrl::try_new(p.location, None)?;
                    Ok((vec![url], Some(p.values)))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip(),
            None => {
                let urls = resolve_read_urls(ctx, paths, hadoop_paths).await?;
                let values = vec![None; urls.iter().flatten().count()];
                (urls, values)
            }
        };
        let base_path = (!registered && hadoop_paths && !recursive)
            .then(|| resolve_base_path(ctx, read_case_sensitive, &options, &urls, path))
            .transpose()?;
        let path_glob_filter = read_format
            .path_glob_filter()
            .map(PathGlobFilter::parse)
            .transpose()?;
        let (sampled_files, discovery) = sample_listing_files(
            ctx,
            &urls,
            path_glob_filter.as_ref(),
            base_path.as_ref(),
            recursive || registered,
            sampling,
        )
        .await?;
        let explicit_schema = schema.filter(|s| !s.fields().is_empty());
        let inferred = discovery.fields(base_path.as_ref(), explicit_schema.as_ref())?;
        let inferred_names: Vec<_> = inferred.iter().map(|f| f.name().clone()).collect();
        let compression = read_format.infer_compression(ctx, &sampled_files).await?;

        let positional_partitions =
            !inferred.is_empty() && base_path.is_some() && matches!(T::name(), "csv" | "text");
        let has_explicit_schema = explicit_schema.is_some();
        let (schema, physical) = match explicit_schema {
            Some(schema) => {
                // Parquet resolves requested fields per file, including case-insensitive matches.
                let physical =
                    if T::name() == "parquet" || (read_case_sensitive && !positional_partitions) {
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
                    let partition_names: &[String] = if !registered && base_path.is_none() {
                        &[]
                    } else if partition_by.is_empty() {
                        &inferred_names
                    } else {
                        &partition_by
                    };
                    reconcile_schema_names_case_insensitive(schema, physical, partition_names)?
                } else {
                    // Keeps the user schema if physical schema inference is unavailable.
                    schema
                };
                (Arc::new(schema), physical)
            }
            None => {
                let format = T::name();
                if hadoop_paths
                    && matches!(format, "csv" | "json" | "parquet")
                    && !sampled_files.iter().any(|sample| {
                        !sample.objects.is_empty()
                            || (format != "parquet" && sample.has_listed_file)
                    })
                {
                    return plan_err!(
                        "[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for {format}. It must be specified manually."
                    );
                }
                let schema = read_format
                    .infer_schema(ctx, &sampled_files, compression)
                    .await?;
                (rewrite_unsupported_fields(schema), None)
            }
        };

        let (schema, partition_fields, output_columns) = if base_path.is_some()
            && partition_by.is_empty()
        {
            split_discovered_partitions(
                &schema,
                inferred,
                has_explicit_schema,
                read_case_sensitive,
                if positional_partitions && has_explicit_schema {
                    physical.as_deref()
                } else {
                    positional_partitions.then_some(schema.as_ref())
                },
            )
        } else if has_explicit_schema {
            let partition_by = if partition_by.is_empty() {
                inferred_names
                    .iter()
                    .filter(|name| {
                        schema
                            .fields()
                            .iter()
                            .any(|f| f.name().eq_ignore_ascii_case(name))
                    })
                    .cloned()
                    .collect()
            } else {
                partition_by
            };
            let (fields, schema) = get_partition_columns_and_file_schema(&schema, partition_by)?;
            (Arc::new(schema), fields, None)
        } else if partition_by.is_empty() {
            (schema, inferred, None)
        } else {
            (
                schema,
                partition_by
                    .into_iter()
                    .map(|col| Arc::new(Field::new(col, DataType::Utf8, false)))
                    .collect(),
                None,
            )
        };
        if !inferred_names.is_empty() && !partition_fields.is_empty() {
            if base_path.is_none() && urls.iter().flatten().any(|url| !url.is_collection()) {
                return plan_err!(
                    "Can't create a partitioned table backed by a single file, perhaps the URL is missing a trailing slash?"
                );
            }
            // Catalog tables may declare only a prefix of the discovered partitions.
            let names: Vec<_> = partition_fields.iter().map(|f| f.name()).collect();
            if names.len() > inferred_names.len()
                || names.iter().zip(&inferred_names).any(|(actual, expected)| {
                    !schema_names_match(
                        actual,
                        expected,
                        base_path.is_none() || read_case_sensitive,
                    )
                })
            {
                return plan_err!(
                    "Inferred partitions to be {inferred_names:?}, but got {names:?}"
                );
            }
        }

        let base_path = base_path
            .map(|options| {
                discovery
                    .resolve_values(&options, &partition_fields)
                    .map(Arc::new)
            })
            .transpose()?;
        let schema = TableSchema::builder(schema)
            .with_table_partition_cols(partition_fields)
            .build();
        let output_projection = output_columns
            .map(|names| {
                names
                    .iter()
                    .map(|name| schema.table_schema().index_of(name))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let source = ListingTableSource::try_new(ListingTableSourceConfig {
            output_projection,
            table_paths: urls.into_iter().flatten().zip(values).collect(),
            recursive_file_lookup: recursive,
            base_path,
            schema,
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
    Ok(store.list(Some(path.prefix())).try_next().await?.is_some())
}

// Reconciles a user-specified schema's field names with the physical file schema
// case-insensitively, matching Spark's default `spark.sql.caseSensitive=false`.
fn reconcile_schema_names_case_insensitive(
    schema: Schema,
    physical: &Schema,
    partitions: &[String],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        if partitions
            .iter()
            .any(|p| spark_case_insensitive_name_eq(p, name))
        {
            fields.push(Arc::clone(field));
            continue;
        }
        let mut matches = physical
            .fields()
            .iter()
            .filter(|f| spark_case_insensitive_name_eq(f.name(), name));
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
