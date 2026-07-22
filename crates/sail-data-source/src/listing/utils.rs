use std::sync::Arc;

use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::helpers::expr_applicable_for_cols;
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::logical_expr::Expr;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, GetExt, Result, internal_datafusion_err, plan_err};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_session::Session;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::listing::source::ListingFileSample;

pub fn rewrite_utf8view_fields(schema: Arc<Schema>) -> Arc<Schema> {
    // TODO: Spark doesn't support Utf8View
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if matches!(field.data_type(), &DataType::Utf8View) {
                field.as_ref().clone().with_data_type(DataType::Utf8)
            } else {
                field.as_ref().clone()
            }
        })
        .collect();

    Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
}

fn ends_with_ignore_ascii_case(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len()
        && s.as_bytes()[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
}

/// Infer file-level compression from file names.
///
/// This function returns a concrete compression (including "uncompressed") when *all* sampled files
/// end with the same compression suffix, or [`None`] if the file sample is empty.
/// This function returns an error when sampled files contain a mix of compressed and uncompressed
/// files or multiple compression types.
pub fn infer_listing_compression(
    files: &[ListingFileSample<'_>],
) -> Result<Option<CompressionTypeVariant>> {
    let mut inferred: Option<CompressionTypeVariant> = None;
    for group in files {
        for object in &group.objects {
            let path = object.location.as_ref();
            let compression = [
                CompressionTypeVariant::GZIP,
                CompressionTypeVariant::BZIP2,
                CompressionTypeVariant::XZ,
                CompressionTypeVariant::ZSTD,
            ]
            .into_iter()
            .find(|variant| {
                let ext = FileCompressionType::from(*variant).get_ext();
                ends_with_ignore_ascii_case(path, &ext)
            })
            .unwrap_or(CompressionTypeVariant::UNCOMPRESSED);

            match inferred {
                None => inferred = Some(compression),
                Some(x) if x == compression => {}
                Some(_) => return plan_err!("found mixed compression types"),
            }
        }
    }

    Ok(inferred)
}

/// List up to 10 files per URL into in-memory groups, suitable for schema inference, compression
/// inference, and partition inference.
///
/// File extensions are intentionally ignored since `ListingTableUrl` carries the filtering glob
/// already, and Spark reads every non-hidden file regardless of extension.
pub async fn sample_listing_files<'a>(
    ctx: &dyn Session,
    urls: &'a [ListingTableUrl],
) -> Result<Vec<ListingFileSample<'a>>> {
    let mut samples = vec![];
    for url in urls {
        let store = ctx.runtime_env().object_store(url)?;
        let objects: Vec<_> = list_all_files(url, ctx, store.as_ref())
            .await?
            // Empty files can't contribute to schema / partition inference and may error when read.
            .try_filter(|meta| futures::future::ready(meta.size > 0))
            .take(10)
            .try_collect()
            .await?;
        samples.push(ListingFileSample {
            url,
            store,
            objects,
        });
    }
    Ok(samples)
}

pub fn validate_partitions(
    files: &[ListingFileSample<'_>],
    partition_base_url: Option<&ListingTableUrl>,
    table_partition_fields: &[FieldRef],
) -> Result<()> {
    if table_partition_fields.is_empty() {
        return Ok(());
    }
    let inferred = infer_partitions(files, partition_base_url)?;
    if inferred.is_empty() {
        return Ok(());
    }

    for group in files {
        let url = partition_base_url.unwrap_or(group.url);
        if !url.is_collection() {
            return plan_err!(
                "Can't create a partitioned table backed by a single file, \
            perhaps the URL is missing a trailing slash?"
            );
        }

        let table_partition_names = table_partition_fields
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();

        if inferred.len() < table_partition_names.len() {
            return plan_err!(
                "Inferred partitions to be {:?}, but got {:?}",
                inferred,
                table_partition_names
            );
        }

        // Match prefix to allow creating tables with partial partitions.
        for (idx, col) in table_partition_names.iter().enumerate() {
            if inferred.get(idx) != Some(col) {
                return plan_err!(
                    "Inferred partitions to be {:?}, but got {:?}",
                    inferred,
                    table_partition_names
                );
            }
        }
    }
    Ok(())
}

pub fn infer_partitions(
    files: &[ListingFileSample<'_>],
    partition_base_url: Option<&ListingTableUrl>,
) -> Result<Vec<String>> {
    let mut inferred: Option<Vec<String>> = None;
    for group in files {
        for file in &group.objects {
            let url = partition_base_url.unwrap_or(group.url);
            let path_parts = url
                .strip_prefix(&file.location)
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "failed to strip partition base path from object location: {}",
                        file.location
                    )
                })?
                .collect::<Vec<_>>();

            let keys = path_parts
                .into_iter()
                .rev()
                .skip(1) // get parents only and skip the file itself
                .rev()
                .filter(|s| s.contains('='))
                .map(|s| s.split('=').next().unwrap_or("").to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();

            match &mut inferred {
                None => inferred = Some(keys),
                Some(x) if x == &keys => {}
                Some(x) => {
                    return plan_err!("found mixed partition values {x:?} and {keys:?}");
                }
            }
        }
    }

    Ok(inferred.unwrap_or_default())
}

pub async fn list_all_files<'a>(
    url: &'a ListingTableUrl,
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    let exec_options = &ctx.config_options().execution;
    let ignore_subdirectory = exec_options.listing_table_ignore_subdirectory;
    // If the prefix is a file, use a head request, otherwise use a list request.
    let list = match url.is_collection() {
        true => match ctx.runtime_env().cache_manager.get_list_files_cache() {
            None => store.list(Some(url.prefix())),
            Some(cache) => {
                let key = TableScopedPath {
                    table: None,
                    path: url.prefix().clone(),
                };
                if let Some(res) = cache.get(&key) {
                    debug!("Hit list all files cache");
                    futures::stream::iter(res.files.as_ref().clone().into_iter().map(Ok)).boxed()
                } else {
                    let list_res = store.list(Some(url.prefix()));
                    let vec = list_res.try_collect::<Vec<ObjectMeta>>().await?;
                    cache.put(&key, CachedFileList::new(vec.clone()));
                    futures::stream::iter(vec.into_iter().map(Ok)).boxed()
                }
            }
        },
        false => futures::stream::once(store.head(url.prefix())).boxed(),
    };
    Ok(list
        .try_filter(move |meta| {
            let path = &meta.location;
            let glob_match = url.contains(path, ignore_subdirectory);
            futures::future::ready(glob_match)
        })
        .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
        .boxed())
}

pub fn can_be_evaluated_for_partition_pruning(
    partition_column_names: &[&str],
    expr: &Expr,
) -> bool {
    !partition_column_names.is_empty() && expr_applicable_for_cols(partition_column_names, expr)
}
