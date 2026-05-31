use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::helpers::expr_applicable_for_cols;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::execution::cache::TableScopedPath;
use datafusion::logical_expr::Expr;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{plan_err, DataFusionError, GetExt, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::ListingTableUrl;
use datafusion_session::Session;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

pub struct ListingFileSample<'a> {
    pub url: &'a ListingTableUrl,
    pub store: Arc<dyn ObjectStore>,
    pub objects: Vec<ObjectMeta>,
}

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
/// Returns:
/// - `UNCOMPRESSED` when no sampled file ends with a known compression suffix.
/// - A concrete compression when *all* sampled files end with the same compression suffix.
/// - An error when sampled files contain a mix of compressed/uncompressed or multiple compression
///   types (DataFusion scan configuration accepts only a single compression type).
pub fn infer_listing_compression(objects: &[ObjectMeta]) -> Result<CompressionTypeVariant> {
    if objects.is_empty() {
        return Ok(CompressionTypeVariant::UNCOMPRESSED);
    }

    let mut inferred: Option<CompressionTypeVariant> = None;
    for object in objects {
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

        if compression == CompressionTypeVariant::UNCOMPRESSED {
            if inferred.is_some() {
                return plan_err!("Found mixed compression types on disk");
            }
            continue;
        }

        match inferred {
            None => inferred = Some(compression),
            Some(prev) if prev == compression => {}
            Some(_) => return plan_err!("Found mixed compression types on disk"),
        }
    }

    Ok(inferred.unwrap_or(CompressionTypeVariant::UNCOMPRESSED))
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
    let mut file_samples = vec![];
    for url in urls {
        let store = ctx.runtime_env().object_store(url)?;
        let objects: Vec<_> = list_all_files(url, ctx, store.as_ref())
            .await?
            // Empty files can't contribute to schema / partition inference and may error when read.
            .try_filter(|meta| futures::future::ready(meta.size > 0))
            .take(10)
            .try_collect()
            .await?;
        file_samples.push(ListingFileSample {
            url,
            store,
            objects,
        });
    }
    Ok(file_samples)
}

pub fn infer_partition_names(
    table_path: &ListingTableUrl,
    objects: &[ObjectMeta],
) -> Result<Vec<String>> {
    if objects.is_empty() {
        return Ok(vec![]);
    }

    let mut inferred: Option<Vec<String>> = None;
    for file in objects {
        let path_parts = table_path
            .strip_prefix(&file.location)
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "failed to strip listing prefix from object location: {}",
                    file.location
                )
            })?
            .collect::<Vec<_>>();

        let keys = path_parts
            .into_iter()
            .rev()
            .skip(1) // get parents only; skip the file itself
            .rev()
            // Partitions are expected to follow the format "column_name=value", so we ignore parts
            // that don't match.
            .filter(|s| s.contains('='))
            .map(|s| s.split('=').next().unwrap_or("").to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        match &mut inferred {
            None => inferred = Some(keys),
            Some(current) if current == &keys => {}
            Some(current) => {
                let mut diff = [current.clone(), keys];
                diff.sort();
                return plan_err!("Found mixed partition values on disk {:?}", diff);
            }
        }
    }

    Ok(inferred.unwrap_or_default())
}

fn validate_partitions_for_url(
    table_path: &ListingTableUrl,
    objects: &[ObjectMeta],
    table_partition_cols: &[(String, DataType)],
) -> Result<()> {
    if table_partition_cols.is_empty() {
        return Ok(());
    }
    if objects.is_empty() {
        return Ok(());
    }
    if !table_path.is_collection() {
        return plan_err!(
            "Can't create a partitioned table backed by a single file, \
            perhaps the URL is missing a trailing slash?"
        );
    }

    let inferred = infer_partition_names(table_path, objects)?;
    if inferred.is_empty() {
        return Ok(());
    }

    let table_partition_names = table_partition_cols
        .iter()
        .map(|(col_name, _)| col_name.clone())
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

    Ok(())
}

pub fn validate_partitions(
    files: &[ListingFileSample<'_>],
    table_partition_cols: &[(String, DataType)],
) -> Result<()> {
    for file_sample in files {
        validate_partitions_for_url(file_sample.url, &file_sample.objects, table_partition_cols)?;
    }
    Ok(())
}

fn infer_partitions_for_url(
    table_path: &ListingTableUrl,
    objects: &[ObjectMeta],
) -> Result<Vec<(String, DataType)>> {
    let mut partitions = infer_partition_names(table_path, objects)?
        .into_iter()
        .map(|col_name| {
            (
                col_name,
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            )
        })
        .collect::<Vec<_>>();

    partitions.iter_mut().for_each(|(_col, data_type)| {
        // FIXME: infer concrete partition types (Int / Float / String) from observed values to match Spark's `partitionColumnTypeInference`.
        if matches!(data_type, DataType::Dictionary(_, _)) {
            *data_type = DataType::Utf8;
        }
    });

    Ok(partitions)
}

pub fn infer_partitions(files: &[ListingFileSample<'_>]) -> Result<Vec<(String, DataType)>> {
    let Some(sample) = files.iter().find(|sample| !sample.objects.is_empty()) else {
        return Ok(vec![]);
    };
    infer_partitions_for_url(sample.url, &sample.objects)
}

pub async fn list_all_files<'a>(
    url: &'a ListingTableUrl,
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    let exec_options = &ctx.config_options().execution;
    let ignore_subdirectory = exec_options.listing_table_ignore_subdirectory;
    // If the prefix is a file, use a head request, otherwise list
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
