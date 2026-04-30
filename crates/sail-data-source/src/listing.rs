use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::execution::cache::TableScopedPath;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, plan_err, DataFusionError, GetExt, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::listing::{ListingFormat, ListingTableFormat};

pub async fn resolve_listing_schema<T: ListingFormat>(
    ctx: &dyn Session,
    urls: &[ListingTableUrl],
    options: &mut ListingOptions,
    extension_with_compression: &Option<String>,
    options_vec: Vec<OptionLayer>,
    listing_format: &ListingTableFormat<T>,
) -> Result<Arc<Schema>> {
    let file_groups = list_sample_files(ctx, urls, extension_with_compression.as_deref()).await?;
    let empty = file_groups.iter().all(|(_, files)| files.is_empty());
    if empty {
        let urls = urls
            .iter()
            .map(|url| url.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return plan_err!("No files found in the specified paths: {urls}")?;
    }

    apply_inferred_compression(
        ctx,
        options,
        &file_groups,
        extension_with_compression,
        &options_vec,
        listing_format,
    )?;

    let mut schemas = vec![];
    for (store, files) in file_groups.iter() {
        let schema_inferrer = listing_format.inner().schema_inferrer();
        let schema = schema_inferrer
            .get_schema(ctx, store, files, options, &options_vec)
            .await?;
        schemas.push(schema);
    }
    let schema = Schema::try_merge(schemas)?;

    // TODO: Spark doesn't support Utf8View
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if matches!(field.data_type(), &DataType::Utf8View) {
                let mut new_field = field.as_ref().clone();
                new_field = new_field.with_data_type(DataType::Utf8);
                new_field
            } else {
                field.as_ref().clone()
            }
        })
        .collect();

    Ok(Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    )))
}

/// Like [`str::ends_with`], but ignores ASCII case so paths such as
/// `data.CSV` match the lowercase `.csv` extension that upstream
/// `FileFormat` implementations report. Spark behaves the same way.
fn ends_with_ignore_ascii_case(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len()
        && s.as_bytes()[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
}

/// List up to 10 files per URL into in-memory groups, suitable for schema
/// inference and compression detection.
///
/// The base file-extension filter is intentionally cleared (passed as `""`)
/// so callers see every non-hidden file in the directory — matching Spark's
/// behavior of reading every file regardless of extension. The hidden-file
/// glob attached in [`crate::url::resolve_listing_urls`] still excludes
/// `_*` / `.*` files. `extension_with_compression` is preserved so that an
/// explicitly requested compressed variant still narrows the listing.
async fn list_sample_files(
    ctx: &dyn Session,
    urls: &[ListingTableUrl],
    extension_with_compression: Option<&str>,
) -> Result<Vec<(Arc<dyn ObjectStore>, Vec<ObjectMeta>)>> {
    // The logic is similar to `ListingOptions::infer_schema()`
    // but here we also check for the existence of files.
    let mut file_groups = vec![];
    for url in urls {
        let store = ctx.runtime_env().object_store(url)?;
        // Pass `""` so the base extension filter accepts every path (Spark parity).
        let files: Vec<_> = list_all_files(url, ctx, &store, "", extension_with_compression)
            .await?
            // Here we sample up to 10 files to infer the schema.
            // The value is hard-coded here since DataFusion uses the same hard-coded value
            // for operations such as `infer_partitions_from_path`.
            // We can make it configurable if DataFusion makes those operations configurable
            // as well in the future.
            .take(10)
            .try_collect()
            .await?;
        file_groups.push((store, files));
    }
    Ok(file_groups)
}

/// Inspect the suffixes in `file_groups` and, if a compressed variant is
/// observed, rebuild `options.format` with the matching compression and
/// update `options.file_extension`. Compression is carried by the
/// `FileFormat` itself (DataFusion consults `format.compression_type()`
/// when reading bytes), which is why we re-create the format here.
fn apply_inferred_compression<T: ListingFormat>(
    ctx: &dyn Session,
    options: &mut ListingOptions,
    file_groups: &[(Arc<dyn ObjectStore>, Vec<ObjectMeta>)],
    extension_with_compression: &Option<String>,
    options_vec: &[OptionLayer],
    listing_format: &ListingTableFormat<T>,
) -> Result<()> {
    let file_extension = if let Some(extension_with_compression) = extension_with_compression {
        resolve_listing_file_extension(
            file_groups,
            &options.file_extension,
            extension_with_compression,
        )
    } else {
        let result = infer_listing_file_extension(file_groups, &options.file_extension);
        if let Some(result) = result {
            let (file_extension, compression_type) = result;
            let file_compression_type = CompressionTypeVariant::from_str(
                compression_type
                    .strip_prefix(".")
                    .unwrap_or(compression_type.as_str()),
            )?;
            options.format = listing_format.inner().create_read_format(
                ctx,
                options_vec.to_vec(),
                Some(file_compression_type),
            )?;
            Some(file_extension)
        } else {
            None
        }
    };
    if let Some(file_extension) = file_extension.clone() {
        options.file_extension = file_extension;
    };
    Ok(())
}

/// Best-effort compression auto-detection for callers that bypass
/// [`resolve_listing_schema`] (i.e. an explicit schema was provided).
/// Lists a sample of files and applies any detected compression to
/// `options`. Silently no-ops when the listing is empty so the
/// downstream scan can surface "no files found" with full context.
pub async fn detect_listing_compression<T: ListingFormat>(
    ctx: &dyn Session,
    urls: &[ListingTableUrl],
    options: &mut ListingOptions,
    extension_with_compression: &Option<String>,
    options_vec: &[OptionLayer],
    listing_format: &ListingTableFormat<T>,
) -> Result<()> {
    let file_groups = list_sample_files(ctx, urls, extension_with_compression.as_deref()).await?;
    if file_groups.iter().all(|(_, files)| files.is_empty()) {
        return Ok(());
    }
    apply_inferred_compression(
        ctx,
        options,
        &file_groups,
        extension_with_compression,
        options_vec,
        listing_format,
    )
}

fn resolve_listing_file_extension(
    file_groups: &[(Arc<dyn ObjectStore>, Vec<ObjectMeta>)],
    file_extension: &str,
    extension_with_compression: &str,
) -> Option<String> {
    // TODO: compression detection only fires when a file's base extension
    //  matches the format's canonical one (e.g. `.csv` for CSV). Files like
    //  `data.tsv.gz` won't get GZIP applied automatically. Spark detects
    //  compression from any trailing `.gz` / `.bz2` / `.xz` / `.zstd`
    //  suffix regardless of base.
    let mut count_with_compression = 0;
    let mut count_without_compression = 0;
    for (_, object_metas) in file_groups {
        for object_meta in object_metas {
            let path = &object_meta.location;
            if ends_with_ignore_ascii_case(path.as_ref(), extension_with_compression) {
                count_with_compression += 1;
            } else if ends_with_ignore_ascii_case(path.as_ref(), file_extension) {
                count_without_compression += 1;
            }
        }
    }
    if count_with_compression > count_without_compression {
        Some(extension_with_compression.to_string())
    } else {
        None
    }
}

fn infer_listing_file_extension(
    file_groups: &[(Arc<dyn ObjectStore>, Vec<ObjectMeta>)],
    file_extension: &str,
) -> Option<(String, String)> {
    // TODO: compression detection only fires when a file's base extension
    //  matches the format's canonical one (e.g. `.csv` for CSV). Files like
    //  `data.tsv.gz` won't get GZIP applied automatically. Spark detects
    //  compression from any trailing `.gz` / `.bz2` / `.xz` / `.zstd`
    //  suffix regardless of base.
    let mut counts: HashMap<(String, String), usize> = HashMap::new();
    let mut base_count = 0;
    for (_, object_metas) in file_groups {
        for object_meta in object_metas {
            let path = &object_meta.location;
            if ends_with_ignore_ascii_case(path.as_ref(), file_extension) {
                base_count += 1;
            }
            for c in [
                FileCompressionType::from(CompressionTypeVariant::GZIP),
                FileCompressionType::from(CompressionTypeVariant::BZIP2),
                FileCompressionType::from(CompressionTypeVariant::XZ),
                FileCompressionType::from(CompressionTypeVariant::ZSTD),
            ] {
                let compression_ext = c.get_ext();
                let candidate = format!("{file_extension}{compression_ext}");
                if ends_with_ignore_ascii_case(path.as_ref(), &candidate) {
                    *counts.entry((candidate, compression_ext)).or_default() += 1;
                }
            }
        }
    }

    // If ALL files do not end with the plain base extension, pick the most common compressed one.
    if base_count == 0 && !counts.is_empty() {
        counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(ext, _)| ext)
    } else {
        None
    }
}

/// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
pub async fn list_all_files<'a>(
    url: &'a ListingTableUrl,
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    file_extension: &'a str,
    extension_with_compression: Option<&'a str>,
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
            let extension_with_compression_match = extension_with_compression
                .is_some_and(|ext| ends_with_ignore_ascii_case(path.as_ref(), ext));
            let extension_match = ends_with_ignore_ascii_case(path.as_ref(), file_extension)
                || extension_with_compression_match;
            let extension_match = if !extension_match && extension_with_compression.is_none() {
                [
                    FileCompressionType::from(CompressionTypeVariant::GZIP),
                    FileCompressionType::from(CompressionTypeVariant::BZIP2),
                    FileCompressionType::from(CompressionTypeVariant::XZ),
                    FileCompressionType::from(CompressionTypeVariant::ZSTD),
                ]
                .iter()
                .any(|c| {
                    let candidate = format!("{file_extension}{}", c.get_ext());
                    ends_with_ignore_ascii_case(path.as_ref(), &candidate)
                })
            } else {
                extension_match
            };
            let glob_match = url.contains(path, ignore_subdirectory);
            futures::future::ready(extension_match && glob_match)
        })
        .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
        .boxed())
}

/// The inferred partition columns are of `Dictionary` types by default, which cannot be
/// understood by the Spark client. So we rewrite the type to be `Utf8`.
pub fn rewrite_listing_partitions(mut config: ListingTableConfig) -> Result<ListingTableConfig> {
    let Some(options) = config.options.as_mut() else {
        return internal_err!("listing options should be present in the config");
    };
    options
        .table_partition_cols
        .iter_mut()
        .for_each(|(_col, data_type)| {
            // FIXME: infer concrete partition types (Int / Float / String) from observed values to match Spark's `partitionColumnTypeInference`.
            if matches!(data_type, DataType::Dictionary(_, _)) {
                *data_type = DataType::Utf8;
            }
        });
    Ok(config)
}
