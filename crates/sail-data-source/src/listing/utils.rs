use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::{ListingOptions, ListingTableConfig};
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::execution::cache::TableScopedPath;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, plan_err, DataFusionError, GetExt, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::ListingTableUrl;
use datafusion_session::Session;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::listing::source::ReadFormat;

#[derive(Debug, Clone)]
struct InferredPartitionType {
    seen_value: bool,
    all_i32: bool,
    all_i64: bool,
    all_f64: bool,
}

impl Default for InferredPartitionType {
    fn default() -> Self {
        Self {
            seen_value: false,
            all_i32: true,
            all_i64: true,
            all_f64: true,
        }
    }
}

fn update_inferred_partition_type(state: &mut InferredPartitionType, value: &str) {
    // Spark uses this marker to represent NULL partition values.
    if value.is_empty() || value == "__HIVE_DEFAULT_PARTITION__" {
        return;
    }
    state.seen_value = true;

    if state.all_i32 && value.parse::<i32>().is_err() {
        state.all_i32 = false;
    }
    if state.all_i64 && value.parse::<i64>().is_err() {
        state.all_i64 = false;
    }
    if state.all_f64 {
        match value.parse::<f64>() {
            Ok(v) if v.is_finite() => {}
            _ => state.all_f64 = false,
        }
    }
}

fn inferred_data_type(state: &InferredPartitionType) -> Option<DataType> {
    if !state.seen_value {
        return None;
    }
    if state.all_i32 {
        Some(DataType::Int32)
    } else if state.all_i64 {
        Some(DataType::Int64)
    } else if state.all_f64 {
        Some(DataType::Float64)
    } else {
        Some(DataType::Utf8)
    }
}

fn iter_partition_segments<'a>(path: &'a str) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
    path.split(object_store::path::DELIMITER)
        .filter_map(|segment| segment.split_once('='))
        .filter(|(key, _value)| !key.is_empty())
}

pub async fn resolve_listing_schema<R: ReadFormat>(
    ctx: &dyn Session,
    urls: &[ListingTableUrl],
    options: &mut ListingOptions,
    extension_with_compression: &Option<String>,
    read_format: &R,
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
        options,
        &file_groups,
        extension_with_compression,
        read_format,
    )?;

    let mut schemas = vec![];
    for (store, files) in file_groups.iter() {
        let schema_inferrer = read_format.schema_inferrer();
        let schema = schema_inferrer
            .get_schema(ctx, store, files, options)
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
fn apply_inferred_compression<R: ReadFormat>(
    options: &mut ListingOptions,
    file_groups: &[(Arc<dyn ObjectStore>, Vec<ObjectMeta>)],
    extension_with_compression: &Option<String>,
    read_format: &R,
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
            options.format = read_format.create_read_format(Some(file_compression_type))?;
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
pub async fn detect_listing_compression<R: ReadFormat>(
    ctx: &dyn Session,
    urls: &[ListingTableUrl],
    options: &mut ListingOptions,
    extension_with_compression: &Option<String>,
    read_format: &R,
) -> Result<()> {
    let file_groups = list_sample_files(ctx, urls, extension_with_compression.as_deref()).await?;
    if file_groups.iter().all(|(_, files)| files.is_empty()) {
        return Ok(());
    }
    apply_inferred_compression(
        options,
        &file_groups,
        extension_with_compression,
        read_format,
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
/// understood by the Spark client.
///
/// For Spark parity, we also attempt to infer concrete partition types (int / double / string)
/// from observed `key=value` directory segments, matching Spark's
/// `spark.sql.sources.partitionColumnTypeInference.enabled=true` default behavior.
pub async fn rewrite_listing_partitions(
    ctx: &dyn Session,
    mut config: ListingTableConfig,
) -> Result<ListingTableConfig> {
    let Some(options) = config.options.as_mut() else {
        return internal_err!("listing options should be present in the config");
    };

    if options.table_partition_cols.is_empty() {
        return Ok(config);
    }

    let mut col_index_by_name_lower: HashMap<String, usize> = HashMap::new();
    for (idx, (col, _)) in options.table_partition_cols.iter().enumerate() {
        col_index_by_name_lower.insert(col.to_ascii_lowercase(), idx);
    }

    let mut inferred = vec![InferredPartitionType::default(); options.table_partition_cols.len()];

    // Bound the amount of listing work per path to avoid pathological planning-time costs.
    // This mirrors DataFusion's own use of a small sample for inference paths.
    const MAX_FILES_PER_PATH: usize = 128;

    for url in &config.table_paths {
        let store = ctx.runtime_env().object_store(url)?;
        let mut files = list_all_files(url, ctx, &store, "", None).await?;
        let mut seen = 0usize;
        while let Some(meta) = files.try_next().await? {
            for (key, value) in iter_partition_segments(meta.location.as_ref()) {
                if let Some(&idx) = col_index_by_name_lower.get(&key.to_ascii_lowercase()) {
                    update_inferred_partition_type(&mut inferred[idx], value);
                }
            }
            seen += 1;
            if seen >= MAX_FILES_PER_PATH {
                break;
            }
        }
    }

    for (idx, (_col, data_type)) in options.table_partition_cols.iter_mut().enumerate() {
        // Only infer when the type is unknown / stringy. Explicit-schema types must be preserved.
        let should_infer = matches!(data_type, DataType::Dictionary(_, _) | DataType::Utf8);
        if should_infer {
            if let Some(dt) = inferred_data_type(&inferred[idx]) {
                *data_type = dt;
            } else if matches!(data_type, DataType::Dictionary(_, _)) {
                // No usable values observed; keep Spark-client compatibility.
                *data_type = DataType::Utf8;
            }
        }
    }

    Ok(config)
}
