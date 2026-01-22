use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{internal_err, plan_err, DataFusionError, GetExt, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::{ObjectMeta, ObjectStore};

use crate::formats::listing::{ListingFormat, ListingTableFormat};

pub async fn resolve_listing_schema<T: ListingFormat>(
    ctx: &dyn Session,
    urls: &[ListingTableUrl],
    options: &mut ListingOptions,
    extension_with_compression: &Option<String>,
    options_vec: Vec<HashMap<String, String>>,
    listing_format: &ListingTableFormat<T>,
) -> Result<Arc<Schema>> {
    // The logic is similar to `ListingOptions::infer_schema()`
    // but here we also check for the existence of files.
    let mut file_groups = vec![];
    for url in urls {
        let store = ctx.runtime_env().object_store(url)?;
        let files: Vec<_> = list_all_files(
            url,
            ctx,
            &store,
            &options.file_extension,
            extension_with_compression.as_deref(),
        )
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

    let empty = file_groups.iter().all(|(_, files)| files.is_empty());
    if empty {
        let urls = urls
            .iter()
            .map(|url| url.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return plan_err!("No files found in the specified paths: {urls}")?;
    }

    let file_extension = if let Some(extension_with_compression) = extension_with_compression {
        resolve_listing_file_extension(
            &file_groups,
            &options.file_extension,
            extension_with_compression,
        )
    } else {
        let result = infer_listing_file_extension(&file_groups, &options.file_extension);
        if let Some(result) = result {
            let (file_extension, compression_type) = result;
            let file_compression_type = CompressionTypeVariant::from_str(
                compression_type
                    .strip_prefix(".")
                    .unwrap_or(compression_type.as_str()),
            )?;
            options.format = listing_format.inner().create_read_format(
                ctx,
                options_vec.clone(),
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

    let mut schemas = vec![];
    for (store, files) in file_groups.iter() {
        let schema_inferrer = listing_format.inner().schema_inferrer();
        let schema = schema_inferrer
            .get_schema(ctx, store, files, options, &options_vec)
            .await?;
        schemas.push(schema);
    }
    let schema = Schema::try_merge(schemas)?;

    // FIXME: DataFusion 43.0.0 suddenly doesn't support Utf8View
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

fn resolve_listing_file_extension(
    file_groups: &[(Arc<dyn ObjectStore>, Vec<ObjectMeta>)],
    file_extension: &str,
    extension_with_compression: &str,
) -> Option<String> {
    // TODO: Future work can support reading all files of the same `FileFormat` regardless of the file extension.
    let mut count_with_compression = 0;
    let mut count_without_compression = 0;
    for (_, object_metas) in file_groups {
        for object_meta in object_metas {
            let path = &object_meta.location;
            if path.as_ref().ends_with(extension_with_compression) {
                count_with_compression += 1;
            } else if path.as_ref().ends_with(file_extension) {
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
    // TODO: Future work can support reading all files of the same `FileFormat` regardless of the file extension.
    let mut counts: HashMap<(String, String), usize> = HashMap::new();
    let mut base_count = 0;
    for (_, object_metas) in file_groups {
        for object_meta in object_metas {
            let path = &object_meta.location;
            if path.as_ref().ends_with(file_extension) {
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
                if path.as_ref().ends_with(&candidate) {
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
                if let Some(res) = cache.get(url.prefix()) {
                    debug!("Hit list all files cache");
                    futures::stream::iter(res.as_ref().clone().into_iter().map(Ok)).boxed()
                } else {
                    let list_res = store.list(Some(url.prefix()));
                    let vec = list_res.try_collect::<Vec<ObjectMeta>>().await?;
                    cache.put(url.prefix(), Arc::new(vec.clone()));
                    futures::stream::iter(vec.into_iter().map(Ok)).boxed()
                }
            }
        },
        false => futures::stream::once(store.head(url.prefix())).boxed(),
    };
    Ok(list
        .try_filter(move |meta| {
            let path = &meta.location;
            let extension_with_compression_match =
                extension_with_compression.is_some_and(|ext| path.as_ref().ends_with(ext));
            let extension_match =
                path.as_ref().ends_with(file_extension) || extension_with_compression_match;
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
                    path.as_ref().ends_with(&candidate)
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
            if matches!(data_type, DataType::Dictionary(_, _)) {
                *data_type = DataType::Utf8;
            }
        });
    Ok(config)
}
