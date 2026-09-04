use std::sync::Arc;

use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::listing::source::ListingFileSample;
use crate::url::PathGlobFilter;

/// Rewrites inferred field types that have no Spark counterpart.
///
/// Spark's type system is narrower than Arrow's, and the Arrow-to-Spark conversion rejects
/// what it cannot represent. Coercing the inferred schema here, rather than letting the
/// conversion fail later, keeps files readable that DataFusion can already read: the listing
/// table casts each file to this schema as it scans.
pub fn rewrite_unsupported_fields(schema: Arc<Schema>) -> Arc<Schema> {
    Arc::new(normalize_unsupported_fields(&schema))
}

/// Merges per-file inferred schemas, normalizing each one before the merge.
///
/// [`Schema::try_merge`] rejects fields whose data types differ, so the normalization has to
/// happen before the merge rather than after it. A directory holding the same column as
/// `timestamp[ms]` in one file and `timestamp[us]` in another would otherwise fail to merge,
/// even though Spark represents both as a single timestamp type, and that failure would occur
/// before [`rewrite_unsupported_fields`] ever ran.
pub fn try_merge_normalized(schemas: impl IntoIterator<Item = Schema>) -> Result<Schema> {
    Ok(Schema::try_merge(
        schemas
            .into_iter()
            .map(|schema| normalize_unsupported_fields(&schema)),
    )?)
}

fn normalize_unsupported_fields(schema: &Schema) -> Schema {
    // TODO: Apply Spark-compatible type normalization recursively inside structs, lists, and
    // maps. Only top-level fields are normalized today, so nested millisecond timestamps remain
    // unsupported even though Spark accepts them (SPARK recurses through nested timestamp leaves).
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            // TODO: Spark doesn't support Utf8View
            DataType::Utf8View => field.as_ref().clone().with_data_type(DataType::Utf8),
            // Spark timestamps are microseconds, so second and millisecond timestamps are
            // widened here; the conversion would otherwise reject them even though the
            // widening is lossless. Nanoseconds are left alone so that they are still
            // reported rather than silently truncated, which matches Spark: its Parquet
            // reader accepts `MILLIS` and `MICROS` but not `NANOS` (SPARK-40819).
            DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, tz) => field
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            _ => field.as_ref().clone(),
        })
        .collect();

    Schema::new_with_metadata(new_fields, schema.metadata().clone())
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
    path_glob_filter: Option<&'a PathGlobFilter>,
) -> Result<Vec<ListingFileSample<'a>>> {
    let mut samples = vec![];
    for url in urls {
        let store = ctx.runtime_env().object_store(url)?;
        let objects: Vec<_> = list_all_files(url, ctx, store.as_ref(), path_glob_filter)
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
    table_partition_fields: &[FieldRef],
) -> Result<()> {
    if table_partition_fields.is_empty() {
        return Ok(());
    }
    let inferred = infer_partitions(files)?;
    if inferred.is_empty() {
        return Ok(());
    }

    for group in files {
        if !group.url.is_collection() {
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

pub fn infer_partitions(files: &[ListingFileSample<'_>]) -> Result<Vec<String>> {
    let mut inferred: Option<Vec<String>> = None;
    for group in files {
        for file in &group.objects {
            let path_parts = group
                .url
                .strip_prefix(&file.location)
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "failed to strip listing prefix from object location: {}",
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
    path_glob_filter: Option<&'a PathGlobFilter>,
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
            let included = url.contains(path, ignore_subdirectory)
                && !has_hidden_path_component(url, path)
                && matches_path_glob_filter(path_glob_filter, path);
            futures::future::ready(included)
        })
        .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
        .boxed())
}

pub fn matches_path_glob_filter(
    path_glob_filter: Option<&PathGlobFilter>,
    location: &Path,
) -> bool {
    path_glob_filter.is_none_or(|filter| {
        location
            .filename()
            .is_some_and(|filename| filter.matches(filename))
    })
}

/// Returns `true` if the path is hidden per Spark's `HadoopFSUtils.shouldFilterOutPathName`.
pub fn has_hidden_path_component(url: &ListingTableUrl, location: &Path) -> bool {
    let is_hidden = |name: &str| {
        let exclude = (name.starts_with('_') && !name.contains('='))
            || name.starts_with('.')
            || name.ends_with("._COPYING_");
        let keep = name.starts_with("_common_metadata") || name.starts_with("_metadata");
        exclude && !keep
    };
    url.strip_prefix(location)
        .is_some_and(|mut segments| segments.any(is_hidden))
        || location.filename().is_some_and(is_hidden)
}

pub fn can_be_evaluated_for_partition_pruning(
    partition_column_names: &[&str],
    expr: &Expr,
) -> bool {
    !partition_column_names.is_empty() && expr_applicable_for_cols(partition_column_names, expr)
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_has_hidden_path_component() {
        let dir = ListingTableUrl::parse("file:///data/").unwrap();
        let hidden = |path: &str| has_hidden_path_component(&dir, &Path::from(path));

        // Data files and partition directories are kept.
        assert!(!hidden("data/part-0.parquet"));
        assert!(!hidden("data/year=2020/part-0.parquet"));

        // Hidden markers and hidden directories are excluded.
        assert!(hidden("data/_SUCCESS"));
        assert!(hidden("data/.hidden.json"));
        assert!(hidden("data/_temporary/0/part-0.parquet"));
        assert!(hidden("data/visible/_hidden/bad.json"));

        // Files mid-copy (Hadoop `._COPYING_`) are excluded.
        assert!(hidden("data/part-0.parquet._COPYING_"));

        // Spark keeps the Parquet summary files.
        assert!(!hidden("data/_metadata"));
        assert!(!hidden("data/_common_metadata"));

        // Spark keeps `_`-prefixed partition directories (they contain `=`).
        assert!(!hidden("data/_part=1/part-0.parquet"));

        // A hidden listing root is not itself filtered.
        let hidden_root = ListingTableUrl::parse("file:///_root/").unwrap();
        assert!(!has_hidden_path_component(
            &hidden_root,
            &Path::from("_root/part-0.parquet")
        ));

        // A location outside the prefix falls back to judging its file name.
        assert!(!hidden("outside/part-0.parquet"));
        assert!(hidden("outside/_SUCCESS"));

        // An explicitly targeted hidden file is excluded.
        let file = ListingTableUrl::parse("file:///data/_data.json").unwrap();
        assert!(has_hidden_path_component(
            &file,
            &Path::from("data/_data.json")
        ));
    }

    #[test]
    fn test_rewrite_unsupported_fields() {
        let utc = Some("UTC".into());
        let metadata = HashMap::from([("k".to_string(), "v".to_string())]);
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("view", DataType::Utf8View, true),
                Field::new(
                    "ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )
                .with_metadata(metadata.clone()),
                Field::new(
                    "ms_tz",
                    DataType::Timestamp(TimeUnit::Millisecond, utc.clone()),
                    true,
                ),
                Field::new("s", DataType::Timestamp(TimeUnit::Second, None), true),
                Field::new("us", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new("ns", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                Field::new("other", DataType::Int64, true),
            ])
            .with_metadata(metadata.clone()),
        );
        let schema = rewrite_unsupported_fields(schema);
        let field = |name: &str| schema.field_with_name(name).unwrap().clone();

        // Spark has no `Utf8View` type.
        assert_eq!(field("view").data_type(), &DataType::Utf8);

        // Second and millisecond timestamps widen to microseconds, keeping the time zone.
        let us = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(field("ms").data_type(), &us);
        assert_eq!(field("s").data_type(), &us);
        assert_eq!(
            field("ms_tz").data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, utc)
        );

        // Microsecond timestamps and unrelated types are left alone.
        assert_eq!(field("us").data_type(), &us);
        assert_eq!(field("other").data_type(), &DataType::Int64);

        // Nanoseconds are still reported rather than silently truncated.
        assert_eq!(
            field("ns").data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        // Rewriting a field preserves its nullability and metadata, and the schema keeps
        // its own metadata.
        assert!(!field("ms").is_nullable());
        assert_eq!(field("ms").metadata(), &metadata);
        assert_eq!(schema.metadata(), &metadata);
    }

    #[test]
    fn test_try_merge_normalized_mixed_timestamp_units() {
        let ts = |unit| {
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(unit, None),
                true,
            )])
        };

        // Files that disagree only on timestamp unit merge into a single microsecond field.
        let merged =
            try_merge_normalized([ts(TimeUnit::Millisecond), ts(TimeUnit::Microsecond)]).unwrap();
        assert_eq!(
            merged.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        // Without normalizing first, that same merge fails - which is what this guards against.
        assert!(Schema::try_merge([ts(TimeUnit::Millisecond), ts(TimeUnit::Microsecond)]).is_err());

        // `Utf8View` and `Utf8` reconcile the same way.
        let merged = try_merge_normalized([
            Schema::new(vec![Field::new("s", DataType::Utf8View, true)]),
            Schema::new(vec![Field::new("s", DataType::Utf8, true)]),
        ])
        .unwrap();
        assert_eq!(merged.field(0).data_type(), &DataType::Utf8);

        // Nanoseconds are deliberately left alone, so they still conflict.
        assert!(
            try_merge_normalized([ts(TimeUnit::Nanosecond), ts(TimeUnit::Microsecond)]).is_err()
        );
    }
}
