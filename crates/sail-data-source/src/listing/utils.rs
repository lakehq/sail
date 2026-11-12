use std::sync::Arc;

use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::listing::helpers::{
    evaluate_partition_prefix, filter_partitioned_file,
};
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::logical_expr::{Expr, Volatility};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{
    DFSchema, DataFusionError, GetExt, Result, ScalarValue, internal_datafusion_err, plan_err,
};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::{ListingTableUrl, PartitionedFile};
use datafusion_session::Session;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::listing::commit::partition_key;
use crate::listing::source::ListingFileSample;
use crate::url::PathGlobFilter;

/// What normalization does with an inferred nanosecond timestamp.
///
/// Spark has no nanosecond timestamp type, so a nanosecond column has to be either rejected or
/// truncated. Which one matches Spark depends on the format, so the format's reader chooses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NanosecondTimestamps {
    /// Leave nanoseconds alone, so that the Arrow-to-Spark conversion reports them rather than
    /// losing precision silently. This is what Spark's Parquet reader does: it accepts `MILLIS`
    /// and `MICROS` but rejects `NANOS` (SPARK-40819).
    Report,
    /// Widen nanoseconds to microseconds, discarding the last three digits. This is what Spark's
    /// text-format readers do: given `2018-05-01 00:00:05.123456789`, Spark's CSV reader with
    /// `inferSchema` infers a timestamp and stores `.123456` (verified on Spark 4.0.1; its JSON
    /// reader with `inferTimestamp` does the same).
    WidenToMicrosecond,
}

/// Rewrites inferred field types that have no Spark counterpart.
///
/// Spark's type system is narrower than Arrow's, and the Arrow-to-Spark conversion rejects
/// what it cannot represent. Coercing the inferred schema here, rather than letting the
/// conversion fail later, keeps files readable that DataFusion can already read: the listing
/// table casts each file to this schema as it scans.
///
/// This runs on a schema a format has already inferred and normalized, so it applies the
/// conservative [`NanosecondTimestamps::Report`] policy; widening, where the format wants it, has
/// happened in [`try_merge_normalized`] already.
pub fn rewrite_unsupported_fields(schema: Arc<Schema>) -> Arc<Schema> {
    Arc::new(normalize_unsupported_fields(
        &schema,
        NanosecondTimestamps::Report,
    ))
}

/// Merges per-file inferred schemas, normalizing each one before the merge.
///
/// [`Schema::try_merge`] rejects fields whose data types differ, so the normalization has to
/// happen before the merge rather than after it. A directory holding the same column as
/// `timestamp[ms]` in one file and `timestamp[us]` in another would otherwise fail to merge,
/// even though Spark represents both as a single timestamp type, and that failure would occur
/// before [`rewrite_unsupported_fields`] ever ran.
pub fn try_merge_normalized(
    schemas: impl IntoIterator<Item = Schema>,
    nanoseconds: NanosecondTimestamps,
) -> Result<Schema> {
    Ok(Schema::try_merge(schemas.into_iter().map(|schema| {
        normalize_unsupported_fields(&schema, nanoseconds)
    }))?)
}

fn normalize_unsupported_fields(schema: &Schema, nanoseconds: NanosecondTimestamps) -> Schema {
    // TODO: Apply Spark-compatible type normalization recursively inside structs, lists, and
    // maps. Only top-level fields are normalized today, so nested millisecond timestamps remain
    // unsupported even though Spark accepts them (SPARK recurses through nested timestamp leaves).
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            // Spark has neither Utf8View nor BinaryView, so coerce view types to their plain
            // counterparts. This also lets a directory that mixes a view file with a plain one
            // (e.g. a raw write followed by an INSERT) merge, since `Schema::try_merge` rejects
            // Utf8View-vs-Utf8 and BinaryView-vs-Binary.
            DataType::Utf8View => field.as_ref().clone().with_data_type(DataType::Utf8),
            DataType::BinaryView => field.as_ref().clone().with_data_type(DataType::Binary),
            // Spark timestamps are microseconds, so second and millisecond timestamps are
            // widened here; the conversion would otherwise reject them even though the
            // widening is lossless.
            DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, tz) => field
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            // Nanoseconds are lossy to widen, so what matches Spark depends on the format; see
            // [`NanosecondTimestamps`]. For Parquet they are left alone and therefore reported
            // rather than silently truncated; for a text format Spark itself truncates, so
            // rejecting the column would be the larger deviation.
            DataType::Timestamp(TimeUnit::Nanosecond, tz)
                if nanoseconds == NanosecondTimestamps::WidenToMicrosecond =>
            {
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
            }
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
    decode_names: bool,
) -> Result<()> {
    if table_partition_fields.is_empty() {
        return Ok(());
    }
    let inferred = infer_partitions(files, decode_names)?;
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

pub fn infer_partitions(
    files: &[ListingFileSample<'_>],
    decode_names: bool,
) -> Result<Vec<String>> {
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
                .map(|s| {
                    let name = s.split('=').next().unwrap_or("");
                    if decode_names {
                        percent_encoding::percent_decode_str(name)
                            .decode_utf8_lossy()
                            .into_owned()
                    } else {
                        name.to_string()
                    }
                })
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

/// Choose only an unambiguous leading prefix. NULLs, escaped spellings, and
/// floating-point directory aliases must be decoded before predicate evaluation.
fn hive_partition_prefix(columns: &[(String, DataType)], filters: &[Expr]) -> Option<Path> {
    if filters.iter().any(|filter| {
        filter
            .exists(|expr| Ok(matches!(expr, Expr::Literal(value, _) if value.is_null())))
            .unwrap_or(true)
    }) {
        return None;
    }
    let columns = columns
        .iter()
        .take_while(|(name, dt)| {
            name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                && !matches!(
                    dt,
                    DataType::Float16
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Timestamp(_, _)
                )
        })
        .cloned()
        .collect::<Vec<_>>();
    let prefix = evaluate_partition_prefix(&columns, filters)?;
    let parts = prefix
        .parts()
        .take_while(|part| {
            let Some((_, value)) = part.as_ref().split_once('=') else {
                return false;
            };
            !value.is_empty()
                && value != "__HIVE_DEFAULT_PARTITION__"
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        })
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| Path::from_iter(parts))
}

/// Decode Hive nulls before type conversion and predicate evaluation, retaining
/// prefix pruning for ordinary unambiguous partition equalities.
pub async fn hive_partitioned_files<'a>(
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    columns: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    let names = columns
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let schema = DFSchema::from_unqualified_fields(
        columns
            .iter()
            .map(|(name, dt)| Field::new(name, dt.clone(), true))
            .collect(),
        Default::default(),
    )?;
    let prefix = hive_partition_prefix(columns, filters);
    let filters = datafusion_expr::utils::conjunction(filters.iter().cloned())
        .map(|filter| vec![Expr::IsTrue(Box::new(filter))])
        .unwrap_or_default();
    let files = table_path
        .list_prefixed_files(ctx, store, prefix, "")
        .await?
        .try_filter(|meta| futures::future::ready(meta.size > 0))
        .map_ok(|meta| {
            let mut file = PartitionedFile::from(meta);
            file.table_reference.clone_from(table_path.get_table_ref());
            file
        });
    Ok(files
        .try_filter_map(move |mut file| {
            let result = (|| {
                let Some(values) =
                    partition_key(&file.object_meta.location, table_path.prefix(), &names)
                else {
                    return Ok(None);
                };
                file.partition_values = values
                    .into_iter()
                    .zip(columns)
                    .map(|(value, (_, dt))| {
                        if value == "__HIVE_DEFAULT_PARTITION__" {
                            ScalarValue::try_from(dt)
                        } else {
                            ScalarValue::try_from_string(value, dt)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                filter_partitioned_file(file, &filters, &schema)
            })();
            futures::future::ready(result)
        })
        .boxed())
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
    if partition_column_names.is_empty() {
        return false;
    }
    // Stable functions can prune partitions because their values do not change
    // within a query. DataFusion's listing helper only accepts immutable UDFs.
    let unsupported = expr.exists(|expr| {
        Ok(match expr {
            Expr::Column(column) => !partition_column_names.contains(&column.name.as_str()),
            Expr::ScalarFunction(function) => {
                function.func.signature().volatility == Volatility::Volatile
            }
            Expr::HigherOrderFunction(function) => {
                function.func.signature().volatility == Volatility::Volatile
            }
            Expr::Literal(_, _)
            | Expr::Alias(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::BinaryExpr(_)
            | Expr::Between(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::InList(_)
            | Expr::Case(_)
            | Expr::Lambda(_)
            | Expr::LambdaVariable(_) => false,
            _ => true,
        })
    });
    matches!(unsupported, Ok(false))
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[derive(Debug, Default)]
    struct CountingListingStore {
        inner: object_store::memory::InMemory,
        listed: Arc<std::sync::atomic::AtomicUsize>,
        prefixes: std::sync::Mutex<Vec<Option<Path>>>,
    }

    impl std::fmt::Display for CountingListingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingListingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingListingStore {
        async fn put_opts(
            &self,
            path: &Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(path, payload, options).await
        }
        async fn put_multipart_opts(
            &self,
            path: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(path, options).await
        }
        async fn get_opts(
            &self,
            path: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(path, options).await
        }
        fn delete_stream(
            &self,
            paths: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(paths)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.prefixes.lock().unwrap().push(prefix.cloned());
            let count = self.listed.clone();
            self.inner
                .list(prefix)
                .map_ok(move |meta| {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    meta
                })
                .boxed()
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn hive_listing_prunes_safe_equalities_and_lists_ambiguous_keys() -> Result<()> {
        use datafusion::logical_expr::{col, lit};
        use datafusion::prelude::SessionContext;
        use std::sync::atomic::Ordering;
        let store = CountingListingStore::default();
        for path in [
            "ints/p=1/file",
            "ints/p=2/file",
            "ints/p=__HIVE_DEFAULT_PARTITION__/file",
            "strings/p=a%3Db/file",
            "strings/p=other/file",
            "floats/p=1/file",
            "floats/p=1.0/file",
            "floats/p=2/file",
        ] {
            store
                .put(
                    &Path::parse(path)?,
                    bytes::Bytes::from_static(b"data").into(),
                )
                .await?;
        }
        for (table, dt, filter, rows, listed, prefix) in [
            (
                "ints",
                DataType::Int32,
                col("p").eq(lit(1_i32)),
                1,
                1,
                "ints/p=1",
            ),
            ("ints", DataType::Int32, col("p").is_null(), 1, 3, "ints"),
            (
                "strings",
                DataType::Utf8,
                col("p").eq(lit("a=b")),
                1,
                2,
                "strings",
            ),
            (
                "floats",
                DataType::Float64,
                col("p").eq(lit(1.0_f64)),
                2,
                3,
                "floats",
            ),
        ] {
            store.listed.store(0, Ordering::SeqCst);
            let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                .with_cache_manager(
                    datafusion::execution::cache::cache_manager::CacheManagerConfig::default()
                        .with_list_files_cache_limit(0),
                )
                .build_arc()?;
            let state = SessionContext::new_with_config_rt(Default::default(), runtime).state();
            let url = ListingTableUrl::parse(format!("memory://bucket/{table}/"))?;
            let filters = [filter];
            let columns = [("p".into(), dt)];
            let files = hive_partitioned_files(&state, &store, &url, &filters, &columns)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(files.len(), rows);
            assert_eq!(store.listed.load(Ordering::SeqCst), listed);
            assert_eq!(
                store.prefixes.lock().unwrap().last(),
                Some(&Some(Path::parse(prefix)?))
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn hive_partition_nulls_are_decoded_before_cast_and_pruning() -> Result<()> {
        use datafusion::logical_expr::{col, lit};
        use datafusion::prelude::SessionContext;
        use object_store::memory::InMemory;
        let store = InMemory::new();
        let url = ListingTableUrl::parse("memory://bucket/table/")?;
        for path in [
            "table/p=__HIVE_DEFAULT_PARTITION__/file.parquet",
            "table/p=2/file.parquet",
        ] {
            store
                .put(
                    &Path::parse(path)?,
                    bytes::Bytes::from_static(b"data").into(),
                )
                .await?;
        }
        let state = SessionContext::new().state();
        let columns = vec![("p".into(), DataType::Int32)];
        for (filter, expected) in [
            (col("p").is_null(), ScalarValue::Int32(None)),
            (col("p").eq(lit(2_i32)), ScalarValue::Int32(Some(2))),
        ] {
            let filters = [filter];
            let files = hive_partitioned_files(&state, &store, &url, &filters, &columns)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].partition_values, vec![expected]);
        }
        Ok(())
    }

    #[test]
    fn test_partition_pruning_function_volatility() {
        use datafusion::logical_expr::{col, create_udf, lit};

        for (volatility, expected) in [
            (Volatility::Immutable, true),
            (Volatility::Stable, true),
            (Volatility::Volatile, false),
        ] {
            let function = create_udf(
                "partition_value",
                vec![DataType::Utf8],
                DataType::Utf8,
                volatility,
                Arc::new(|args| {
                    args.first()
                        .cloned()
                        .ok_or_else(|| internal_datafusion_err!("missing input"))
                }),
            );
            let predicate = function.call(vec![col("p")]).eq(lit("2020-01-01"));
            assert_eq!(
                can_be_evaluated_for_partition_pruning(&["p"], &predicate),
                expected
            );
            assert!(!can_be_evaluated_for_partition_pruning(&["q"], &predicate));
            assert!(!can_be_evaluated_for_partition_pruning(&[], &predicate));
        }
    }

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
                Field::new("binview", DataType::BinaryView, true),
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

        // Spark has neither `Utf8View` nor `BinaryView`; both coerce to their plain forms.
        assert_eq!(field("view").data_type(), &DataType::Utf8);
        assert_eq!(field("binview").data_type(), &DataType::Binary);

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
        let merged = try_merge_normalized(
            [ts(TimeUnit::Millisecond), ts(TimeUnit::Microsecond)],
            NanosecondTimestamps::Report,
        )
        .unwrap();
        assert_eq!(
            merged.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        // Without normalizing first, that same merge fails - which is what this guards against.
        assert!(Schema::try_merge([ts(TimeUnit::Millisecond), ts(TimeUnit::Microsecond)]).is_err());

        // `Utf8View` and `Utf8` reconcile the same way.
        let merged = try_merge_normalized(
            [
                Schema::new(vec![Field::new("s", DataType::Utf8View, true)]),
                Schema::new(vec![Field::new("s", DataType::Utf8, true)]),
            ],
            NanosecondTimestamps::Report,
        )
        .unwrap();
        assert_eq!(merged.field(0).data_type(), &DataType::Utf8);

        // Nanoseconds are deliberately left alone, so they still conflict.
        assert!(
            try_merge_normalized(
                [ts(TimeUnit::Nanosecond), ts(TimeUnit::Microsecond)],
                NanosecondTimestamps::Report
            )
            .is_err()
        );
    }

    #[test]
    fn test_try_merge_normalized_nanosecond_policy() {
        let ts = |unit| {
            Schema::new(vec![
                Field::new("ts", DataType::Timestamp(unit, None), true),
                Field::new("ts_tz", DataType::Timestamp(unit, Some("UTC".into())), true),
            ])
        };
        let us = DataType::Timestamp(TimeUnit::Microsecond, None);
        let us_tz = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

        // Under `Report`, nanoseconds pass through: the Arrow-to-Spark conversion rejects the
        // column rather than truncating it, which is what Spark's Parquet reader does.
        let merged =
            try_merge_normalized([ts(TimeUnit::Nanosecond)], NanosecondTimestamps::Report).unwrap();
        assert_eq!(
            merged.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        // Under `WidenToMicrosecond`, nanoseconds widen like seconds and milliseconds, keeping
        // the time zone, so a text format infers a Spark timestamp and truncates as Spark does.
        let merged = try_merge_normalized(
            [ts(TimeUnit::Nanosecond)],
            NanosecondTimestamps::WidenToMicrosecond,
        )
        .unwrap();
        assert_eq!(merged.field(0).data_type(), &us);
        assert_eq!(merged.field(1).data_type(), &us_tz);

        // And a directory mixing nanosecond and microsecond files then merges instead of failing.
        let merged = try_merge_normalized(
            [ts(TimeUnit::Nanosecond), ts(TimeUnit::Microsecond)],
            NanosecondTimestamps::WidenToMicrosecond,
        )
        .unwrap();
        assert_eq!(merged.field(0).data_type(), &us);
    }
}
