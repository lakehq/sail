// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::logical_expr::{Expr, Volatility};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DataFusionError, GetExt, Result, plan_err};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_session::Session;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use sail_cache::scope::listing_table_ref;
use sail_common_datafusion::session::config::SessionConfigService;

use crate::listing::partition::{BasePathOptions, PartitionDiscovery};
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
/// View types (`Utf8View`, `BinaryView`) are not touched here. Whether they appear is decided
/// per format when per-file schemas are merged (see [`try_merge_normalized`]). The
/// Arrow-to-Spark conversion maps them to Spark's string and binary types, and
/// `optimizer.expand_views_at_output` converts the data at the query output.
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
///
/// The same applies to view types. A directory can hold a column as `Utf8View` in one file
/// and `Utf8` in another (e.g. a raw write followed by an `INSERT`), and `Schema::try_merge`
/// rejects that too. Such a column is upcast to its view counterpart: the conversion is
/// lossless and keeps the type the file declares. Columns that are plain in every file stay
/// plain. This is independent of Parquet's `schema_force_view_types`, which only controls
/// whether plain columns are *forced* to view types after the merge; with it disabled, a file
/// that declares a view type is still read as one, so the conflict has to be resolved either way.
pub fn try_merge_normalized(
    schemas: impl IntoIterator<Item = Schema>,
    nanoseconds: NanosecondTimestamps,
) -> Result<Schema> {
    let schemas = schemas
        .into_iter()
        .map(|schema| normalize_unsupported_fields(&schema, nanoseconds))
        .collect::<Vec<_>>();
    let view_fields = schemas
        .iter()
        .flat_map(|schema| schema.fields().iter())
        .filter(|field| matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView))
        .map(|field| field.name().clone())
        .collect::<HashSet<_>>();
    Ok(Schema::try_merge(schemas.iter().map(|schema| {
        upcast_to_view_fields(schema, &view_fields)
    }))?)
}

/// Upcasts plain string and binary fields named in `view_fields` to their view counterparts.
fn upcast_to_view_fields(schema: &Schema, view_fields: &HashSet<String>) -> Schema {
    map_fields(schema, |field| {
        let view_type = match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
            DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
            _ => return None,
        };
        view_fields.contains(field.name()).then_some(view_type)
    })
}

/// Rebuilds `schema`, replacing the data type of each top-level field for which `f` returns
/// one. Field nullability and metadata and the schema metadata are preserved.
fn map_fields(schema: &Schema, f: impl Fn(&Field) -> Option<DataType>) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match f(field) {
            Some(data_type) => field.as_ref().clone().with_data_type(data_type),
            None => field.as_ref().clone(),
        })
        .collect();

    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

fn normalize_unsupported_fields(schema: &Schema, nanoseconds: NanosecondTimestamps) -> Schema {
    // TODO: Apply Spark-compatible type normalization recursively inside structs, lists, and
    // maps. Only top-level fields are normalized today, so nested millisecond timestamps remain
    // unsupported even though Spark accepts them (SPARK recurses through nested timestamp leaves).
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaSampling {
    Sample,
    /// Multiline CSV infers types from whole files, including loose files in a partitioned table.
    MultilineCsv,
    /// Ordinary Parquet reads use the existing bounded sample when mergeSchema is unset.
    SampleParquet,
    /// Prefer a summary footer, falling back to the lexicographically first data file.
    SingleParquet,
    MergeParquet {
        summaries_only: bool,
    },
}

/// Collect the files needed for schema and compression inference. Bounded samples are per
/// original input, and partition discovery examines every leaf directory below basePath.
pub(super) async fn sample_listing_files<'a>(
    ctx: &dyn Session,
    inputs: &'a [Vec<ListingTableUrl>],
    path_glob_filter: Option<&PathGlobFilter>,
    partition_directories_only: bool,
    base_path: Option<&BasePathOptions>,
    recursive: bool,
    registered: bool,
    sampling: SchemaSampling,
) -> Result<(Vec<ListingFileSample<'a>>, PartitionDiscovery)> {
    const SAMPLE_SIZE: usize = 10;
    let merge = matches!(sampling, SchemaSampling::MergeParquet { .. });
    let all_files = merge || sampling == SchemaSampling::MultilineCsv;
    let directory_order =
        sampling == SchemaSampling::MultilineCsv && uses_directory_listing(ctx, inputs)?;
    let mut discovery = PartitionDiscovery::default();
    let mut samples = Vec::new();
    let mut direct_samples = Vec::new();
    let mut footer = None;
    let mut direct_footer = None;

    for roots in inputs {
        let Some(first) = roots.first() else { continue };
        // Glob alternatives from one input use the same object store.
        let store = ctx.runtime_env().object_store(first)?;
        let mut objects = Vec::new();
        let mut direct = Vec::new();
        let mut has_listed_file = false;
        let mut has_direct_file = false;
        'roots: for url in roots {
            let start = objects.len();
            let mut files = if registered {
                list_partition_files(url, store.as_ref(), None).await?
            } else {
                list_all_files(url, ctx, store.as_ref(), None, partition_directories_only).await?
            };
            while let Some(file) = files.try_next().await? {
                let selected = matches_path_glob_filter(path_glob_filter, &file.location);
                let name = file.location.filename().unwrap_or_default();
                let metadata_file = is_metadata_file(name);
                let partitioned = if metadata_file {
                    true
                } else if let Some(base) = base_path {
                    // Even empty and filtered files participate in partition discovery.
                    discovery.observe(url, &file.location, Some(base))?
                } else {
                    if !recursive && !registered && selected && file.size > 0 {
                        discovery.observe(url, &file.location, None)?;
                    }
                    true
                };
                if !selected {
                    continue;
                }
                let direct_input = base_path.is_some() && is_direct_input(url, &file.location);
                has_listed_file |= partitioned;
                has_direct_file |= direct_input;
                // Spark excludes metadata prefixes from row scans and partition discovery.
                // Only the two exact filenames receive special treatment when choosing footers.
                let parquet_summary = matches!(name, "_common_metadata" | "_metadata");
                if (metadata_file
                    && matches!(
                        sampling,
                        SchemaSampling::Sample | SchemaSampling::MultilineCsv
                    ))
                    || (!parquet_summary && file.size == 0)
                {
                    continue;
                }
                if sampling == SchemaSampling::SingleParquet
                    || (sampling == SchemaSampling::SampleParquet && parquet_summary)
                {
                    if direct_input {
                        keep_parquet_footer(&mut direct_footer, url, &store, &file);
                    }
                    keep_parquet_footer(&mut footer, url, &store, &file);
                    continue;
                }
                if !parquet_summary
                    && matches!(
                        sampling,
                        SchemaSampling::MergeParquet {
                            summaries_only: true
                        }
                    )
                {
                    continue;
                }
                if direct_input && (all_files || direct.len() < SAMPLE_SIZE) {
                    direct.push(file.clone());
                }
                if (partitioned || all_files) && (all_files || objects.len() < SAMPLE_SIZE) {
                    objects.push(file);
                }
                if base_path.is_none() && !all_files && objects.len() == SAMPLE_SIZE {
                    break 'roots;
                }
            }
            if directory_order && url.is_collection() {
                order_csv_directory_files(&mut objects, start, url.prefix());
            }
        }
        samples.push(ListingFileSample {
            url: first,
            store,
            objects,
            has_listed_file,
        });
        direct_samples.push((direct, has_direct_file));
    }
    if base_path.is_some() && !discovery.has_partitions() {
        for (sample, (direct, listed)) in samples.iter_mut().zip(direct_samples) {
            sample.objects = direct;
            sample.has_listed_file = listed;
        }
        footer = direct_footer;
    }
    if let Some((url, store, object)) = footer {
        samples = vec![ListingFileSample {
            url,
            store,
            objects: vec![object],
            has_listed_file: true,
        }];
    }
    Ok((samples, discovery))
}

/// Spark can use a flat recursive listing for a single root on configured filesystems.
fn uses_directory_listing(ctx: &dyn Session, inputs: &[Vec<ListingTableUrl>]) -> Result<bool> {
    let [roots] = inputs else { return Ok(true) };
    let [url] = roots.as_slice() else {
        return Ok(true);
    };
    let schemes = ctx
        .config()
        .get_extension::<SessionConfigService>()
        .map(|service| service.get_option("spark.sql.sources.useListFilesFileSystemList"))
        .transpose()?
        .flatten();
    Ok(!schemes
        .as_deref()
        .unwrap_or("s3a")
        .split(',')
        .any(|scheme| scheme.trim().eq_ignore_ascii_case(url.scheme())))
}

/// Hadoop emits the immediate files in each directory before walking its child directories.
/// Preserve the listing order of sibling directories and of files in the same directory.
fn order_csv_directory_files(files: &mut Vec<ObjectMeta>, start: usize, root: &Path) {
    if files.len() - start < 2 {
        return;
    }
    fn relative<'a>(file: &'a ObjectMeta, root: &Path) -> &'a str {
        file.location
            .as_ref()
            .strip_prefix(root.as_ref())
            .unwrap_or(file.location.as_ref())
            .trim_start_matches('/')
    }
    if files[start..]
        .iter()
        .all(|file| !relative(file, root).contains('/'))
    {
        return;
    }
    let mut directories = HashMap::<String, usize>::new();
    let mut entries = Vec::new();
    for file in files.drain(start..) {
        let path = relative(&file, root);
        let mut order = Vec::new();
        for (end, _) in path.match_indices('/') {
            let directory = &path[..end];
            let id = match directories.get(directory) {
                Some(&id) => id,
                None => {
                    let id = directories.len();
                    directories.insert(directory.to_owned(), id);
                    id
                }
            };
            order.push(id);
        }
        entries.push((order, file));
    }
    // Ancestor keys are strict prefixes of their descendants.
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    files.extend(entries.into_iter().map(|(_, file)| file));
}

/// Spark orders Parquet schemas by Hadoop Path.toString, a qualified, decoded UTF-16 string.
pub(crate) fn parquet_path_sort_key<'a>(
    url: &'a ListingTableUrl,
    path: &'a Path,
) -> impl Iterator<Item = u16> + 'a {
    url.get_url()[..url::Position::BeforePath]
        .encode_utf16()
        .chain([u16::from(b'/')])
        .chain(path.as_ref().encode_utf16())
}

// Spark prefers _common_metadata over _metadata, then the lexicographically first path.
fn keep_parquet_footer<'a>(
    current: &mut Option<(&'a ListingTableUrl, Arc<dyn ObjectStore>, ObjectMeta)>,
    url: &'a ListingTableUrl,
    store: &Arc<dyn ObjectStore>,
    file: &ObjectMeta,
) {
    let key = |file: &ObjectMeta| match file.location.filename() {
        Some("_common_metadata") => 0,
        Some("_metadata") => 1,
        _ => 2,
    };
    if current.as_ref().is_none_or(|(previous_url, _, previous)| {
        key(file)
            .cmp(&key(previous))
            .then_with(|| {
                parquet_path_sort_key(url, &file.location)
                    .cmp(parquet_path_sort_key(previous_url, &previous.location))
            })
            .is_lt()
    }) {
        *current = Some((url, Arc::clone(store), file.clone()));
    }
}

pub(super) fn is_direct_input(url: &ListingTableUrl, path: &Path) -> bool {
    url.strip_prefix(path)
        .is_some_and(|mut parts| parts.nth(1).is_none())
}

/// List only direct children. Nested prefixes may belong to other registered partitions.
pub(super) async fn list_partition_files<'a>(
    url: &'a ListingTableUrl,
    store: &dyn ObjectStore,
    filter: Option<&'a PathGlobFilter>,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    let listing = store.list_with_delimiter(Some(url.prefix())).await?;
    Ok(futures::stream::iter(
        listing
            .objects
            .into_iter()
            .filter(move |object| {
                // Object stores can return a directory marker for the prefix itself.
                &object.location != url.prefix()
                    && !has_hidden_path_component(url, &object.location)
                    && matches_path_glob_filter(filter, &object.location)
            })
            .map(Ok),
    )
    .boxed())
}

pub async fn list_all_files<'a>(
    url: &'a ListingTableUrl,
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    path_glob_filter: Option<&'a PathGlobFilter>,
    partition_directories_only: bool,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    let exec_options = &ctx.config_options().execution;
    let ignore_subdirectory = exec_options.listing_table_ignore_subdirectory;
    // If the prefix is a file, use a head request, otherwise use a list request.
    let list = match url.is_collection() {
        true => match ctx.runtime_env().cache_manager.get_list_files_cache() {
            None => store.list(Some(url.prefix())),
            Some(cache) => {
                // This key must match the one DataFusion uses when listing for a scan.
                let key = TableScopedPath {
                    table: Some(listing_table_ref(url, ctx.session_id())),
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
                && (!partition_directories_only || in_partition_directories(url, path))
                && !has_hidden_path_component(url, path)
                && matches_path_glob_filter(path_glob_filter, path);
            futures::future::ready(included)
        })
        .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
        .boxed())
}

/// A migrated file table owns root files and Hive partition directories only.
pub(super) fn in_partition_directories(root: &ListingTableUrl, location: &Path) -> bool {
    root.strip_prefix(location).is_some_and(|segments| {
        let mut segments = segments.peekable();
        while let Some(part) = segments.next() {
            if segments.peek().is_some() && !part.contains('=') {
                return false;
            }
        }
        true
    })
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

fn has_metadata_prefix(name: &str) -> bool {
    name.starts_with("_common_metadata") || name.starts_with("_metadata")
}

pub fn is_metadata_file(name: &str) -> bool {
    !name.contains('=') && has_metadata_prefix(name)
}

pub fn is_hidden_path_name(name: &str) -> bool {
    let exclude = (name.starts_with('_') && !name.contains('='))
        || name.starts_with('.')
        || name.ends_with("._COPYING_");
    exclude && !has_metadata_prefix(name)
}

/// Returns `true` if the path is hidden per Spark's `HadoopFSUtils.shouldFilterOutPathName`.
pub fn has_hidden_path_component(url: &ListingTableUrl, location: &Path) -> bool {
    url.strip_prefix(location)
        .is_some_and(|mut segments| segments.any(is_hidden_path_name))
        || location.filename().is_some_and(is_hidden_path_name)
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

    use datafusion::datasource::listing::helpers::pruned_partition_list;
    use datafusion::prelude::SessionContext;
    use datafusion_common::internal_datafusion_err;
    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use sail_cache::scope::scoped_listing_url;

    use super::*;

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

        // View types are kept rather than collapsed to their plain forms.
        assert_eq!(field("view").data_type(), &DataType::Utf8View);
        assert_eq!(field("binview").data_type(), &DataType::BinaryView);

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
    fn test_try_merge_normalized_upcasts_mixed_view_types() {
        let schema = |fields: Vec<(&str, DataType)>| {
            Schema::new(
                fields
                    .into_iter()
                    .map(|(name, data_type)| Field::new(name, data_type, true))
                    .collect::<Vec<_>>(),
            )
        };
        // `s` and `b` are a view in one file and plain (including `Large*`) in others;
        // `plain` is plain in every file.
        let schemas = || {
            [
                schema(vec![
                    ("s", DataType::Utf8View),
                    ("b", DataType::Binary),
                    ("plain", DataType::Utf8),
                ]),
                schema(vec![
                    ("s", DataType::Utf8),
                    ("b", DataType::BinaryView),
                    ("plain", DataType::Utf8),
                ]),
                schema(vec![
                    ("s", DataType::LargeUtf8),
                    ("b", DataType::LargeBinary),
                ]),
            ]
        };
        let types = |merged: Schema| {
            ["s", "b", "plain"]
                .map(|name| merged.field_with_name(name).unwrap().data_type().clone())
        };

        // The mixed columns merge as views; the all-plain column stays plain.
        assert_eq!(
            types(try_merge_normalized(schemas(), NanosecondTimestamps::Report).unwrap()),
            [DataType::Utf8View, DataType::BinaryView, DataType::Utf8]
        );

        // Without reconciling first, the mixed merge fails - which is what this guards against.
        assert!(
            Schema::try_merge([
                schema(vec![("s", DataType::Utf8View)]),
                schema(vec![("s", DataType::Utf8)]),
            ])
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

    #[tokio::test]
    async fn file_listing_cache_keeps_object_stores_apart() -> Result<()> {
        let ctx = SessionContext::new();
        let cache = ctx
            .runtime_env()
            .cache_manager
            .get_list_files_cache()
            .ok_or_else(|| internal_datafusion_err!("missing file listing cache"))?;
        let state = ctx.state();
        let mut listed = vec![];
        for (bucket, file) in [("bucket-a", "a.parquet"), ("bucket-b", "b.parquet")] {
            let store = InMemory::new();
            store
                .put(
                    &Path::from(format!("data/{file}")),
                    PutPayload::from_static(b"x"),
                )
                .await?;
            let url = ListingTableUrl::parse(format!("s3://{bucket}/data/"))?;
            let files = list_all_files(&url, &state, &store, None, false)
                .await?
                .map_ok(|meta| meta.location.to_string())
                .try_collect::<Vec<_>>()
                .await?;
            listed.push(files);
            // The scan lists through DataFusion, which must reuse the same cache entry.
            let scanned = pruned_partition_list(
                &state,
                &store,
                &scoped_listing_url(&url, state.session_id()),
                &[],
                "",
                &[],
            )
            .await?
            .map_ok(|file| file.object_meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await?;
            listed.push(scanned);
        }
        assert_eq!(
            listed,
            vec![
                vec!["data/a.parquet".to_string()],
                vec!["data/a.parquet".to_string()],
                vec!["data/b.parquet".to_string()],
                vec!["data/b.parquet".to_string()]
            ]
        );
        assert_eq!(cache.len(), 2);
        Ok(())
    }
}
