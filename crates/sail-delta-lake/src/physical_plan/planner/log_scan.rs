// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::common::config::TableParquetOptions;
use datafusion::common::encryption::FileDecryptionProperties;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{
    CachedParquetFileReaderFactory, transform_binary_to_string, transform_schema_to_view,
};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::extensions::Extensions;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use sail_cache::scope::scoped_file_metadata_cache;
use sail_data_source::formats::parquet::parse_coerce_int96_string;

use super::context::PlannerContext;
use crate::datasource::create_object_store_url;
use crate::physical_plan::COL_LOG_VERSION;
use crate::spec::{
    CheckpointActionRow, delta_log_file_path, is_json_checkpoint_filename, parse_version_prefix,
};

/// The canonical Delta log file schema with proper Map types for fields like `partitionValues`.
///
/// JSON schema inference gives `partitionValues` as a Struct, which breaks `map_extract`.
/// By using this fixed schema for JSON-only log reads (when no parquet checkpoint exists),
/// we ensure consistent Map types regardless of whether a checkpoint is present.
static DELTA_LOG_FILE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    #[expect(clippy::expect_used)]
    let action_type = DataType::try_from(&crate::spec::DataType::from(
        CheckpointActionRow::struct_type(),
    ))
    .expect("canonical checkpoint action schema should convert to Arrow");
    let DataType::Struct(fields) = action_type else {
        unreachable!("checkpoint action schema should be a struct")
    };
    Arc::new(Schema::new(fields))
});

fn recursively_nullable_field(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(recursively_nullable_field(field)))
                .collect::<Fields>(),
        ),
        DataType::List(element) => DataType::List(Arc::new(recursively_nullable_field(element))),
        DataType::LargeList(element) => {
            DataType::LargeList(Arc::new(recursively_nullable_field(element)))
        }
        DataType::FixedSizeList(element, size) => {
            DataType::FixedSizeList(Arc::new(recursively_nullable_field(element)), *size)
        }
        DataType::Map(entries, sorted) => {
            let entries_type = match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    let key = Arc::clone(&fields[0]);
                    let value = Arc::new(recursively_nullable_field(&fields[1]));
                    DataType::Struct(Fields::from(vec![key, value]))
                }
                data_type => data_type.clone(),
            };
            DataType::Map(
                Arc::new(
                    Field::new(entries.name(), entries_type, false)
                        .with_metadata(entries.metadata().clone()),
                ),
                *sorted,
            )
        }
        data_type => data_type.clone(),
    };
    Field::new(field.name(), data_type, true).with_metadata(field.metadata().clone())
}

fn replay_scan_schema(checkpoint_schema: &Schema) -> Result<SchemaRef> {
    // Checkpoint schemas may omit null action fields and contain checkpoint-only parsed fields.
    // JSON tails must still be decoded with every canonical action field available.
    let checkpoint_schema = Schema::new_with_metadata(
        checkpoint_schema
            .fields()
            .iter()
            .map(|field| Arc::new(recursively_nullable_field(field)) as FieldRef)
            .collect::<Vec<_>>(),
        checkpoint_schema.metadata().clone(),
    );
    let replay_schema =
        Schema::try_merge([checkpoint_schema, DELTA_LOG_FILE_SCHEMA.as_ref().clone()])?;
    Ok(Arc::new(replay_schema))
}

#[derive(Debug, Clone, Default)]
pub struct LogScanOptions {
    /// Optional projection of top-level log columns (e.g. ["add", "remove", "metaData"]).
    ///
    /// When set, the scan will only read these columns plus any required partition columns.
    pub projection: Option<Vec<String>>,
    /// Optional pushdown predicate for checkpoint parquet scans.
    pub parquet_predicate: Option<Arc<dyn PhysicalExpr>>,
}

fn parse_log_version_prefix(filename: &str) -> Option<u64> {
    parse_version_prefix(filename)?.try_into().ok()
}

fn log_file_path(table_root_path: &Path, filename: &str) -> Path {
    // Object store paths are absolute for local filesystem stores in our setup (DataFusion uses
    // `ObjectStoreUrl::local_filesystem()`).
    delta_log_file_path(table_root_path, filename)
}

async fn head_many(
    store: &Arc<dyn ObjectStore>,
    table_root_path: &Path,
    files: &[String],
) -> Result<Vec<ObjectMeta>> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    // Concurrency is intentionally bounded to avoid overwhelming object stores.
    let concurrency = std::cmp::min(64usize, files.len());
    stream::iter(files.iter().cloned())
        .map(|f| {
            let store = Arc::clone(store);
            let p = log_file_path(table_root_path, &f);
            async move {
                store.head(&p).await.map_err(|e| {
                    DataFusionError::External(Box::<dyn std::error::Error + Send + Sync>::from(e))
                })
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await
}

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/bdd86f1da39949d09e915ca46c95f32a253689db/datafusion/datasource-parquet/src/file_format.rs#L247-L368
/// Clears all metadata (Schema level and field level) on an iterator
/// of Schemas, except for the extension type of the fields, which is part
/// of the type of a field.
fn clear_metadata(schemas: impl IntoIterator<Item = Schema>) -> impl Iterator<Item = Schema> {
    schemas.into_iter().map(|schema| {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                let metadata = field
                    .metadata()
                    .iter()
                    .filter(|(key, _)| {
                        key.as_str() == EXTENSION_TYPE_NAME_KEY
                            || key.as_str() == EXTENSION_TYPE_METADATA_KEY
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<HashMap<String, String>>();
                field.as_ref().clone().with_metadata(metadata)
            })
            .collect::<Fields>();
        Schema::new(fields)
    })
}

/// Widen fields missing from some of their containing schemas. An absent parent
/// produces a null parent, not a null value for each required child, so children
/// are compared only across files in which their parent exists.
fn mark_partially_present_fields_nullable(schema: Schema, sources: &[Schema]) -> Schema {
    if sources.len() <= 1
        || sources[1..]
            .iter()
            .all(|source| source.fields() == sources[0].fields())
    {
        return schema;
    }
    let fields = widen_fields(
        schema.fields(),
        &sources.iter().map(Schema::fields).collect::<Vec<_>>(),
    );
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn widen_fields(fields: &Fields, sources: &[&Fields]) -> Fields {
    if !sources.is_empty()
        && fields == sources[0]
        && sources[1..].iter().all(|source| *source == sources[0])
    {
        return fields.clone();
    }

    let mut present: HashMap<&str, Vec<&FieldRef>> = HashMap::new();
    for source in sources {
        for field in *source {
            present.entry(field.name()).or_default().push(field);
        }
    }
    fields
        .iter()
        .map(|field| {
            let occurrences = present
                .get(field.name().as_str())
                .map(Vec::as_slice)
                .unwrap_or_default();
            let missing = occurrences.is_empty() || occurrences.len() < sources.len();
            widen_field(field, occurrences, missing)
        })
        .collect()
}

fn widen_field(field: &FieldRef, sources: &[&FieldRef], missing: bool) -> FieldRef {
    let source_types = sources.iter().map(|f| f.data_type()).collect::<Vec<_>>();
    let data_type = widen_nested_type(field.data_type(), &source_types);
    let nullable = field.is_nullable() || missing;
    if &data_type == field.data_type() && nullable == field.is_nullable() {
        Arc::clone(field)
    } else {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_nullable(nullable),
        )
    }
}

fn widen_nested_type(data_type: &DataType, sources: &[&DataType]) -> DataType {
    match data_type {
        DataType::Struct(fields) => {
            let sources = sources
                .iter()
                .filter_map(|t| match t {
                    DataType::Struct(fields) => Some(fields),
                    _ => None,
                })
                .collect::<Vec<_>>();
            DataType::Struct(widen_fields(fields, &sources))
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let sources = sources
                .iter()
                .filter_map(|t| match t {
                    DataType::List(f) | DataType::LargeList(f) => Some(f),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let field = widen_field(field, &sources, false);
            if matches!(data_type, DataType::List(_)) {
                DataType::List(field)
            } else {
                DataType::LargeList(field)
            }
        }
        // Parquet cannot represent unions. Arrow requires exact type equality
        // for ListView, LargeListView, FixedSizeList, Map, Dictionary, and
        // RunEndEncoded, so nested evolution in those types is rejected earlier.
        _ => data_type.clone(),
    }
}
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/bdd86f1da39949d09e915ca46c95f32a253689db/datafusion/datasource-parquet/src/file_format.rs#L370-L402
// The `parquet_encryption` variant is left out since Sail does not enable that DataFusion feature.
#[expect(clippy::unused_async)]
async fn get_file_decryption_properties(
    _state: &dyn datafusion::catalog::Session,
    _options: &TableParquetOptions,
    _file_path: &Path,
) -> Result<Option<Arc<FileDecryptionProperties>>> {
    Ok(None)
}
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/bdd86f1da39949d09e915ca46c95f32a253689db/datafusion/datasource-parquet/src/file_format.rs#L451-L549
/// Infers the merged schema of Parquet checkpoint files, reading the footers through the given
/// file metadata cache.
///
/// This mirrors `ParquetFormat::infer_schema()` at the DataFusion commit credited above, with the
/// default format options, except that it reads the footers through `file_metadata_cache` instead
/// of the runtime's unscoped file metadata cache.
async fn infer_parquet_checkpoint_schema(
    state: &dyn datafusion::catalog::Session,
    store: &Arc<dyn ObjectStore>,
    file_metadata_cache: &Arc<FileMetadataCache>,
    objects: &[ObjectMeta],
) -> Result<SchemaRef> {
    let format = ParquetFormat::default();
    // TODO: uncomment after DataFusion 56
    // let coerce_int96 = format.coerce_int96().map(arrow::datatypes::TimeUnit::from);
    let coerce_int96 = format
        .coerce_int96()
        .map(|time_unit| parse_coerce_int96_string(&time_unit))
        .transpose()?;
    let coerce_int96_tz = format
        .options()
        .global
        .coerce_int96_tz
        .as_ref()
        .map(|tz| Arc::<str>::from(tz.as_str()));

    let mut schemas: Vec<_> = futures::stream::iter(objects)
        .map(|object| async {
            let file_decryption_properties =
                get_file_decryption_properties(state, format.options(), &object.location).await?;
            let meta = DFParquetMetadata::new(store.as_ref(), object)
                .with_metadata_size_hint(format.metadata_size_hint())
                .with_decryption_properties(file_decryption_properties)
                .with_file_metadata_cache(Some(Arc::clone(file_metadata_cache)))
                .with_coerce_int96(coerce_int96)
                .with_coerce_int96_tz(coerce_int96_tz.clone())
                // TODO: uncomment after DataFusion 56
                // .with_enable_rle_to_dictionary(
                //     format.options().global.enable_rle_to_dictionary,
                // )
                ;
            let result = (object.location.clone(), meta.fetch_schema().await?);
            Ok::<_, DataFusionError>(result)
        })
        .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
        // fetch schemas concurrently, if requested
        .buffer_unordered(
            state
                .config_options()
                .execution
                .meta_fetch_concurrency
                .get(),
        )
        .try_collect()
        .await?;

    // Schema inference adds fields based the order they are seen
    // which depends on the order the files are processed. For some
    // object stores (like local file systems) the order returned from list
    // is not deterministic. Thus, to ensure deterministic schema inference
    // sort the files first.
    // https://github.com/apache/datafusion/pull/6629
    schemas.sort_unstable_by(|(location1, _), (location2, _)| location1.cmp(location2));

    // TODO: uncomment after DataFusion 56
    // let mut seen = HashSet::new();
    // for (location, schema) in &schemas {
    //     ensure_unique_field_names(schema, &mut seen).map_err(|err| {
    //         DataFusionError::Context(
    //             format!("Error when processing Parquet file {location}"),
    //             Box::new(err),
    //         )
    //     })?;
    // }
    // drop(seen);

    // Normalize dict-promoted schemas before merging so mixed dict/plain files merge cleanly.
    let schemas: Vec<Schema> = schemas.into_iter().map(|(_, schema)| schema).collect();
    // TODO: uncomment after DataFusion 56
    // if format.options().global.enable_rle_to_dictionary {
    //     schemas = crate::schema_coercion::uniform_dict_schemas(schemas);
    // }

    let source_schemas = schemas.clone();
    let schema = if format.skip_metadata() {
        Schema::try_merge(clear_metadata(schemas))
    } else {
        Schema::try_merge(schemas)
    }?;

    let schema = mark_partially_present_fields_nullable(schema, &source_schemas);

    let schema = if format.binary_as_string() {
        transform_binary_to_string(&schema)
    } else {
        schema
    };

    let schema = if format.force_view_types() {
        transform_schema_to_view(&schema)
    } else {
        schema
    };

    Ok(Arc::new(schema))
}
// SPDX-SnippetEnd

fn to_partitioned_files(metas: Vec<ObjectMeta>) -> Result<Vec<PartitionedFile>> {
    metas
        .into_iter()
        .map(|m| {
            let loc = m.location.as_ref();
            let filename = m.location.filename().unwrap_or(loc);
            let ver = parse_log_version_prefix(filename).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to parse delta log version from file '{filename}'"
                ))
            })?;
            let ver = i64::try_from(ver).map_err(|_| {
                DataFusionError::Plan(format!(
                    "delta log version '{ver}' does not fit into Int64 for file '{filename}'"
                ))
            })?;
            Ok(PartitionedFile {
                object_meta: m,
                partition_values: vec![ScalarValue::Int64(Some(ver))],
                range: None,
                statistics: None,
                ordering: None,
                extensions: Extensions::default(),
                metadata_size_hint: None,
                table_reference: None,
                arrow_schema: None,
            })
        })
        .collect()
}

/// Create partitioned files from V2 sidecar file metadata, assigning a fixed checkpoint version
/// as the partition value since sidecar filenames do not carry a version prefix.
fn to_partitioned_files_with_version(
    metas: Vec<ObjectMeta>,
    version: i64,
) -> Result<Vec<PartitionedFile>> {
    Ok(metas
        .into_iter()
        .map(|m| PartitionedFile {
            object_meta: m,
            partition_values: vec![ScalarValue::Int64(Some(version))],
            range: None,
            statistics: None,
            ordering: None,
            extensions: Extensions::default(),
            metadata_size_hint: None,
            table_reference: None,
            arrow_schema: None,
        })
        .collect())
}

fn to_file_groups(metas: Vec<ObjectMeta>, target_partitions: usize) -> Result<Vec<FileGroup>> {
    Ok(group_partitioned_files(
        to_partitioned_files(metas)?,
        target_partitions,
    ))
}

fn group_partitioned_files(
    mut files: Vec<PartitionedFile>,
    target_partitions: usize,
) -> Vec<FileGroup> {
    if files.is_empty() {
        return Vec::new();
    }

    // Metadata lookups are concurrent, so sort by path for stable EXPLAIN output.
    files.sort_by(|a, b| {
        a.object_meta
            .location
            .as_ref()
            .cmp(b.object_meta.location.as_ref())
    });
    let num_groups = std::cmp::min(target_partitions, files.len());
    let num_groups = num_groups.max(1);
    let chunk_size = files.len().div_ceil(num_groups);

    let mut groups = Vec::with_capacity(num_groups);
    while !files.is_empty() {
        let rest = if files.len() > chunk_size {
            files.split_off(chunk_size)
        } else {
            Vec::new()
        };
        groups.push(FileGroup::from(std::mem::take(&mut files)));
        files = rest;
    }
    groups
}

fn build_json_log_scan(
    object_store_url: ObjectStoreUrl,
    table_schema: TableSchema,
    metas: Vec<ObjectMeta>,
    target_partitions: usize,
    projection_indices: Option<Vec<usize>>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if metas.is_empty() {
        return Ok(None);
    }
    let source: Arc<dyn datafusion::datasource::physical_plan::FileSource> = Arc::new(
        datafusion::datasource::physical_plan::JsonSource::new(table_schema),
    );
    let groups = to_file_groups(metas, target_partitions)?;
    let config = FileScanConfigBuilder::new(object_store_url, source)
        .with_file_groups(groups)
        .with_projection_indices(projection_indices)?
        .build();
    Ok(Some(DataSourceExec::from_data_source(config)))
}

pub async fn build_delta_log_datasource_scans_with_options(
    ctx: &PlannerContext<'_>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    sidecar_files: Vec<String>,
    options: LogScanOptions,
) -> Result<(
    Option<Arc<dyn ExecutionPlan>>,
    Option<Arc<dyn ExecutionPlan>>,
    Vec<String>,
    Vec<String>,
)> {
    let store = ctx.object_store()?;
    let log_store = ctx.log_store()?;
    let object_store_url = create_object_store_url(&log_store.config().location).map_err(|e| {
        DataFusionError::External(Box::<dyn std::error::Error + Send + Sync>::from(e))
    })?;

    // The checkpoint footers are cached under keys scoped to the object store, both for schema
    // inference and for the scan, so that the two share cache entries. Workers rebuild the scan
    // with the same scoped cache.
    let metadata_cache = scoped_file_metadata_cache(
        ctx.session()
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache(),
        &object_store_url,
    );

    // Commit/checkpoint lists are expected to be selected by the planner log-segment resolver.
    // This builder only materializes datasource scans from those resolved filenames.
    let table_root = Path::from_url_path(log_store.config().location.path())?;
    let table_root_path = &table_root;
    let (checkpoint_metas, commit_metas, sidecar_metas) = tokio::try_join!(
        head_many(&store, table_root_path, &checkpoint_files),
        head_many(&store, table_root_path, &commit_files),
        head_many(&store, table_root_path, &sidecar_files)
    )?;
    let (json_checkpoint_metas, parquet_checkpoint_metas): (Vec<_>, Vec<_>) =
        checkpoint_metas.into_iter().partition(|meta| {
            meta.location
                .filename()
                .is_some_and(is_json_checkpoint_filename)
        });

    // Resolve the checkpoint version for assigning to sidecar partition values.
    let checkpoint_version = checkpoint_files
        .first()
        .and_then(|f| parse_log_version_prefix(f))
        .map(|v| v as i64);

    // Infer schemas for parquet checkpoint files only. JSON commit files use the canonical
    // Delta log file schema (see `DELTA_LOG_FILE_SCHEMA`) to avoid type mismatches for
    // map-like fields (e.g. `add.partitionValues`).
    //
    // For V2 checkpoints with sidecars, infer the checkpoint schema from the Parquet sidecars
    // even when the top-level checkpoint manifest is JSON.
    let all_parquet_metas: Vec<ObjectMeta> = parquet_checkpoint_metas
        .iter()
        .chain(sidecar_metas.iter())
        .cloned()
        .collect();
    let parquet_schema = if all_parquet_metas.is_empty() {
        None
    } else {
        Some(
            infer_parquet_checkpoint_schema(
                ctx.session(),
                &store,
                &metadata_cache,
                &all_parquet_metas,
            )
            .await?,
        )
    };
    let has_json_files = !json_checkpoint_metas.is_empty() || !commit_metas.is_empty();

    let replay_schema = match (parquet_schema, has_json_files) {
        (Some(p), _) => replay_scan_schema(&p)?,
        (None, true) => {
            // No parquet checkpoint exists (e.g. before the first checkpoint interval fires).
            // Use the canonical Delta log file schema so that `partitionValues` and other
            // map-like fields have the correct Map Arrow type instead of an inferred Struct.
            Arc::clone(&*DELTA_LOG_FILE_SCHEMA)
        }
        (None, false) => {
            return Err(DataFusionError::Plan(
                "no _delta_log files found to build log scan".to_string(),
            ));
        }
    };

    let projection_indices = if let Some(projection) = &options.projection {
        let mut projection = projection.clone();
        if !projection.iter().any(|col| col == COL_LOG_VERSION) {
            projection.push(COL_LOG_VERSION.to_string());
        }
        let file_schema_len = replay_schema.fields().len();
        let mut indices = Vec::with_capacity(projection.len());
        for col in &projection {
            if col == COL_LOG_VERSION {
                indices.push(file_schema_len);
                continue;
            }

            match replay_schema.index_of(col) {
                Ok(idx) => indices.push(idx),
                Err(_) => {
                    // Some Delta writers/checkpoint formats may omit an action column
                    // (for example, no `remove` records in a newly created table).
                    // Skip missing projected columns and let downstream replay logic
                    // treat them as absent.
                }
            }
        }
        Some(indices)
    } else {
        None
    };

    let target_partitions = ctx.session().config().target_partitions();
    let table_schema = TableSchema::builder(Arc::clone(&replay_schema))
        .with_table_partition_cols(vec![Arc::new(Field::new(
            COL_LOG_VERSION,
            DataType::Int64,
            false,
        ))])
        .build();

    let parquet_checkpoint_scan: Option<Arc<dyn ExecutionPlan>> = {
        let mut source =
            datafusion::datasource::physical_plan::ParquetSource::new(table_schema.clone())
                .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
                    Arc::clone(&store),
                    metadata_cache,
                )));
        if let Some(predicate) = &options.parquet_predicate {
            source = source.with_predicate(Arc::clone(predicate));
        }
        let source: Arc<dyn datafusion::datasource::physical_plan::FileSource> = Arc::new(source);
        // Include V2 Parquet sidecars alongside any Parquet top-level checkpoint.
        let mut all_checkpoint_files = to_partitioned_files(parquet_checkpoint_metas)?;
        if let Some(cp_version) = checkpoint_version {
            let sidecar_partitioned = to_partitioned_files_with_version(sidecar_metas, cp_version)?;
            all_checkpoint_files.extend(sidecar_partitioned);
        }
        if all_checkpoint_files.is_empty() {
            None
        } else {
            let groups = group_partitioned_files(all_checkpoint_files, target_partitions);
            let conf = FileScanConfigBuilder::new(object_store_url.clone(), source)
                .with_file_groups(groups)
                .with_projection_indices(projection_indices.clone())?
                .build();
            Some(DataSourceExec::from_data_source(conf))
        }
    };

    let json_checkpoint_scan = build_json_log_scan(
        object_store_url.clone(),
        table_schema.clone(),
        json_checkpoint_metas,
        target_partitions,
        projection_indices.clone(),
    )?;

    let checkpoint_scan: Option<Arc<dyn ExecutionPlan>> =
        match (parquet_checkpoint_scan, json_checkpoint_scan) {
            (Some(parquet), Some(json)) => Some(UnionExec::try_new(vec![parquet, json])?),
            (Some(scan), None) | (None, Some(scan)) => Some(scan),
            (None, None) => None,
        };

    let commit_scan = build_json_log_scan(
        object_store_url,
        table_schema,
        commit_metas,
        target_partitions,
        projection_indices,
    )?;

    Ok((checkpoint_scan, commit_scan, checkpoint_files, commit_files))
}

#[expect(dead_code)]
pub async fn build_delta_log_datasource_union_with_options(
    ctx: &PlannerContext<'_>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    options: LogScanOptions,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<String>, Vec<String>)> {
    let (checkpoint_scan, commit_scan, checkpoint_files, commit_files) =
        build_delta_log_datasource_scans_with_options(
            ctx,
            checkpoint_files,
            commit_files,
            vec![],
            options,
        )
        .await?;

    let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    if let Some(cp) = checkpoint_scan {
        inputs.push(cp);
    }
    if let Some(c) = commit_scan {
        inputs.push(c);
    }

    Ok((UnionExec::try_new(inputs)?, checkpoint_files, commit_files))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::execution::cache::cache_manager::{CacheManager, CacheManagerConfig};
    use datafusion::prelude::SessionContext;
    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use parquet::arrow::ArrowWriter;

    use super::*;

    async fn put_parquet(
        store: &Arc<dyn ObjectStore>,
        path: &str,
        columns: Vec<(Field, ArrayRef)>,
    ) -> Result<ObjectMeta> {
        let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([("k".to_string(), "v".to_string())]),
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let path = Path::from(path);
        store.put(&path, PutPayload::from(buf)).await?;
        Ok(store.head(&path).await?)
    }

    #[tokio::test]
    async fn checkpoint_schema_inference_reads_footers_through_scoped_cache() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let field_metadata = HashMap::from([("f".to_string(), "m".to_string())]);
        let b = put_parquet(
            &store,
            "table/_delta_log/b.parquet",
            vec![
                (
                    Field::new("x", DataType::Int64, true).with_metadata(field_metadata.clone()),
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                ),
                (
                    Field::new("bin", DataType::Binary, true),
                    Arc::new(BinaryArray::from(vec![b"a".as_ref()])) as ArrayRef,
                ),
            ],
        )
        .await?;
        let a = put_parquet(
            &store,
            "table/_delta_log/a.parquet",
            vec![(
                Field::new("y", DataType::Int64, true).with_metadata(field_metadata),
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
            )],
        )
        .await?;
        let objects = vec![b, a];

        let ctx = SessionContext::new();
        let state = ctx.state();
        // DataFusion 55.2 gives the same schema here as the DataFusion commit credited above, since
        // the files have no extension type metadata and every field is nullable.
        let expected = ParquetFormat::default()
            .infer_schema(&state, &store, &objects)
            .await?;

        let shared =
            CacheManager::try_new(&CacheManagerConfig::default())?.get_file_metadata_cache();
        let url = ObjectStoreUrl::parse("memory://")?;
        let scoped = scoped_file_metadata_cache(Arc::clone(&shared), &url);
        let actual = infer_parquet_checkpoint_schema(&state, &store, &scoped, &objects).await?;

        assert_eq!(actual, expected);
        for object in &objects {
            assert!(scoped.contains_key(&object.location));
            assert!(!shared.contains_key(&object.location));
        }
        Ok(())
    }

    #[test]
    #[expect(clippy::expect_used)]
    fn replay_schema_relaxes_checkpoint_fields_and_restores_log_fields() -> Result<()> {
        let parsed = Field::new(
            "partitionValues_parsed",
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "category",
                DataType::Utf8,
                false,
            ))])),
            false,
        );
        let checkpoint = Schema::new(vec![Field::new(
            "add",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("path", DataType::Utf8, false)),
                Arc::new(parsed),
            ])),
            true,
        )]);

        let replay = replay_scan_schema(&checkpoint).expect("replay schema should merge");
        let add = replay
            .field_with_name("add")
            .expect("add should be present");
        let DataType::Struct(add_fields) = add.data_type() else {
            return Err(DataFusionError::Plan(
                "add should remain a struct".to_string(),
            ));
        };
        let parsed = add_fields
            .find("partitionValues_parsed")
            .map(|(_, field)| field)
            .expect("parsed partition values should be present");
        let DataType::Struct(parsed_fields) = parsed.data_type() else {
            return Err(DataFusionError::Plan(
                "parsed partition values should remain a struct".to_string(),
            ));
        };

        assert!(parsed.is_nullable());
        assert!(parsed_fields[0].is_nullable());
        assert!(add_fields.find("deletionVector").is_some());
        assert!(replay.field_with_name("remove").is_ok());
        assert!(replay.field_with_name("domainMetadata").is_ok());
        assert!(replay.field_with_name("checkpointMetadata").is_ok());
        assert!(replay.field_with_name("sidecar").is_ok());
        Ok(())
    }
}
