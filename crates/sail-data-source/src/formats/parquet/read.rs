// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::metadata::{
    DFParquetMetadata, ordering_from_parquet_metadata,
};
use datafusion::datasource::physical_plan::parquet::{
    CachedParquetFileReaderFactory, Int96Coercer,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::basic::{
    ConvertedType, LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysicalType,
};
use datafusion::parquet::file::metadata::FileMetaData;
use datafusion::parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, SchemaDescriptor};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{
    ColumnStatistics, DataFusionError, Result, internal_datafusion_err, plan_err,
};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use sail_cache::scope::scoped_file_metadata_cache;
use sail_common::spec::SAIL_SPARK_INTERVAL_METADATA_KEY;
use sail_common_datafusion::metadata::type_metadata;
use sail_common_datafusion::min_max::min_max_agg::float_file_statistics_bounds;
use sail_common_datafusion::parquet_schema::{
    INTEGER_STORAGE_META_KEY, is_parquet_integer_target, normalize_parquet_field,
    parquet_integer_cast_wraps, parquet_temporal_storage_cast, validate_parquet_field,
};
use sail_common_datafusion::scan::load_parquet_scan_metadata_with_validation;
use sail_common_datafusion::schema::nullable_query_field;
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionCastMode, SchemaEvolutionPhysicalExprAdapterFactoryWithMatching,
    StructFieldMatching, find_matching_struct_field,
};
use sail_common_datafusion::session::config::SessionConfigService;

use crate::listing::source::{ListingFileMeta, ListingFileSample, ListingScanInput, ReadFormat};
use crate::listing::utils::SchemaSampling;
use crate::options::r#gen::ParquetReadOptions;

#[derive(Debug, Clone)]
pub struct ParquetReadFormat {
    pub(super) options: ParquetReadOptions,
    pub(super) matching: StructFieldMatching,
    pub(super) duration_hints: DurationHints,
}

fn fail_for_encryption_factory(options: &TableParquetOptions) -> Result<()> {
    if let Some(x) = &options.crypto.factory_id {
        Err(DataFusionError::Configuration(format!(
            "Parquet encryption factory ID is set to '{x}' but parquet encryption is unsupported"
        )))
    } else {
        Ok(())
    }
}

fn dictionary_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    }
}

/// Spark day-time intervals use Arrow duration[us] over unannotated Parquet INT64.
#[derive(Debug, Clone, Copy)]
pub(super) enum DurationHints {
    /// Ordinary Sail reads also recognize unqualified Arrow intervals.
    All,
    /// Spark-style schema inference recognizes intervals written with a Sail qualifier.
    Qualified,
    /// Physical scans use integer storage for both BIGINT and interval projections.
    Storage,
}

/// Arrow can carry temporal types only as embedded hints on ordinary integer leaves.
fn hinted_integer_type(field: &Field, durations: DurationHints) -> bool {
    match dictionary_value_type(field.data_type()) {
        DataType::Duration(TimeUnit::Microsecond) => match durations {
            DurationHints::All => false,
            DurationHints::Qualified => !field
                .metadata()
                .contains_key(SAIL_SPARK_INTERVAL_METADATA_KEY),
            DurationHints::Storage => true,
        },
        ty => matches!(
            ty,
            DataType::Timestamp(_, _)
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
        ),
    }
}

fn unannotated_integer(column: &ColumnDescriptor) -> Option<DataType> {
    if column.logical_type_ref().is_some() || column.converted_type() != ConvertedType::NONE {
        return None;
    }
    match column.physical_type() {
        PhysicalType::INT32 => Some(DataType::Int32),
        PhysicalType::INT64 => Some(DataType::Int64),
        _ => None,
    }
}

fn is_nanos_timestamp(column: &ColumnDescriptor) -> bool {
    matches!(column.logical_type_ref(), Some(LogicalType::Timestamp(ts)) if ts.unit == ParquetTimeUnit::NANOS)
}

/// Arrow lists and maps may rename or elide Parquet group wrappers. Both schemas
/// retain the same depth-first leaf order, so each leaf consumes one descriptor.
fn reconcile_parquet_field<'a>(
    field: &Arc<Field>,
    columns: &mut impl Iterator<Item = &'a ColumnDescPtr>,
    for_integer_read: bool,
    durations: DurationHints,
) -> Result<Arc<Field>> {
    let data_type = dictionary_value_type(field.data_type());
    let ty = match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| reconcile_parquet_field(f, columns, for_integer_read, durations))
                .collect::<Result<Fields>>()?,
        ),
        DataType::List(child) => DataType::List(reconcile_parquet_field(
            child,
            columns,
            for_integer_read,
            durations,
        )?),
        DataType::LargeList(child) => DataType::LargeList(reconcile_parquet_field(
            child,
            columns,
            for_integer_read,
            durations,
        )?),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(
            reconcile_parquet_field(child, columns, for_integer_read, durations)?,
            *size,
        ),
        DataType::Map(entries, sorted) => DataType::Map(
            reconcile_parquet_field(entries, columns, for_integer_read, durations)?,
            *sorted,
        ),
        _ => {
            let column = columns.next().ok_or_else(|| {
                internal_datafusion_err!(
                    "Missing Parquet descriptor for Arrow field {}",
                    field.name()
                )
            })?;
            if is_nanos_timestamp(column)
                && let DataType::Timestamp(_, zone) = data_type
            {
                // Arrow can apply a microsecond INT96 hint to an INT64 NANOS timestamp.
                // Keep its actual unit and storage for raw BIGINT reads and timestamp validation.
                let mut field = field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, zone.clone()));
                field
                    .metadata_mut()
                    .insert(INTEGER_STORAGE_META_KEY.to_owned(), "INT64".to_owned());
                return Ok(Arc::new(field));
            }
            if hinted_integer_type(field, durations)
                && let Some(storage) = unannotated_integer(column)
            {
                return Ok(Arc::new(field.as_ref().clone().with_data_type(storage)));
            }
            match data_type {
                DataType::Date64
                    if for_integer_read && column.physical_type() == PhysicalType::INT32 =>
                {
                    DataType::Date32
                }
                ty if for_integer_read
                    && (matches!(ty, DataType::Timestamp(_, _)) || ty.is_decimal()) =>
                {
                    // The same Arrow type can originate from different Parquet physical types.
                    let mut field = field.as_ref().clone().with_data_type(data_type.clone());
                    match column.physical_type() {
                        ty @ (PhysicalType::INT32 | PhysicalType::INT64) => {
                            field
                                .metadata_mut()
                                .insert(INTEGER_STORAGE_META_KEY.to_owned(), ty.to_string());
                        }
                        _ => {
                            field.metadata_mut().remove(INTEGER_STORAGE_META_KEY);
                        }
                    }
                    return Ok(Arc::new(field));
                }
                _ => return Ok(Arc::clone(field)),
            }
        }
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(ty)))
}

/// Spark reads INT96 as TIMESTAMP whatever the `ARROW:schema` hint says, while `Int96Coercer`
/// coerces an INT96 column to `data_type` only when the hint gives nanoseconds. pyarrow hints the
/// unit of the written values, such as `timestamp[us]`, which would otherwise read as
/// TIMESTAMP_NTZ.
// TODO: Coerce nested INT96 values with such a hint.
fn coerce_hinted_int96_timestamps(
    schema: Schema,
    parquet_schema: &SchemaDescriptor,
    data_type: &DataType,
) -> Schema {
    let int96 = parquet_schema
        .columns()
        .iter()
        .filter(|column| {
            column.physical_type() == PhysicalType::INT96 && column.path().parts().len() == 1
        })
        .map(|column| column.name())
        .collect::<Vec<_>>();
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Timestamp(_, None) if int96.contains(&field.name().as_str()) => {
                field.as_ref().clone().with_data_type(data_type.clone())
            }
            _ => field.as_ref().clone(),
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn parquet_file_schema(
    file_metadata: &FileMetaData,
    coerce_int96: Option<TimeUnit>,
    coerce_int96_tz: Option<&Arc<str>>,
    durations: DurationHints,
) -> Result<Schema> {
    // Arrow-hinted schema: mirror `DFParquetMetadata::fetch_schema` exactly, which
    // reads the `ARROW:schema` key-value hint and then applies `coerce_int96` to the
    // INT96-derived columns. Reproduced here (rather than calling `fetch_schema()`)
    // so the inferred types stay identical to today for hinted and INT96 columns.
    // INT96 columns coerce to a UTC timestamp, matching the reader's `coerce_int96_tz`.
    let schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    let schema = coerce_int96
        .as_ref()
        .and_then(|time_unit| {
            Int96Coercer::new(file_metadata.schema_descr(), &schema, time_unit)
                .with_timezone(coerce_int96_tz.cloned())
                .coerce()
        })
        .unwrap_or(schema);
    let schema = match &coerce_int96 {
        Some(time_unit) => coerce_hinted_int96_timestamps(
            schema,
            file_metadata.schema_descr(),
            &DataType::Timestamp(*time_unit, coerce_int96_tz.cloned()),
        ),
        None => schema,
    };
    Ok(reconciled_read_schema(
        file_metadata,
        &schema,
        &[],
        StructFieldMatching::Name,
        durations,
    )?
    .unwrap_or(schema))
}

/// Return the logical type represented by the footer statistics and its integer storage.
fn temporal_statistics_types(
    source: &Field,
    requested: &DataType,
    descriptor: &SchemaDescriptor,
) -> Option<(DataType, DataType)> {
    let ty = dictionary_value_type(source.data_type());
    if ty == &DataType::Date64 && requested.is_numeric() {
        let column = descriptor
            .columns()
            .iter()
            .find(|c| c.path().parts().len() == 1 && c.name() == source.name())?;
        // Arrow represents a DATE as milliseconds when a Date64 hint is present.
        // An unannotated INT64 carrying the same hint retains all its raw ticks.
        return match column.physical_type() {
            PhysicalType::INT32 => parquet_temporal_storage_cast(&DataType::Date32, requested)
                .map(|storage| (DataType::Date32, storage)),
            PhysicalType::INT64 if requested == &DataType::Int64 => {
                Some((DataType::Int64, DataType::Int64))
            }
            _ => None,
        };
    }
    parquet_temporal_storage_cast(ty, requested).map(|storage| (ty.clone(), storage))
}

/// Use the descriptor for unannotated integer leaves, independent of projection.
/// Projected integer readers also need the backing type of annotated timestamps and decimals.
fn reconciled_read_schema(
    metadata: &FileMetaData,
    schema: &Schema,
    integer_fields: &[Arc<Field>],
    matching: StructFieldMatching,
    durations: DurationHints,
) -> Result<Option<Schema>> {
    let descriptor = metadata.schema_descr();
    let leaves = schema.flattened_fields();
    let has_hints = leaves.iter().any(|f| hinted_integer_type(f, durations))
        && descriptor
            .columns()
            .iter()
            .any(|column| unannotated_integer(column).is_some());
    let mut integer_roots = vec![false; schema.fields().len()];
    if !integer_fields.is_empty()
        && leaves.iter().any(|f| {
            let ty = dictionary_value_type(f.data_type());
            ty.is_decimal() || matches!(ty, DataType::Timestamp(_, _) | DataType::Date64)
        })
    {
        for field in integer_fields {
            if let Some((index, _)) = find_matching_struct_field(schema.fields(), field, matching)?
            {
                integer_roots[index] = true;
            }
        }
    }
    if !has_hints
        && !integer_roots.contains(&true)
        && !descriptor.columns().iter().any(|c| is_nanos_timestamp(c))
    {
        return Ok(None);
    }

    let mut columns = descriptor.columns().iter();
    let fields = schema
        .fields()
        .iter()
        .zip(integer_roots)
        .map(|(field, integer_read)| {
            reconcile_parquet_field(field, &mut columns, integer_read, durations)
        })
        .collect::<Result<Fields>>()?;
    Ok((fields != schema.fields)
        .then(|| Schema::new_with_metadata(fields, schema.metadata.clone())))
}

#[async_trait::async_trait]
impl ReadFormat for ParquetReadFormat {
    fn configure_schema_inference(
        &mut self,
        ctx: &dyn Session,
        hadoop_paths: bool,
        case_sensitive: bool,
    ) -> Result<SchemaSampling> {
        self.matching = if !case_sensitive {
            StructFieldMatching::CaseInsensitiveName
        } else {
            StructFieldMatching::Name
        };
        self.duration_hints = if hadoop_paths || self.options.merge_schema.is_some() {
            DurationHints::Qualified
        } else {
            DurationHints::All
        };
        match self.options.merge_schema {
            None if hadoop_paths => return Ok(SchemaSampling::SingleParquet),
            None => return Ok(SchemaSampling::SampleParquet),
            Some(false) => return Ok(SchemaSampling::SingleParquet),
            Some(true) => {}
        }
        let key = "spark.sql.parquet.respectSummaryFiles";
        let summaries_only = ctx
            .config()
            .get_extension::<SessionConfigService>()
            .map(|service| service.get_option(key))
            .transpose()?
            .flatten()
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        Ok(SchemaSampling::MergeParquet { summaries_only })
    }

    fn statistics_field_matching(&self) -> StructFieldMatching {
        self.matching
    }

    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant> {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
    }

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/datasource-parquet/src/file_format.rs
    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        files: &[ListingFileSample<'_>],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        let options = self.options.clone().into_table_options();
        fail_for_encryption_factory(&options)?;

        let coerce_int96 = options
            .global
            .coerce_int96
            .as_deref()
            .map(parse_coerce_int96_string)
            .transpose()?;
        let coerce_int96_tz = options.global.coerce_int96_tz.as_deref().map(Arc::from);

        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata_size_hint = options.global.metadata_size_hint;
        let metadata_fetch_concurrency: usize =
            ctx.config_options().execution.meta_fetch_concurrency.into();

        let objects = files.iter().flat_map(|group| {
            let metadata_cache = scoped_file_metadata_cache(
                Arc::clone(&metadata_cache),
                &group.url.object_store(),
                ctx.session_id(),
            );
            group
                .objects
                .iter()
                .map(move |object| (&group.store, Arc::clone(&metadata_cache), object))
        });

        let mut schemas: Vec<(object_store::path::Path, Schema)> = futures::stream::iter(objects)
            .map(|(store, metadata_cache, object)| async {
                let reader = DFParquetMetadata::new(store.as_ref(), object)
                    .with_metadata_size_hint(metadata_size_hint)
                    .with_file_metadata_cache(Some(metadata_cache))
                    .with_coerce_int96(coerce_int96);
                // Fetch the Parquet footer once and derive both schemas from it.
                // `fetch_schema()` would read (and parse) the footer a second time; when the
                // file-metadata cache is disabled (`file_metadata_cache.type=none`, a supported
                // zero-capacity mode) that second read is not cached, doubling remote footer I/O
                // during schema inference.
                let metadata = reader.fetch_metadata().await?;
                let schema = parquet_file_schema(
                    metadata.file_metadata(),
                    coerce_int96,
                    coerce_int96_tz.as_ref(),
                    self.duration_hints,
                )?;
                Ok::<_, DataFusionError>((object.location.clone(), schema))
            })
            .boxed() // Workaround for https://github.com/rust-lang/rust/issues/64552
            // fetch schemas concurrently
            .buffer_unordered(metadata_fetch_concurrency)
            .try_collect()
            .await?;

        // Ensure deterministic ordering for stable schema inference.
        // Spark merges data schemas first, followed by metadata and common metadata.
        let order = |path: &object_store::path::Path| match path.filename() {
            Some("_common_metadata") => 2,
            Some("_metadata") => 1,
            _ => 0,
        };
        schemas.sort_unstable_by(|(a, _), (b, _)| (order(a), a).cmp(&(order(b), b)));

        let session_timezone = ctx
            .config_options()
            .execution
            .time_zone
            .as_deref()
            .unwrap_or("UTC");
        let merge_schema = self.options.merge_schema == Some(true);
        // Spark merges adjusted-to-UTC timestamps in the session timezone regardless of the
        // timezone labels in the embedded Arrow schema. Legacy sampling only retags fields
        // that participate in millisecond widening.
        let merge_timezone = merge_schema.then_some(session_timezone);
        if !merge_schema {
            normalize_widened_timestamp_timezones(&mut schemas, session_timezone);
        }
        let schemas = schemas.into_iter().map(|(_, mut schema)| {
            schema.fields = schema
                .fields
                .iter()
                .map(|field| {
                    normalize_parquet_field(field, merge_timezone, options.global.binary_as_string)
                })
                .collect();
            if options.global.skip_metadata {
                clear_metadata(schema)
            } else {
                schema
            }
        });
        let merged = if merge_schema {
            merge_parquet_schemas(schemas, self.matching)?
        } else {
            Schema::try_merge(schemas)?
        };

        let merged = if options.global.schema_force_view_types {
            datafusion::datasource::file_format::parquet::transform_schema_to_view(&merged)
        } else {
            merged
        };

        let fields = merged
            .fields()
            .iter()
            .map(|field| nullable_query_field(field))
            .collect::<Vec<_>>();
        Ok(Arc::new(Schema::new_with_metadata(fields, merged.metadata)))
    }
    // SPDX-SnippetEnd

    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        object_store_url: &ObjectStoreUrl,
        object: &ObjectMeta,
        file_schema: SchemaRef,
        _compression: CompressionTypeVariant,
    ) -> Result<ListingFileMeta> {
        let options = self.options.clone().into_table_options();
        let metadata_cache = scoped_file_metadata_cache(
            ctx.runtime_env().cache_manager.get_file_metadata_cache(),
            object_store_url,
            ctx.session_id(),
        );
        let metadata = DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(options.global.metadata_size_hint)
            .with_file_metadata_cache(Some(metadata_cache))
            .fetch_metadata()
            .await?;
        // Read integer statistics with their physical signedness before converting
        // bounds to the scan type. UINT32 and UINT64 can exceed the signed range.
        let file_metadata = metadata.file_metadata();
        let physical = parquet_file_schema(file_metadata, None, None, DurationHints::Storage)?;
        let mut ambiguous_indices = Vec::new();
        let fields = file_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok(
                    match find_matching_struct_field(physical.fields(), field, self.matching) {
                        Ok(Some((_, source)))
                            if source.data_type().is_unsigned_integer()
                                || (dictionary_value_type(source.data_type()).is_decimal()
                                    && is_parquet_integer_target(field.data_type()))
                                || temporal_statistics_types(
                                    source,
                                    field.data_type(),
                                    file_metadata.schema_descr(),
                                )
                                .is_some()
                                || parquet_integer_cast_wraps(
                                    source.data_type(),
                                    field.data_type(),
                                ) =>
                        {
                            Arc::clone(source)
                        }
                        Ok(Some((_, source))) if source.name() != field.name() => {
                            Arc::new(field.as_ref().clone().with_name(source.name()))
                        }
                        Ok(_) => Arc::clone(field),
                        Err(_) if self.matching == StructFieldMatching::CaseInsensitiveName => {
                            ambiguous_indices.push(index);
                            Arc::clone(field)
                        }
                        Err(error) => return Err(error),
                    },
                )
            })
            .collect::<Result<Fields>>()?;
        let stats_schema = Arc::new(Schema::new(fields));
        let mut statistics =
            DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &stats_schema)?;
        let num_rows = match statistics.num_rows {
            Precision::Exact(num_rows) => Some(num_rows),
            _ => None,
        };
        for ((field, source), column) in file_schema
            .fields()
            .iter()
            .zip(stats_schema.fields())
            .zip(statistics.column_statistics.iter_mut())
        {
            if dictionary_value_type(source.data_type()).is_decimal()
                && is_parquet_integer_target(field.data_type())
            {
                // Footer bounds describe scaled decimal values. The scan returns unscaled integers.
                *column = ColumnStatistics::new_unknown().with_null_count(column.null_count);
            } else if parquet_integer_cast_wraps(source.data_type(), field.data_type()) {
                // Wrapping can reorder values and map different source values to the same integer.
                *column = ColumnStatistics::new_unknown().with_null_count(column.null_count);
            } else if let Some((from, storage)) =
                temporal_statistics_types(source, field.data_type(), file_metadata.schema_descr())
            {
                if parquet_integer_cast_wraps(&from, &storage)
                    || parquet_integer_cast_wraps(&storage, field.data_type())
                {
                    *column = ColumnStatistics::new_unknown().with_null_count(column.null_count);
                } else {
                    let convert = |bound: &Precision<datafusion_common::ScalarValue>| {
                        bound
                            .cast_to(&from)?
                            .cast_to(&storage)?
                            .cast_to(field.data_type())
                    };
                    column.min_value = convert(&column.min_value).unwrap_or(Precision::Absent);
                    column.max_value = convert(&column.max_value).unwrap_or(Precision::Absent);
                }
            } else if source.data_type().is_unsigned_integer()
                && source.data_type() != field.data_type()
            {
                let mut converted =
                    ColumnStatistics::new_unknown().with_null_count(column.null_count);
                if field.data_type().is_numeric() {
                    converted.min_value = column
                        .min_value
                        .cast_to(field.data_type())
                        .unwrap_or(Precision::Absent);
                    converted.max_value = column
                        .max_value
                        .cast_to(field.data_type())
                        .unwrap_or(Precision::Absent);
                }
                *column = converted;
            }
            // TODO: Row-group and page pruning still use the float maximum, which leaves out NaN.
            if field.data_type().is_floating() {
                let non_null_count = match (num_rows, &column.null_count) {
                    (Some(num_rows), Precision::Exact(null_count)) => {
                        num_rows.checked_sub(*null_count)
                    }
                    _ => None,
                };
                (column.min_value, column.max_value) = float_file_statistics_bounds(
                    std::mem::take(&mut column.min_value),
                    std::mem::take(&mut column.max_value),
                    non_null_count,
                );
            }
        }
        // Exact-name footer statistics cannot describe an ambiguous case-insensitive match.
        let ordering =
            if ambiguous_indices.is_empty() && file_schema.fields() == stats_schema.fields() {
                ordering_from_parquet_metadata(&metadata, &file_schema)?
            } else {
                None
            };
        for index in ambiguous_indices {
            statistics.column_statistics[index] = ColumnStatistics::new_unknown();
        }
        Ok(ListingFileMeta {
            statistics,
            ordering,
        })
    }

    // SPDX-SnippetBegin
    // SPDX-License-Identifier: Apache-2.0
    // SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/datasource-parquet/src/file_format.rs
    async fn scan(&self, ctx: &dyn Session, mut input: ListingScanInput) -> Result<FileScanConfig> {
        let options = self.options.clone().into_table_options();
        fail_for_encryption_factory(&options)?;

        if let Some(unit) = &options.global.coerce_int96 {
            parse_coerce_int96_string(unit)?;
        }

        // Partition columns have no physical field. Validate only projected file columns.
        let file_schema = input.schema.file_schema();
        let projected: Fields = match &input.projection {
            Some(indices) => indices
                .iter()
                .filter_map(|&i| file_schema.fields().get(i).cloned())
                .collect(),
            None => file_schema.fields().clone(),
        };
        let mut source =
            ParquetSource::new(input.schema).with_table_parquet_options(options.clone());

        let metadata_cache = scoped_file_metadata_cache(
            ctx.runtime_env().cache_manager.get_file_metadata_cache(),
            &input.object_store_url,
            ctx.session_id(),
        );
        let store = ctx.runtime_env().object_store(&input.object_store_url)?;

        let integer_fields: Fields = projected
            .iter()
            .filter(|field| {
                Schema::new([Arc::clone(field)])
                    .flattened_fields()
                    .iter()
                    .any(|leaf| is_parquet_integer_target(leaf.data_type()))
            })
            .cloned()
            .collect();
        input.file_groups = load_parquet_scan_metadata_with_validation(
            input.file_groups,
            &store,
            &metadata_cache,
            options.global.metadata_size_hint,
            ctx.config_options().execution.meta_fetch_concurrency.into(),
            |file, metadata| {
                // Physical projection pushdown selects struct leaves after this pass.
                // The expression adapter validates them once it has the final projection.
                if projected.is_empty() {
                    return Ok(());
                }
                let mut physical = parquet_to_arrow_schema(
                    metadata.schema_descr(),
                    metadata.key_value_metadata(),
                )?;
                if let Some(schema) = reconciled_read_schema(
                    metadata,
                    &physical,
                    &integer_fields,
                    self.matching,
                    DurationHints::Storage,
                )? {
                    physical = schema;
                    file.arrow_schema = Some(Arc::new(physical.clone()));
                }
                for field in projected
                    .iter()
                    .filter(|field| !matches!(field.data_type(), DataType::Struct(_)))
                {
                    if let Some((_, actual)) =
                        find_matching_struct_field(physical.fields(), field, self.matching)?
                    {
                        validate_parquet_field(field.name(), field, actual, self.matching)?;
                    }
                }
                Ok(())
            },
        )
        .await?;
        let cached_parquet_read_factory =
            Arc::new(CachedParquetFileReaderFactory::new(store, metadata_cache));
        source = source.with_parquet_file_reader_factory(cached_parquet_read_factory);

        if let Some(metadata_size_hint) = options.global.metadata_size_hint {
            source = source.with_metadata_size_hint(metadata_size_hint)
        }

        let config = FileScanConfigBuilder::new(input.object_store_url, Arc::new(source))
            .with_file_groups(input.file_groups)
            .with_constraints(input.constraints)
            .with_statistics(input.statistics)
            .with_expr_adapter(Some(Arc::new(
                SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::with_cast_mode(
                    self.matching,
                    SchemaEvolutionCastMode::Parquet,
                ),
            )))
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_preserve_order(input.preserve_order)
            .with_output_partitioning(input.output_partitioning)
            .build();

        Ok(config)
    }
    // SPDX-SnippetEnd

    fn path_glob_filter(&self) -> Option<&str> {
        self.options.path_glob_filter.as_deref()
    }
}

/// Spark merges corresponding struct fields in first-file order and widens decimal
/// precision when the scales agree. Lists and maps merge their child types recursively.
fn merge_parquet_schemas(
    mut schemas: impl Iterator<Item = Schema>,
    matching: StructFieldMatching,
) -> Result<Schema> {
    let Some(first) = schemas.next() else {
        return Ok(Schema::empty());
    };
    let mut merged = first.fields.clone();
    for schema in schemas {
        if merged != schema.fields {
            merged = merge_parquet_fields(&merged, &schema.fields, matching)?;
        }
    }
    Ok(Schema::new_with_metadata(merged, first.metadata))
}

fn merge_parquet_fields(
    left: &Fields,
    right: &Fields,
    matching: StructFieldMatching,
) -> Result<Fields> {
    let mut fields = Vec::new();
    let mut used = vec![false; right.len()];
    for field in left {
        fields.push(match find_matching_struct_field(right, field, matching)? {
            Some((i, other)) => {
                used[i] = true;
                merge_parquet_field(field, other, matching)?
            }
            None => Arc::new(field.as_ref().clone().with_nullable(true)),
        });
    }
    fields.extend(
        right
            .iter()
            .zip(used)
            .filter(|(_, used)| !used)
            .map(|(field, _)| Arc::new(field.as_ref().clone().with_nullable(true))),
    );
    Ok(fields.into())
}

fn merge_parquet_field(
    left: &Field,
    right: &Field,
    matching: StructFieldMatching,
) -> Result<Arc<Field>> {
    let ty = match (left.data_type(), right.data_type()) {
        (a, b) if a == b => a.clone(),
        (DataType::Decimal128(a, s), DataType::Decimal128(b, t)) if s == t => {
            DataType::Decimal128(*a.max(b), *s)
        }
        (DataType::Struct(a), DataType::Struct(b)) => {
            DataType::Struct(merge_parquet_fields(a, b, matching)?)
        }
        (DataType::List(a), DataType::List(b)) => {
            DataType::List(merge_parquet_field(a, b, matching)?)
        }
        (DataType::Map(a, sorted), DataType::Map(b, _)) => {
            DataType::Map(merge_parquet_field(a, b, matching)?, *sorted)
        }
        (a, b) => {
            return plan_err!(
                "Cannot merge Parquet column '{}' with types {a} and {b}",
                left.name()
            );
        }
    };
    Ok(Arc::new(
        left.clone()
            .with_data_type(ty)
            .with_nullable(left.is_nullable() || right.is_nullable()),
    ))
}

/// Retags producer-specific Arrow timezone labels to the Spark session timezone on timestamp
/// fields that participate in the millisecond widening, so a directory mixing e.g.
/// `timestamp[ms, UTC]` and `timestamp[us, America/New_York]` merges instead of failing at
/// [`Schema::try_merge`].
///
/// Spark maps every timezone-aware (adjusted-to-UTC) timestamp to a single session-zoned
/// `TimestampType`, and Arrow defines relabeling one nonempty timezone to another as
/// metadata-only (the stored instants are identical), so the retag is lossless. A `None` timezone
/// (`TIMESTAMP_NTZ`) is preserved untouched so it stays distinct from the aware type. Only fields
/// that appear as timezone-aware milliseconds in at least one file are retagged; a
/// microsecond-only field keeps its producer label in legacy sampling mode.
fn normalize_widened_timestamp_timezones(
    schemas: &mut [(object_store::path::Path, Schema)],
    session_timezone: &str,
) {
    let widened_timestamp_fields = schemas
        .iter()
        .flat_map(|(_, schema)| schema.fields().iter())
        .filter_map(|field| match field.data_type() {
            DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => Some(field.name().clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    for (_, schema) in schemas {
        let metadata = schema.metadata().clone();
        let fields = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Timestamp(
                    unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond),
                    Some(_),
                ) if widened_timestamp_fields.contains(field.name()) => field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Timestamp(*unit, Some(session_timezone.into()))),
                _ => field.as_ref().clone(),
            })
            .collect::<Vec<_>>();
        *schema = Schema::new_with_metadata(fields, metadata);
    }
}

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf/datafusion/datasource-parquet/src/file_format.rs
/// Clears all metadata (Schema level and field level) for a schema.
fn clear_metadata(schema: Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let metadata = if field
                .metadata()
                .contains_key(SAIL_SPARK_INTERVAL_METADATA_KEY)
            {
                type_metadata(&FieldMetadata::from(field.as_ref())).to_hashmap()
            } else {
                Default::default()
            };
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Fields>();
    Schema::new(fields)
}
// SPDX-SnippetEnd

/// Parses `coerce_int96` setting into an Arrow [`TimeUnit`].
///
/// This is adapted from DataFusion's Parquet data source implementation.
pub fn parse_coerce_int96_string(setting: &str) -> Result<TimeUnit> {
    match setting.to_lowercase().as_str() {
        "ns" => Ok(TimeUnit::Nanosecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ms" => Ok(TimeUnit::Millisecond),
        "s" => Ok(TimeUnit::Second),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet `coerce_int96` setting: {setting}. Valid values are: ns, us, ms, and s."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp_field(name: &str, unit: TimeUnit, tz: Option<&str>) -> Field {
        Field::new(name, DataType::Timestamp(unit, tz.map(Into::into)), true)
    }

    fn located(name: &str, field: Field) -> (object_store::path::Path, Schema) {
        (
            object_store::path::Path::from(name),
            Schema::new(vec![field]),
        )
    }

    #[test]
    fn retags_aware_fields_participating_in_millisecond_widening() {
        // `ts` is aware microseconds (New York) in one file and aware milliseconds (UTC) in
        // another; both are retagged to the session timezone so the later widening + merge agree.
        let mut schemas = vec![
            located(
                "a",
                timestamp_field("ts", TimeUnit::Microsecond, Some("America/New_York")),
            ),
            located(
                "b",
                timestamp_field("ts", TimeUnit::Millisecond, Some("UTC")),
            ),
        ];
        normalize_widened_timestamp_timezones(&mut schemas, "America/Los_Angeles");
        let tz = Some("America/Los_Angeles".into());
        assert_eq!(
            schemas[0].1.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
        );
        assert_eq!(
            schemas[1].1.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, tz)
        );
    }

    #[test]
    fn preserves_timestamp_ntz_even_for_a_widened_field() {
        // `ts` participates in the widening (aware ms in file a) but is TIMESTAMP_NTZ in file b;
        // the `None` timezone must survive so it stays distinct from the aware type.
        let mut schemas = vec![
            located(
                "a",
                timestamp_field("ts", TimeUnit::Millisecond, Some("UTC")),
            ),
            located("b", timestamp_field("ts", TimeUnit::Microsecond, None)),
        ];
        normalize_widened_timestamp_timezones(&mut schemas, "America/Los_Angeles");
        assert_eq!(
            schemas[0].1.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("America/Los_Angeles".into()))
        );
        assert_eq!(
            schemas[1].1.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn leaves_microsecond_only_fields_untouched() {
        // No file has this field as aware milliseconds, so each producer label is retained
        // (the pre-existing microsecond-only case, deferred).
        let mut schemas = vec![
            located(
                "a",
                timestamp_field("ts", TimeUnit::Microsecond, Some("UTC")),
            ),
            located(
                "b",
                timestamp_field("ts", TimeUnit::Microsecond, Some("America/New_York")),
            ),
        ];
        normalize_widened_timestamp_timezones(&mut schemas, "America/Los_Angeles");
        assert_eq!(
            schemas[0].1.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schemas[1].1.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()))
        );
    }
}
