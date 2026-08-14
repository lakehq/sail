use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::metadata::{
    DFParquetMetadata, ordering_from_parquet_metadata,
};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result, plan_err};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use sail_common_datafusion::schema_evolution::SchemaEvolutionPhysicalExprAdapterFactory;

use crate::listing::source::{ListingFileMeta, ListingFileSample, ListingScanInput, ReadFormat};
use crate::options::r#gen::ParquetReadOptions;

#[derive(Debug, Clone)]
pub struct ParquetReadFormat {
    pub(super) options: ParquetReadOptions,
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

#[async_trait::async_trait]
impl ReadFormat for ParquetReadFormat {
    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant> {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
    }

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

        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata_size_hint = options.global.metadata_size_hint;
        let metadata_fetch_concurrency = ctx.config_options().execution.meta_fetch_concurrency;

        let objects = files
            .iter()
            .flat_map(|group| group.objects.iter().map(|object| (&group.store, object)));

        let mut schemas: Vec<(object_store::path::Path, Schema)> = futures::stream::iter(objects)
            .map(|(store, object)| async {
                let schema = DFParquetMetadata::new(store.as_ref(), object)
                    .with_metadata_size_hint(metadata_size_hint)
                    .with_file_metadata_cache(Some(Arc::clone(&metadata_cache)))
                    .with_coerce_int96(coerce_int96)
                    .fetch_schema()
                    .await?;
                Ok::<_, DataFusionError>((object.location.clone(), schema))
            })
            .boxed() // Workaround for https://github.com/rust-lang/rust/issues/64552
            // fetch schemas concurrently
            .buffer_unordered(metadata_fetch_concurrency)
            .try_collect()
            .await?;

        // Ensure deterministic ordering for stable schema inference.
        schemas.sort_unstable_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas.into_iter().map(|(_, schema)| schema);

        let merged = if options.global.skip_metadata {
            Schema::try_merge(schemas.map(clear_metadata))
        } else {
            Schema::try_merge(schemas)
        }?;

        let merged = if options.global.binary_as_string {
            datafusion::datasource::file_format::parquet::transform_binary_to_string(&merged)
        } else {
            merged
        };

        let merged = if options.global.schema_force_view_types {
            datafusion::datasource::file_format::parquet::transform_schema_to_view(&merged)
        } else {
            merged
        };

        Ok(Arc::new(merged))
    }

    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        file_schema: SchemaRef,
        _compression: CompressionTypeVariant,
    ) -> Result<ListingFileMeta> {
        let options = self.options.clone().into_table_options();
        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata = DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(options.global.metadata_size_hint)
            .with_file_metadata_cache(Some(metadata_cache))
            .fetch_metadata()
            .await?;
        let statistics =
            DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &file_schema)?;
        let ordering = ordering_from_parquet_metadata(&metadata, &file_schema)?;
        Ok(ListingFileMeta {
            statistics,
            ordering,
        })
    }

    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let options = self.options.clone().into_table_options();
        fail_for_encryption_factory(&options)?;

        let mut source =
            ParquetSource::new(input.schema).with_table_parquet_options(options.clone());

        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let store = ctx
            .runtime_env()
            .object_store(input.object_store_url.clone())?;
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
            .with_expr_adapter(Some(Arc::new(SchemaEvolutionPhysicalExprAdapterFactory {})))
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_preserve_order(input.preserve_order)
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build();

        Ok(config)
    }

    fn requires_explicit_schema_validation(&self) -> bool {
        true
    }

    fn validate_explicit_schema(&self, schema: &Schema, physical: &Schema) -> Result<()> {
        validate_parquet_schema(schema, physical)
    }

    fn path_glob_filter(&self) -> Option<&str> {
        self.options.path_glob_filter.as_deref()
    }
}

fn validate_parquet_schema(schema: &Schema, physical: &Schema) -> Result<()> {
    for field in schema.fields() {
        if let Ok(physical_field) = physical.field_with_name(field.name()) {
            validate_parquet_field(field.name(), field, physical_field)?;
        }
    }
    Ok(())
}

fn validate_parquet_field(path: &str, requested: &Field, physical: &Field) -> Result<()> {
    match (physical.data_type(), requested.data_type()) {
        (DataType::Struct(physical_fields), DataType::Struct(requested_fields)) => {
            for requested_field in requested_fields {
                if let Some(physical_field) = physical_fields
                    .iter()
                    .find(|field| field.name() == requested_field.name())
                {
                    validate_parquet_field(
                        &format!("{path}.{}", requested_field.name()),
                        requested_field,
                        physical_field,
                    )?;
                }
            }
            Ok(())
        }
        (DataType::List(physical_element), DataType::List(requested_element))
        | (DataType::LargeList(physical_element), DataType::LargeList(requested_element)) => {
            validate_parquet_field(&format!("{path}[]"), requested_element, physical_element)
        }
        (
            DataType::FixedSizeList(physical_element, physical_length),
            DataType::FixedSizeList(requested_element, requested_length),
        ) if physical_length == requested_length => {
            validate_parquet_field(&format!("{path}[]"), requested_element, physical_element)
        }
        (DataType::Map(physical_entries, _), DataType::Map(requested_entries, _)) => {
            validate_parquet_field(path, requested_entries, physical_entries)
        }
        (physical_type, requested_type)
            if parquet_primitive_type_compatible(physical_type, requested_type) =>
        {
            Ok(())
        }
        (physical_type, requested_type) => plan_err!(
            "[FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH] Data type mismatches when reading \
             Parquet column `{path}`. Expected {requested_type}, actual Parquet type {physical_type}."
        ),
    }
}

fn parquet_primitive_type_compatible(physical: &DataType, requested: &DataType) -> bool {
    if physical == requested {
        return true;
    }

    match (physical, requested) {
        // Spark's Parquet readers support these lossless numeric widenings.
        (DataType::Int32, DataType::Int64 | DataType::Float64)
        | (DataType::Float32, DataType::Float64)
        | (DataType::UInt8, DataType::Int16)
        | (DataType::UInt16, DataType::Int32)
        | (DataType::UInt32, DataType::Int64) => true,
        (DataType::UInt64, DataType::Decimal128(20, 0)) => true,
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            DataType::Decimal128(precision, scale),
        ) => decimal_integer_digits(*precision, *scale) >= 10,
        (DataType::Int64, DataType::Decimal128(precision, scale)) => {
            decimal_integer_digits(*precision, *scale) >= 20
        }
        (
            DataType::Decimal128(physical_precision, physical_scale),
            DataType::Decimal128(requested_precision, requested_scale),
        )
        | (
            DataType::Decimal256(physical_precision, physical_scale),
            DataType::Decimal256(requested_precision, requested_scale),
        ) => decimal_type_compatible(
            *physical_precision,
            *physical_scale,
            *requested_precision,
            *requested_scale,
        ),
        (
            DataType::Decimal128(physical_precision, physical_scale),
            DataType::Decimal256(requested_precision, requested_scale),
        ) => decimal_type_compatible(
            *physical_precision,
            *physical_scale,
            *requested_precision,
            *requested_scale,
        ),
        // INT96 carries no timezone marker, so the explicit schema determines the
        // timestamp family. Spark permits the same reinterpretation for Parquet.
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
        (DataType::Date32, DataType::Timestamp(_, None)) => true,
        (physical, requested) if is_string_type(physical) && is_string_type(requested) => true,
        (physical, requested) if is_binary_type(physical) && is_binary_type(requested) => true,
        _ => false,
    }
}

fn decimal_integer_digits(precision: u8, scale: i8) -> i16 {
    i16::from(precision) - i16::from(scale)
}

fn decimal_type_compatible(
    physical_precision: u8,
    physical_scale: i8,
    requested_precision: u8,
    requested_scale: i8,
) -> bool {
    let scale_increase = i16::from(requested_scale) - i16::from(physical_scale);
    let precision_increase = i16::from(requested_precision) - i16::from(physical_precision);
    scale_increase >= 0 && precision_increase >= scale_increase
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// Clears all metadata (Schema level and field level) for a schema.
fn clear_metadata(schema: Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Arc::new(field.as_ref().clone().with_metadata(Default::default())) // clear meta
        })
        .collect::<Fields>();
    Schema::new(fields)
}

/// Parses `coerce_int96` setting into an Arrow [`TimeUnit`].
///
/// This is adapted from DataFusion's Parquet data source implementation.
fn parse_coerce_int96_string(setting: &str) -> Result<TimeUnit> {
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
