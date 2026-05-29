use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::parquet::metadata::{
    ordering_from_parquet_metadata, DFParquetMetadata,
};
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use futures::{StreamExt, TryStreamExt};
use sail_common::spec;

use crate::listing::source::{ListingFileMeta, ListingScanInput, ReadFormat};
use crate::options::gen::ParquetReadOptions;

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
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let options = self.options.clone().into_table_options();
        Ok(Arc::new(ParquetFormat::default().with_options(options)))
    }

    fn file_extension_override(&self) -> Result<Option<String>> {
        Ok(Some(self.options.extension.clone()))
    }

    async fn infer_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        objects: &[object_store::ObjectMeta],
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

        let mut schemas: Vec<(object_store::path::Path, Schema)> = futures::stream::iter(objects)
            .map(|object| async {
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

        let merged = if options.global.skip_metadata {
            let base = Schema::try_merge(schemas.iter().map(|(_, schema)| clear_metadata(schema)))?;
            let metadata = Schema::try_merge(schemas.into_iter().map(|(_, schema)| schema))?;
            let base = apply_parquet_schema_options(base, &options);
            let metadata = apply_parquet_schema_options(metadata, &options);
            restore_spark_metadata_in_schema(base, &metadata)
        } else {
            let merged = Schema::try_merge(schemas.into_iter().map(|(_, schema)| schema))?;
            apply_parquet_schema_options(merged, &options)
        };

        Ok(Arc::new(merged))
    }

    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        file_schema: SchemaRef,
        object: &object_store::ObjectMeta,
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
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_preserve_order(input.preserve_order)
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build();

        Ok(config)
    }
}

fn restore_spark_metadata_in_schema(base: Schema, metadata: &Schema) -> Schema {
    let fields = restore_spark_metadata_in_fields(base.fields(), metadata.fields());
    Schema::new_with_metadata(fields, spark_metadata(metadata.metadata()))
}

fn restore_spark_metadata_in_fields(base: &Fields, metadata: &Fields) -> Vec<Field> {
    base.iter()
        .enumerate()
        .map(|(index, base)| {
            if let Some(metadata) = metadata.get(index) {
                restore_spark_metadata_in_field(base, metadata)
            } else {
                base.as_ref().clone()
            }
        })
        .collect()
}

fn restore_spark_metadata_in_field(base: &Field, metadata: &Field) -> Field {
    let data_type = restore_spark_metadata_in_type(base.data_type(), metadata.data_type());
    let mut field = base.as_ref().clone().with_data_type(data_type);
    field.set_metadata(spark_metadata(metadata.metadata()));
    field
}

fn restore_spark_metadata_in_type(base: &DataType, metadata: &DataType) -> DataType {
    match (base, metadata) {
        (DataType::List(base), DataType::List(metadata)) => {
            DataType::List(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::LargeList(base), DataType::LargeList(metadata)) => {
            DataType::LargeList(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::FixedSizeList(base, size), DataType::FixedSizeList(metadata, _)) => {
            DataType::FixedSizeList(
                Arc::new(restore_spark_metadata_in_field(base, metadata)),
                *size,
            )
        }
        (DataType::ListView(base), DataType::ListView(metadata)) => {
            DataType::ListView(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::LargeListView(base), DataType::LargeListView(metadata)) => {
            DataType::LargeListView(Arc::new(restore_spark_metadata_in_field(base, metadata)))
        }
        (DataType::Struct(base), DataType::Struct(metadata)) => DataType::Struct(Fields::from(
            restore_spark_metadata_in_fields(base, metadata),
        )),
        (DataType::Map(base, sorted), DataType::Map(metadata, _)) => DataType::Map(
            Arc::new(restore_spark_metadata_in_field(base, metadata)),
            *sorted,
        ),
        _ => base.clone(),
    }
}

fn spark_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    [
        spec::SPARK_METADATA_JSON_KEY,
        spec::SAIL_SPARK_UDT_METADATA_KEY,
        EXTENSION_TYPE_NAME_KEY,
        EXTENSION_TYPE_METADATA_KEY,
    ]
    .into_iter()
    .filter_map(|key| {
        metadata
            .get(key)
            .map(|value| (key.to_string(), value.clone()))
    })
    .collect()
}

fn apply_parquet_schema_options(schema: Schema, options: &TableParquetOptions) -> Schema {
    let schema = if options.global.binary_as_string {
        datafusion::datasource::file_format::parquet::transform_binary_to_string(&schema)
    } else {
        schema
    };
    if options.global.schema_force_view_types {
        datafusion::datasource::file_format::parquet::transform_schema_to_view(&schema)
    } else {
        schema
    }
}

fn clear_metadata(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Arc::new(clear_metadata_in_field(field)))
        .collect::<Fields>();
    Schema::new(fields)
}

fn clear_metadata_in_field(field: &Field) -> Field {
    field
        .as_ref()
        .clone()
        .with_data_type(clear_metadata_in_type(field.data_type()))
        .with_metadata(Default::default())
}

fn clear_metadata_in_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(Arc::new(clear_metadata_in_field(field))),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(clear_metadata_in_field(field))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(clear_metadata_in_field(field)), *size)
        }
        DataType::ListView(field) => DataType::ListView(Arc::new(clear_metadata_in_field(field))),
        DataType::LargeListView(field) => {
            DataType::LargeListView(Arc::new(clear_metadata_in_field(field)))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(clear_metadata_in_field(field)))
                .collect::<Fields>(),
        ),
        DataType::Map(field, sorted) => {
            DataType::Map(Arc::new(clear_metadata_in_field(field)), *sorted)
        }
        _ => data_type.clone(),
    }
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
