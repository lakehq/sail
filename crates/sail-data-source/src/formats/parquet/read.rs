use std::sync::Arc;

use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
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
    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        _store: &Arc<dyn object_store::ObjectStore>,
        _objects: &[object_store::ObjectMeta],
    ) -> Result<CompressionTypeVariant> {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
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
