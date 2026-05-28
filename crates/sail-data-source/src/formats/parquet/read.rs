use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
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
use sail_common::spec;

use crate::listing::source::{ListingFileMeta, ListingScanInput, ReadFormat, SchemaInfer};
use crate::options::gen::ParquetReadOptions;

#[derive(Debug, Clone)]
pub struct ParquetReadFormat {
    pub(super) options: ParquetReadOptions,
}

fn set_source_encryption_factory(
    options: &TableParquetOptions,
    source: ParquetSource,
) -> Result<ParquetSource> {
    if let Some(encryption_factory_id) = &options.crypto.factory_id {
        Err(DataFusionError::Configuration(format!(
            "Parquet encryption factory id is set to '{encryption_factory_id}' but the parquet_encryption feature is disabled"
        )))
    } else {
        Ok(source)
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

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(ParquetSchemaInfer {
            options: self.options.clone(),
        })
    }

    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        file_schema: SchemaRef,
        object: &object_store::ObjectMeta,
    ) -> Result<ListingFileMeta> {
        let options = self.options.clone().into_table_options();
        let file_metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata = DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(options.global.metadata_size_hint)
            .with_file_metadata_cache(Some(file_metadata_cache))
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
        let mut source =
            ParquetSource::new(input.schema).with_table_parquet_options(options.clone());

        // Use the CachedParquetFileReaderFactory
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

        source = set_source_encryption_factory(&options, source)?;

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

#[derive(Debug)]
struct ParquetSchemaInfer {
    options: ParquetReadOptions,
}

#[async_trait::async_trait]
impl SchemaInfer for ParquetSchemaInfer {
    async fn get_schema(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[object_store::ObjectMeta],
        _list_options: &datafusion::datasource::listing::ListingOptions,
    ) -> Result<Schema> {
        let base_options = self.options.clone().into_table_options();
        let base_schema = ParquetFormat::default()
            .with_options(base_options.clone())
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone();

        if !base_options.global.skip_metadata {
            return Ok(base_schema);
        }

        let mut metadata_options = base_options;
        metadata_options.global.skip_metadata = false;
        let metadata_schema = ParquetFormat::default()
            .with_options(metadata_options)
            .infer_schema(ctx, store, files)
            .await?
            .as_ref()
            .clone();

        Ok(restore_spark_metadata_in_schema(
            base_schema,
            &metadata_schema,
        ))
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
        spec::SAIL_SPARK_INTERVAL_METADATA_KEY,
    ]
    .into_iter()
    .filter_map(|key| {
        metadata
            .get(key)
            .map(|value| (key.to_string(), value.clone()))
    })
    .collect()
}
