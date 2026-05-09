// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-parquet/src/file_format.rs

use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::TableSchema;
use crate::formats::parquet::ParquetReadFormat;
use crate::listing::source::{InferredFileMeta, ListingScanInput, ReadFormat, SchemaInfer};

pub(super) fn create_read_format(
    read: &ParquetReadFormat,
) -> Result<Arc<dyn FileFormat>> {
    let options = read.options.clone().into_table_options();
    Ok(Arc::new(ParquetFormat::default().with_options(options)))
}

pub(super) fn file_extension_override(read: &ParquetReadFormat) -> Result<Option<String>> {
    Ok(Some(read.options.extension.clone()))
}

pub(super) fn schema_inferrer() -> Arc<dyn SchemaInfer> {
    Arc::new(crate::listing::source::DefaultSchemaInfer)
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
        create_read_format(self)
    }

    fn file_extension_override(&self) -> Result<Option<String>> {
        file_extension_override(self)
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        schema_inferrer()
    }

    async fn infer_file_meta(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        file_schema: SchemaRef,
        object: &object_store::ObjectMeta,
    ) -> Result<InferredFileMeta> {
        let options = self.options.clone().into_table_options();
        let file_metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata = DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(options.global.metadata_size_hint)
            .with_file_metadata_cache(Some(file_metadata_cache))
            .fetch_metadata()
            .await?;
        let statistics =
            DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &file_schema)?;
        let ordering = datafusion::datasource::physical_plan::parquet::metadata::ordering_from_parquet_metadata(
            &metadata,
            &file_schema,
        )?;
        Ok(InferredFileMeta { statistics, ordering })
    }

    async fn scan(&self, ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        let options = self.options.clone().into_table_options();

        let partition_fields = input
            .table_partition_cols
            .iter()
            .map(|(col, data_type)| Arc::new(Field::new(col, data_type.clone(), false)))
            .collect::<Vec<_>>();
        let table_schema = TableSchema::new(Arc::clone(&input.file_schema), partition_fields);

        let mut source =
            ParquetSource::new(table_schema).with_table_parquet_options(options.clone());

        // Use the CachedParquetFileReaderFactory
        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let store = ctx.runtime_env().object_store(input.object_store_url.clone())?;
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
