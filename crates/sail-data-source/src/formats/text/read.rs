use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::TableSchema;

use crate::formats::text::file_format::TextFileFormat;
use crate::formats::text::source::TextSource;
use crate::formats::text::TextReadFormat;
use crate::listing::source::{ListingScanInput, ReadFormat, SchemaInfer};

#[async_trait::async_trait]
impl ReadFormat for TextReadFormat {
    fn create_read_format(
        &self,
        compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(TextFileFormat::new(options)))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(crate::listing::source::DefaultSchemaInfer)
    }

    async fn scan(
        &self,
        _ctx: &dyn Session,
        mut input: ListingScanInput,
    ) -> Result<FileScanConfig> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        if let Some(compression) = input.compression.take() {
            options.compression = compression;
        }

        let partition_fields = input
            .table_partition_cols
            .iter()
            .map(|(col, data_type)| {
                Arc::new(datafusion::arrow::datatypes::Field::new(
                    col,
                    data_type.clone(),
                    false,
                ))
            })
            .collect::<Vec<_>>();
        let table_schema = TableSchema::new(Arc::clone(&input.file_schema), partition_fields);

        let line_sep = options.line_sep.map(|c| c as u8);
        let file_source = Arc::new(TextSource::new(table_schema, options.whole_text, line_sep));

        let config = FileScanConfigBuilder::new(input.object_store_url, file_source)
            .with_file_groups(input.file_groups)
            .with_constraints(input.constraints)
            .with_statistics(input.statistics)
            .with_projection_indices(input.projection)?
            .with_limit(input.limit)
            .with_output_ordering(input.output_ordering)
            .with_file_compression_type(FileCompressionType::from(options.compression))
            .with_preserve_order(input.preserve_order)
            .with_partitioned_by_file_group(input.partitioned_by_file_group)
            .build();

        Ok(config)
    }
}
