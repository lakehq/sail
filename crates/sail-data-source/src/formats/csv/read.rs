// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-csv/src/file_format.rs

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::CsvSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::TableSchema;

use crate::formats::csv::{CsvReadFormat, CsvSchemaInfer};
use crate::listing::source::{ListingScanInput, ReadFormat, SchemaInfer};

#[async_trait::async_trait]
impl ReadFormat for CsvReadFormat {
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
        Ok(Arc::new(CsvFormat::default().with_options(options)))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(CsvSchemaInfer {
            infer_schema: self.options.infer_schema,
        })
    }

    async fn scan(&self, ctx: &dyn Session, mut input: ListingScanInput) -> Result<FileScanConfig> {
        let mut options = self
            .options
            .clone()
            .into_table_options()
            .map_err(DataFusionError::from)?;
        if let Some(compression) = input.compression.take() {
            options.compression = compression;
        }

        // Consult configuration options for default values
        let has_header = options
            .has_header
            .unwrap_or_else(|| ctx.config_options().catalog.has_header);
        let newlines_in_values = options
            .newlines_in_values
            .unwrap_or_else(|| ctx.config_options().catalog.newlines_in_values);

        options.has_header = Some(has_header);
        options.newlines_in_values = Some(newlines_in_values);

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

        let source = CsvSource::new(table_schema).with_csv_options(options.clone());

        let config = FileScanConfigBuilder::new(input.object_store_url, Arc::new(source))
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

