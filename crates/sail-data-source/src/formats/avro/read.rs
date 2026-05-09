// [CREDIT]: https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource-avro/src/file_format.rs

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::AvroSource;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::Result;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::TableSchema;

use crate::formats::avro::AvroReadFormat;
use crate::listing::source::{ListingScanInput, ReadFormat, SchemaInfer};

#[async_trait::async_trait]
impl ReadFormat for AvroReadFormat {
    fn create_read_format(
        &self,
        _compression: Option<CompressionTypeVariant>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn schema_inferrer(&self) -> Arc<dyn SchemaInfer> {
        Arc::new(crate::listing::source::DefaultSchemaInfer)
    }

    async fn scan(&self, _ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
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

        let source = AvroSource::new(table_schema);

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

