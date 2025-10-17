use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use parquet::file::properties::WriterProperties;

use crate::spec::partition::UnboundPartitionSpec;
use crate::spec::Schema as IcebergSchema;

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub table_schema: ArrowSchemaRef,
    pub partition_columns: Vec<String>,
    pub writer_properties: WriterProperties,
    pub target_file_size: u64,
    pub write_batch_size: usize,
    pub num_indexed_cols: i32,
    pub stats_columns: Option<Vec<String>>,
    pub iceberg_schema: Arc<IcebergSchema>,
    pub partition_spec: UnboundPartitionSpec,
}

impl WriterConfig {
    pub fn new(
        table_schema: ArrowSchemaRef,
        partition_columns: Vec<String>,
        writer_properties: Option<WriterProperties>,
        target_file_size: u64,
        write_batch_size: usize,
        num_indexed_cols: i32,
        stats_columns: Option<Vec<String>>,
        iceberg_schema: Arc<IcebergSchema>,
        partition_spec: UnboundPartitionSpec,
    ) -> Self {
        let writer_properties = writer_properties.unwrap_or_default();
        Self {
            table_schema,
            partition_columns,
            writer_properties,
            target_file_size,
            write_batch_size,
            num_indexed_cols,
            stats_columns,
            iceberg_schema,
            partition_spec,
        }
    }
}
