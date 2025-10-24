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
