// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use parquet::file::properties::WriterProperties;
pub use sail_common_datafusion::variant::VariantShreddingConfig;

use super::metrics::MetricsConfig;
use crate::spec::Schema as IcebergSchema;
use crate::spec::partition::UnboundPartitionSpec;

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub table_schema: ArrowSchemaRef,
    pub writer_properties: WriterProperties,
    pub target_file_size_bytes: u64,
    pub sort_order_id: Option<i32>,
    pub iceberg_schema: Arc<IcebergSchema>,
    pub partition_spec: UnboundPartitionSpec,
    pub variant_shredding: VariantShreddingConfig,
    pub metrics: MetricsConfig,
}
