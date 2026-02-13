// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// How to split / group incoming record batches by partition key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitioningMode {
    /// Keep current contiguous-split behavior (fast, but can produce many slices on unsorted input).
    Contiguous,
    /// Group within each batch by partition key using take-indices (more work per batch, fewer slices).
    Hash,
    /// Choose a strategy automatically.
    Auto,
}

/// Configuration for Delta parquet writing.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Schema of the delta table (writer schema may be a physical schema).
    pub table_schema: ArrowSchemaRef,
    /// Logical column names the table is partitioned by (used for metadata/actions).
    pub partition_columns: Vec<String>,
    /// Physical column names for partition columns in the input batch/schema.
    pub physical_partition_columns: Vec<String>,
    /// Properties passed to underlying parquet writer.
    pub writer_properties: WriterProperties,
    /// Target file size (bytes) at which we roll to a new file.
    pub target_file_size: u64,
    /// Row chunks passed to parquet writer.
    pub write_batch_size: usize,
    /// Number of indexed columns for statistics.
    pub num_indexed_cols: i32,
    /// Specific columns to collect stats from.
    pub stats_columns: Option<Vec<String>>,
    /// How to group per-batch into partitioned batches.
    pub partitioning_mode: PartitioningMode,
    /// Buffer size for `object_store::buffered::BufWriter`.
    pub objectstore_writer_buffer_size: usize,
    /// Whether to skip embedding Arrow schema metadata in parquet.
    pub skip_arrow_metadata: bool,
}

impl WriterConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_schema: ArrowSchemaRef,
        partition_columns: Vec<String>,
        physical_partition_columns: Vec<String>,
        writer_properties: Option<WriterProperties>,
        target_file_size: u64,
        write_batch_size: usize,
        num_indexed_cols: i32,
        stats_columns: Option<Vec<String>>,
    ) -> Self {
        let writer_properties = writer_properties.unwrap_or_else(|| {
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build()
        });

        Self {
            table_schema,
            partition_columns,
            physical_partition_columns,
            writer_properties,
            target_file_size,
            write_batch_size,
            num_indexed_cols,
            stats_columns,
            partitioning_mode: PartitioningMode::Auto,
            // Match object_store::buffered::BufWriter default (10 MiB) unless overridden by caller.
            objectstore_writer_buffer_size: 10 * 1024 * 1024,
            skip_arrow_metadata: false,
        }
    }

    /// Schema of files written to disk (without partition columns).
    pub fn file_schema(&self) -> ArrowSchemaRef {
        arrow_schema_without_partitions(&self.table_schema, &self.physical_partition_columns)
    }
}

/// Create Arrow schema without partition columns.
pub fn arrow_schema_without_partitions(
    arrow_schema: &ArrowSchemaRef,
    partition_columns: &[String],
) -> ArrowSchemaRef {
    Arc::new(ArrowSchema::new(
        arrow_schema
            .fields()
            .iter()
            .filter(|f| !partition_columns.contains(f.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

