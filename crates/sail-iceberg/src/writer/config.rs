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
