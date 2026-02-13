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

use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use uuid::Uuid;

use crate::conversion::scalar::ScalarExt as _;

/// Delta part file naming, kept compatible with the existing implementation.
///
/// Example:
/// `part-00001-<uuid>-c000.snappy.parquet`
#[derive(Debug)]
pub struct PartFileNamer {
    writer_id: Uuid,
    part_counter: usize,
}

impl PartFileNamer {
    pub fn new() -> Self {
        Self {
            writer_id: Uuid::new_v4(),
            part_counter: 0,
        }
    }

    pub fn next_file_name(&mut self, writer_properties: &WriterProperties) -> String {
        self.part_counter += 1;

        let column_path = ColumnPath::new(Vec::new());
        let compression_suffix = match writer_properties.compression(&column_path) {
            Compression::UNCOMPRESSED => "",
            Compression::SNAPPY => ".snappy",
            Compression::GZIP(_) => ".gz",
            Compression::LZO => ".lzo",
            Compression::BROTLI(_) => ".br",
            Compression::LZ4 => ".lz4",
            Compression::ZSTD(_) => ".zstd",
            Compression::LZ4_RAW => ".lz4raw",
        };

        format!(
            "part-{:05}-{}-c000{}.parquet",
            self.part_counter, self.writer_id, compression_suffix
        )
    }
}

impl Default for PartFileNamer {
    fn default() -> Self {
        Self::new()
    }
}

/// Build `(relative_path, full_path)` under `table_path` with hive partition segments.
pub fn build_data_path(
    table_path: &Path,
    partition_segments: &[String],
    file_name: &str,
) -> (String, Path) {
    let mut full_path = table_path.clone();
    for segment in partition_segments {
        full_path = full_path.child(segment.as_str());
    }
    full_path = full_path.child(file_name);

    let relative_path = if partition_segments.is_empty() {
        file_name.to_string()
    } else {
        format!("{}/{}", partition_segments.join("/"), file_name)
    };

    (relative_path, full_path)
}

/// Trait for creating hive partition paths from partition values.
///
/// This uses `ScalarExt::serialize_encoded`, and therefore is the source of truth for strict
/// compatibility (`__HIVE_DEFAULT_PARTITION__` for null and RFC3986 percent-encoding).
pub trait PartitionsExt {
    fn hive_partition_path(&self) -> String;
    fn hive_partition_segments(&self) -> Vec<String>;
}

impl PartitionsExt for IndexMap<String, Scalar> {
    fn hive_partition_path(&self) -> String {
        self.hive_partition_segments().join("/")
    }

    fn hive_partition_segments(&self) -> Vec<String> {
        if self.is_empty() {
            return vec![];
        }

        self.iter()
            .map(|(k, v)| {
                let value_str = v.serialize_encoded();
                format!("{k}={value_str}")
            })
            .collect()
    }
}
