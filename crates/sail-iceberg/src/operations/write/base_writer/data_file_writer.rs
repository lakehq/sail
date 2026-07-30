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

use std::collections::HashMap;

use crate::operations::write::WriteOutcome;
use crate::operations::write::arrow_parquet::ParquetFileMeta;
use crate::spec::types::values::Literal;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Datum};

pub struct DataFileWriter {
    pub partition_spec_id: i32,
    pub file_path: String,
    pub partition_values: Vec<Option<Literal>>,
}

impl DataFileWriter {
    pub fn new(
        partition_spec_id: i32,
        file_path: String,
        partition_values: Vec<Option<Literal>>,
    ) -> Self {
        Self {
            partition_spec_id,
            file_path,
            partition_values,
        }
    }

    pub fn finish(self, meta: ParquetFileMeta) -> Result<WriteOutcome, String> {
        let (
            column_sizes,
            value_counts,
            null_value_counts,
            lower_bounds,
            upper_bounds,
            split_offsets,
        ) = aggregate_from_parquet_metadata(&meta.parquet_metadata)?;

        let data_file = DataFile {
            content: DataContentType::Data,
            file_path: self.file_path,
            file_format: DataFileFormat::Parquet,
            partition: self.partition_values,
            record_count: meta.num_rows,
            file_size_in_bytes: meta.file_size,
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts: Default::default(),
            lower_bounds,
            upper_bounds,
            raw_lower_bounds: Default::default(),
            raw_upper_bounds: Default::default(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets,
            equality_ids: Vec::new(),
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: self.partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        Ok(WriteOutcome { data_file })
    }
}

type AggregatedMetadata = (
    HashMap<i32, u64>,
    HashMap<i32, u64>,
    HashMap<i32, u64>,
    HashMap<i32, Datum>,
    HashMap<i32, Datum>,
    Vec<i64>,
);

fn aggregate_from_parquet_metadata(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
) -> Result<AggregatedMetadata, String> {
    let row_groups = parquet_meta.row_groups();

    let mut col_sizes: HashMap<i32, u64> = HashMap::new();
    let mut val_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_counts: HashMap<i32, u64> = HashMap::new();
    let lower_bounds: HashMap<i32, Datum> = HashMap::new();
    let upper_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut split_offsets: Vec<i64> = Vec::new();

    for rg in row_groups {
        if let Some(off) = rg.file_offset() {
            split_offsets.push(off);
        }
        for c in rg.columns() {
            let leaf_info = c.column_descr().self_type().get_basic_info();
            if !leaf_info.has_id() {
                continue;
            }
            let field_id = leaf_info.id();
            *col_sizes.entry(field_id).or_insert(0) += c.compressed_size() as u64;
            *val_counts.entry(field_id).or_insert(0) += c.num_values() as u64;
            if let Some(stats) = c.statistics()
                && let Some(n) = stats.null_count_opt()
            {
                *null_counts.entry(field_id).or_insert(0) += n;
            }
            // Do not attempt to parse typed bounds here; leave empty per-field for now.
        }
    }

    Ok((
        col_sizes,
        val_counts,
        null_counts,
        lower_bounds,
        upper_bounds,
        split_offsets,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::operations::write::arrow_parquet::ArrowParquetWriter;

    fn field_with_id(name: &str, data_type: DataType, id: i32) -> Arc<Field> {
        Arc::new(
            Field::new(name, data_type, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
        )
    }

    #[test]
    fn idless_nested_leaves_do_not_inherit_root_metrics() -> Result<(), String> {
        futures::executor::block_on(async {
            let id_field = field_with_id("id", DataType::Int64, 1);
            let nested_fields = vec![
                Arc::new(Field::new("left", DataType::Int64, true)),
                Arc::new(Field::new("right", DataType::Int64, true)),
            ];
            let root_field =
                field_with_id("root", DataType::Struct(nested_fields.clone().into()), 10);
            let schema = Arc::new(Schema::new(vec![id_field, root_field]));
            let nested = Arc::new(StructArray::new(
                nested_fields.into(),
                vec![
                    Arc::new(Int64Array::from(vec![Some(10), Some(20)])),
                    Arc::new(Int64Array::from(vec![Some(30), Some(40)])),
                ],
                None,
            )) as ArrayRef;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)])), nested],
            )
            .map_err(|error| error.to_string())?;

            let mut writer =
                ArrowParquetWriter::try_new(&schema, WriterProperties::builder().build())?;
            writer.write_batch(&batch).await?;
            let (_, metadata) = writer.close().await?;
            let (column_sizes, value_counts, null_counts, _, _, _) =
                aggregate_from_parquet_metadata(&metadata.parquet_metadata)?;

            assert_eq!(column_sizes.keys().copied().collect::<Vec<_>>(), vec![1]);
            assert_eq!(value_counts, HashMap::from([(1, 2)]));
            assert_eq!(null_counts, HashMap::from([(1, 0)]));
            Ok(())
        })
    }
}
