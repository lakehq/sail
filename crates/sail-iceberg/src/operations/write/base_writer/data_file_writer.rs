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

use std::collections::{HashMap, HashSet};

use ordered_float::OrderedFloat;
use parquet::file::statistics::{Statistics, ValueStatistics};

use crate::operations::write::WriteOutcome;
use crate::operations::write::arrow_parquet::ParquetFileMeta;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{DataContentType, DataFile, DataFileFormat, Datum, Schema};

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

    /// Finish a delete-file write without collecting column bounds.
    pub fn finish_without_bounds(self, meta: ParquetFileMeta) -> Result<WriteOutcome, String> {
        let empty_schema = Schema::builder().with_schema_id(0).build()?;
        self.finish_with_schema(meta, &empty_schema)
    }

    pub fn finish_with_schema(
        self,
        meta: ParquetFileMeta,
        iceberg_schema: &Schema,
    ) -> Result<WriteOutcome, String> {
        let (
            column_sizes,
            value_counts,
            null_value_counts,
            lower_bounds,
            upper_bounds,
            split_offsets,
        ) = aggregate_from_parquet_metadata(&meta.parquet_metadata, iceberg_schema)?;

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

fn typed_statistics_bounds<T>(
    statistics: &ValueStatistics<T>,
    iceberg_type: &PrimitiveType,
    to_literal: impl Fn(&T) -> Option<PrimitiveLiteral>,
) -> (Option<Datum>, Option<Datum>) {
    let lower = if statistics.min_is_exact() {
        statistics
            .min_opt()
            .and_then(&to_literal)
            .map(|literal| Datum::new(iceberg_type.clone(), literal))
    } else {
        None
    };
    let upper = if statistics.max_is_exact() {
        statistics
            .max_opt()
            .and_then(to_literal)
            .map(|literal| Datum::new(iceberg_type.clone(), literal))
    } else {
        None
    };
    (lower, upper)
}

fn signed_bytes_to_i128(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > size_of::<i128>() {
        return None;
    }
    let fill = if bytes[0] & 0x80 == 0 { 0 } else { u8::MAX };
    let mut value = [fill; size_of::<i128>()];
    let offset = value.len() - bytes.len();
    value[offset..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(value))
}

fn bytes_to_literal(bytes: &[u8], iceberg_type: &PrimitiveType) -> Option<PrimitiveLiteral> {
    match iceberg_type {
        PrimitiveType::String => std::str::from_utf8(bytes)
            .ok()
            .map(|value| PrimitiveLiteral::String(value.to_string())),
        PrimitiveType::Uuid => bytes
            .try_into()
            .ok()
            .map(u128::from_be_bytes)
            .map(PrimitiveLiteral::UInt128),
        PrimitiveType::Fixed(length) if bytes.len() == *length as usize => {
            Some(PrimitiveLiteral::Binary(bytes.to_vec()))
        }
        PrimitiveType::Binary => Some(PrimitiveLiteral::Binary(bytes.to_vec())),
        PrimitiveType::Decimal { .. } => signed_bytes_to_i128(bytes).map(PrimitiveLiteral::Int128),
        _ => None,
    }
}

fn parquet_statistics_bounds(
    statistics: &Statistics,
    iceberg_type: &PrimitiveType,
) -> (Option<Datum>, Option<Datum>) {
    match (statistics, iceberg_type) {
        (Statistics::Boolean(statistics), PrimitiveType::Boolean) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                Some(PrimitiveLiteral::Boolean(*value))
            })
        }
        (Statistics::Int32(statistics), PrimitiveType::Int | PrimitiveType::Date) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                Some(PrimitiveLiteral::Int(*value))
            })
        }
        (Statistics::Int32(statistics), PrimitiveType::Decimal { .. }) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                Some(PrimitiveLiteral::Int128(i128::from(*value)))
            })
        }
        (
            Statistics::Int64(statistics),
            PrimitiveType::Long
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs,
        ) => typed_statistics_bounds(statistics, iceberg_type, |value| {
            Some(PrimitiveLiteral::Long(*value))
        }),
        (Statistics::Int64(statistics), PrimitiveType::Decimal { .. }) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                Some(PrimitiveLiteral::Int128(i128::from(*value)))
            })
        }
        (Statistics::Float(statistics), PrimitiveType::Float) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                Some(PrimitiveLiteral::Float(OrderedFloat(*value)))
            })
        }
        (Statistics::Double(statistics), PrimitiveType::Double) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                Some(PrimitiveLiteral::Double(OrderedFloat(*value)))
            })
        }
        (Statistics::ByteArray(statistics), _) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                bytes_to_literal(value.data(), iceberg_type)
            })
        }
        (Statistics::FixedLenByteArray(statistics), _) => {
            typed_statistics_bounds(statistics, iceberg_type, |value| {
                bytes_to_literal(value.data(), iceberg_type)
            })
        }
        _ => (None, None),
    }
}

fn update_bound(
    bounds: &mut HashMap<i32, Datum>,
    unknown: &mut HashSet<i32>,
    field_id: i32,
    value: Option<Datum>,
    lower: bool,
) {
    if unknown.contains(&field_id) {
        return;
    }
    let Some(value) = value else {
        bounds.remove(&field_id);
        unknown.insert(field_id);
        return;
    };
    bounds
        .entry(field_id)
        .and_modify(|current| {
            if (lower && value.literal < current.literal)
                || (!lower && value.literal > current.literal)
            {
                *current = value.clone();
            }
        })
        .or_insert(value);
}

fn aggregate_from_parquet_metadata(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
    iceberg_schema: &Schema,
) -> Result<AggregatedMetadata, String> {
    let row_groups = parquet_meta.row_groups();
    let schema_descr = parquet_meta.file_metadata().schema_descr();

    let mut col_sizes: HashMap<i32, u64> = HashMap::new();
    let mut val_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_counts_unknown: HashSet<i32> = HashSet::new();
    let mut lower_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut lower_bounds_unknown: HashSet<i32> = HashSet::new();
    let mut upper_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut upper_bounds_unknown: HashSet<i32> = HashSet::new();
    let mut split_offsets: Vec<i64> = Vec::new();

    for rg in row_groups {
        if let Some(off) = rg.file_offset() {
            split_offsets.push(off);
        }
        for (column_index, column) in rg.columns().iter().enumerate() {
            let leaf_info = column.column_descr().self_type().get_basic_info();
            let Some(field_id) = (if leaf_info.has_id() {
                Some(leaf_info.id())
            } else {
                let root_info = schema_descr.get_column_root(column_index).get_basic_info();
                root_info.has_id().then(|| root_info.id())
            }) else {
                continue;
            };
            *col_sizes.entry(field_id).or_insert(0) += column.compressed_size() as u64;
            *val_counts.entry(field_id).or_insert(0) += column.num_values() as u64;

            let statistics = column.statistics();
            let null_count = statistics.and_then(Statistics::null_count_opt);
            if !null_counts_unknown.contains(&field_id) {
                if let Some(null_count) = null_count {
                    *null_counts.entry(field_id).or_insert(0) += null_count;
                } else {
                    // A missing row-group statistic makes the file-level count unknown.
                    null_counts.remove(&field_id);
                    null_counts_unknown.insert(field_id);
                }
            }

            let Some(primitive_type) = iceberg_schema.field_by_id(field_id).and_then(|field| {
                match field.field_type.as_ref() {
                    Type::Primitive(primitive_type) => Some(primitive_type),
                    Type::Struct(_) | Type::List(_) | Type::Map(_) => None,
                }
            }) else {
                continue;
            };
            let (lower, upper) = statistics
                .map(|statistics| parquet_statistics_bounds(statistics, primitive_type))
                .unwrap_or((None, None));
            update_bound(
                &mut lower_bounds,
                &mut lower_bounds_unknown,
                field_id,
                lower,
                true,
            );
            update_bound(
                &mut upper_bounds,
                &mut upper_bounds_unknown,
                field_id,
                upper,
                false,
            );
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

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::{SessionContext, col, lit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::statistics::Statistics;

    use super::*;
    use crate::operations::write::arrow_parquet::ArrowParquetWriter;
    use crate::spec::types::values::PrimitiveLiteral;
    use crate::spec::types::{NestedField, PrimitiveType};

    #[test]
    fn parquet_integer_statistics_produce_iceberg_bounds() {
        let statistics = Statistics::int32(Some(1), Some(3), None, Some(0), false);

        let (lower, upper) = parquet_statistics_bounds(&statistics, &PrimitiveType::Int);

        assert_eq!(
            lower,
            Some(Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(1)))
        );
        assert_eq!(
            upper,
            Some(Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(3)))
        );
    }

    #[test]
    fn data_file_writer_preserves_parquet_integer_bounds() -> Result<(), String> {
        futures::executor::block_on(async {
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )])),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .map_err(|error| error.to_string())?;
            let mut parquet_writer = ArrowParquetWriter::try_new(
                arrow_schema.as_ref(),
                WriterProperties::builder().build(),
            )?;
            parquet_writer.write_batch(&batch).await?;
            let (_, metadata) = parquet_writer.close().await?;
            let iceberg_schema = Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()?;

            let outcome = DataFileWriter::new(0, "data.parquet".to_string(), vec![])
                .finish_with_schema(metadata, &iceberg_schema)?;

            assert_eq!(
                outcome.data_file.lower_bounds.get(&1),
                Some(&Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(1)))
            );
            assert_eq!(
                outcome.data_file.upper_bounds.get(&1),
                Some(&Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(3)))
            );

            let session = SessionContext::new();
            let (kept, mask) = crate::datasource::pruning::prune_files(
                &session.state(),
                &[col("id").gt(lit(10i32))],
                None,
                arrow_schema,
                vec![outcome.data_file],
                &iceberg_schema,
            )
            .map_err(|error| error.to_string())?;
            assert!(kept.is_empty());
            assert_eq!(mask, Some(vec![false]));
            Ok(())
        })
    }

    #[test]
    fn aggregate_statistics_are_absent_when_any_file_metric_is_missing() {
        let mut bounds = HashMap::new();
        let mut unknown = HashSet::new();
        update_bound(
            &mut bounds,
            &mut unknown,
            1,
            Some(Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(1))),
            true,
        );
        update_bound(&mut bounds, &mut unknown, 1, None, true);
        update_bound(
            &mut bounds,
            &mut unknown,
            1,
            Some(Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(0))),
            true,
        );
        assert!(!bounds.contains_key(&1));
        assert!(unknown.contains(&1));
    }

    #[test]
    fn statistics_drop_bounds_that_do_not_match_current_arrow_type() {
        let statistics = Statistics::int32(Some(1), Some(3), None, Some(0), false);

        let (lower, upper) = parquet_statistics_bounds(&statistics, &PrimitiveType::String);

        assert_eq!(lower, None);
        assert_eq!(upper, None);
    }

    #[test]
    fn parquet_string_statistics_produce_iceberg_bounds() {
        let statistics = Statistics::byte_array(
            Some(ByteArray::from("alpha")),
            Some(ByteArray::from("omega")),
            None,
            Some(0),
            false,
        );

        let (lower, upper) = parquet_statistics_bounds(&statistics, &PrimitiveType::String);

        assert_eq!(
            lower,
            Some(Datum::new(
                PrimitiveType::String,
                PrimitiveLiteral::String("alpha".to_string())
            ))
        );
        assert_eq!(
            upper,
            Some(Datum::new(
                PrimitiveType::String,
                PrimitiveLiteral::String("omega".to_string())
            ))
        );
    }
}
