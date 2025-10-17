use std::collections::HashMap;

use parquet::file::metadata::RowGroupMetaData;
use parquet::schema::types::SchemaDescriptor;

use crate::spec::types::values::Literal;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Datum};
use crate::writer::arrow_parquet::ParquetFileMeta;
use crate::writer::WriteOutcome;

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
    thrift_meta: &parquet::format::FileMetaData,
) -> Result<AggregatedMetadata, String> {
    let schema_ptr = parquet::schema::types::from_thrift(thrift_meta.schema.as_slice())
        .map_err(|e| format!("Failed to parse schema from thrift: {e}"))?;
    let schema_desc = std::sync::Arc::new(SchemaDescriptor::new(schema_ptr));

    let row_groups: Vec<RowGroupMetaData> = thrift_meta
        .row_groups
        .iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_desc.clone(), rg.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse row group metadata: {e}"))?;

    let mut col_sizes: HashMap<i32, u64> = HashMap::new();
    let mut val_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_counts: HashMap<i32, u64> = HashMap::new();
    let lower_bounds: HashMap<i32, Datum> = HashMap::new();
    let upper_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut split_offsets: Vec<i64> = Vec::new();

    for rg in &row_groups {
        if let Some(off) = rg.file_offset() {
            split_offsets.push(off);
        }
        for c in rg.columns() {
            let _path = c.column_descr().path().string();
            // Heuristic: use leaf id if present; otherwise fall back to column index as field id
            let field_id = c.column_descr().self_type().get_basic_info().id();
            *col_sizes.entry(field_id).or_insert(0) += c.compressed_size() as u64;
            *val_counts.entry(field_id).or_insert(0) += c.num_values() as u64;
            if let Some(stats) = c.statistics() {
                if let Some(n) = stats.null_count_opt() {
                    *null_counts.entry(field_id).or_insert(0) += n;
                }
                // Do not attempt to parse typed bounds here; leave empty per-field for now
                let _ = _path; // silence unused
            }
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
