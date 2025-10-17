use crate::spec::{DataContentType, DataFile, DataFileFormat};
use crate::spec::types::values::Literal;
use crate::writer::WriteOutcome;
use crate::writer::arrow_parquet::ParquetFileMeta;

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
        let data_file = DataFile {
            content: DataContentType::Data,
            file_path: self.file_path,
            file_format: DataFileFormat::Parquet,
            partition: self.partition_values,
            record_count: meta.num_rows,
            file_size_in_bytes: meta.file_size,
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: Vec::new(),
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
