use crate::spec::{DataContentType, DataFile, DataFileFormat};
use crate::writer::WriteOutcome;

pub struct DataFileWriter {
    pub partition_spec_id: i32,
    pub file_path: String,
}

impl DataFileWriter {
    pub fn new(partition_spec_id: i32, file_path: String) -> Self {
        Self {
            partition_spec_id,
            file_path,
        }
    }

    pub fn finish(self) -> Result<WriteOutcome, String> {
        let data_file = DataFile {
            content: DataContentType::Data,
            file_path: self.file_path,
            file_format: DataFileFormat::Parquet,
            partition: Vec::new(),
            record_count: 0,
            file_size_in_bytes: 0,
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
