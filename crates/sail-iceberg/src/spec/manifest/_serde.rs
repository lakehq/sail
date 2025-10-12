use apache_avro::types::Value as AvroValue;
use serde::{Deserialize, Serialize};

use super::{DataContentType, DataFile, DataFileFormat};
use crate::spec::Schema;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ManifestEntryAvro {
    #[serde(rename = "status")]
    pub status: i32,
    #[serde(rename = "snapshot_id")]
    pub snapshot_id: Option<i64>,
    #[serde(rename = "sequence_number")]
    pub sequence_number: Option<i64>,
    #[serde(rename = "file_sequence_number")]
    pub file_sequence_number: Option<i64>,
    #[serde(rename = "data_file")]
    pub data_file: DataFileAvro,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct DataFileAvro {
    #[serde(rename = "content", default)]
    pub content: i32,
    #[serde(rename = "file_path")]
    pub file_path: String,
    #[serde(rename = "file_format")]
    pub file_format: String,
    #[serde(rename = "partition")]
    pub partition: serde_json::Value,
    #[serde(rename = "record_count")]
    pub record_count: i64,
    #[serde(rename = "file_size_in_bytes")]
    pub file_size_in_bytes: i64,
    #[serde(skip)]
    pub column_sizes: Option<AvroValue>,
    #[serde(skip)]
    pub value_counts: Option<AvroValue>,
    #[serde(skip)]
    pub null_value_counts: Option<AvroValue>,
    #[serde(skip)]
    pub nan_value_counts: Option<AvroValue>,
    #[serde(skip)]
    pub lower_bounds: Option<AvroValue>,
    #[serde(skip)]
    pub upper_bounds: Option<AvroValue>,
    #[serde(rename = "key_metadata")]
    pub key_metadata: Option<Vec<u8>>,
    #[serde(rename = "split_offsets")]
    pub split_offsets: Option<Vec<i64>>,
    #[serde(rename = "equality_ids")]
    pub equality_ids: Option<Vec<i32>>,
    #[serde(rename = "sort_order_id")]
    pub sort_order_id: Option<i32>,
}

impl DataFileAvro {
    pub fn into_data_file(
        self,
        schema: &Schema,
        _partition_type_len: i32,
        partition_spec_id: i32,
    ) -> DataFile {
        let content = match self.content {
            0 => DataContentType::Data,
            1 => DataContentType::PositionDeletes,
            2 => DataContentType::EqualityDeletes,
            _ => DataContentType::Data,
        };

        let file_format = match self.file_format.to_uppercase().as_str() {
            "PARQUET" => DataFileFormat::Parquet,
            "AVRO" => DataFileFormat::Avro,
            "ORC" => DataFileFormat::Orc,
            _ => DataFileFormat::Parquet,
        };

        let partition = super::super::manifest::parse_partition_values(Some(&self.partition));

        let column_sizes = super::super::manifest::parse_i64_map_from_avro(&self.column_sizes)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();
        let value_counts = super::super::manifest::parse_i64_map_from_avro(&self.value_counts)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();
        let null_value_counts =
            super::super::manifest::parse_i64_map_from_avro(&self.null_value_counts)
                .into_iter()
                .map(|(k, v)| (k, v as u64))
                .collect();
        let nan_value_counts =
            super::super::manifest::parse_i64_map_from_avro(&self.nan_value_counts)
                .into_iter()
                .map(|(k, v)| (k, v as u64))
                .collect();

        let lower_bounds_raw =
            super::super::manifest::parse_bytes_map_from_avro(&self.lower_bounds);
        let upper_bounds_raw =
            super::super::manifest::parse_bytes_map_from_avro(&self.upper_bounds);
        let lower_bounds =
            super::super::manifest::parse_bounds_from_binary(lower_bounds_raw.as_ref(), schema);
        let upper_bounds =
            super::super::manifest::parse_bounds_from_binary(upper_bounds_raw.as_ref(), schema);

        DataFile {
            content,
            file_path: self.file_path,
            file_format,
            partition,
            record_count: self.record_count as u64,
            file_size_in_bytes: self.file_size_in_bytes as u64,
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts,
            lower_bounds,
            upper_bounds,
            block_size_in_bytes: None,
            key_metadata: self.key_metadata,
            split_offsets: self.split_offsets.unwrap_or_default(),
            equality_ids: self.equality_ids.unwrap_or_default().into_iter().collect(),
            sort_order_id: self.sort_order_id,
            first_row_id: None,
            partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }
}
