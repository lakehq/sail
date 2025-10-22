// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest/writer.rs

use std::sync::Arc;

use apache_avro::{Schema as AvroSchema, Writer as AvroWriter};
use serde::Serialize;

use super::{
    DataFile, Manifest, ManifestEntry, ManifestEntryRef, ManifestMetadata, ManifestStatus,
};
use crate::spec::manifest_list::{ManifestContentType, ManifestFile};
use crate::spec::DataFileFormat;

#[derive(Debug, Clone)]
pub struct ManifestWriterBuilder {
    snapshot_id: Option<i64>,
    key_metadata: Option<Vec<u8>>,
    metadata: ManifestMetadata,
}

impl ManifestWriterBuilder {
    pub fn new(
        snapshot_id: Option<i64>,
        key_metadata: Option<Vec<u8>>,
        metadata: ManifestMetadata,
    ) -> Self {
        Self {
            snapshot_id,
            key_metadata,
            metadata,
        }
    }

    pub fn build(self) -> ManifestWriter {
        ManifestWriter::new(self.snapshot_id, self.key_metadata, self.metadata)
    }
}

#[derive(Debug, Clone)]
pub struct ManifestWriter {
    snapshot_id: Option<i64>,
    key_metadata: Option<Vec<u8>>,
    metadata: ManifestMetadata,
    entries: Vec<ManifestEntryRef>,
}

impl ManifestWriter {
    pub fn new(
        snapshot_id: Option<i64>,
        key_metadata: Option<Vec<u8>>,
        metadata: ManifestMetadata,
    ) -> Self {
        Self {
            snapshot_id,
            key_metadata,
            metadata,
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, file: DataFile) {
        let entry = ManifestEntry::new(ManifestStatus::Added, self.snapshot_id, None, None, file);
        self.entries.push(Arc::new(entry));
    }

    pub fn finish(self) -> Manifest {
        Manifest::new(
            self.metadata,
            self.entries.into_iter().map(|e| (*e).clone()).collect(),
        )
    }

    pub fn into_manifest_file(
        self,
        manifest_path: String,
        sequence_number: i64,
        snapshot_id: i64,
    ) -> ManifestFile {
        let added = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, ManifestStatus::Added))
            .count() as i32;
        let existing = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, ManifestStatus::Existing))
            .count() as i32;
        let deleted = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, ManifestStatus::Deleted))
            .count() as i32;
        ManifestFile {
            manifest_path,
            manifest_length: 0,
            partition_spec_id: self.metadata.partition_spec.spec_id(),
            content: ManifestContentType::Data,
            sequence_number,
            min_sequence_number: sequence_number,
            added_snapshot_id: snapshot_id,
            added_files_count: Some(added),
            existing_files_count: Some(existing),
            deleted_files_count: Some(deleted),
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: None,
            key_metadata: self.key_metadata,
        }
    }

    /// Encode this manifest's entries into Avro bytes using a v2 schema subset.
    /// TODO: Support full v2 features (metrics maps, structured partition encoding) instead of omitting them.
    pub fn to_avro_bytes_v2(&self) -> Result<Vec<u8>, String> {
        // TODO: Replace this reduced v2 entry schema with the full specification, including metrics maps
        // and proper partition struct encoding.
        let schema_json = r#"
        {
          "type": "record",
          "name": "manifest_entry",
          "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null","long"], "default": null, "field-id": 1},
            {"name": "sequence_number", "type": ["null","long"], "default": null, "field-id": 3},
            {"name": "file_sequence_number", "type": ["null","long"], "default": null, "field-id": 4},
            {"name": "data_file", "type": {
              "type": "record",
              "name": "data_file",
              "fields": [
                {"name": "content", "type": "int", "field-id": 134},
                {"name": "file_path", "type": "string", "field-id": 100},
                {"name": "file_format", "type": "string", "field-id": 101},
                {"name": "partition", "type": ["null", {"type": "record", "name": "r102", "fields": []}], "default": null, "field-id": 102},
                {"name": "record_count", "type": "long", "field-id": 103},
                {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                {"name": "key_metadata", "type": ["null","bytes"], "default": null, "field-id": 131},
                {"name": "split_offsets", "type": ["null", {"type": "array", "element-id": 133, "items": "long"}], "default": null, "field-id": 132},
                {"name": "equality_ids", "type": ["null", {"type": "array", "element-id": 136, "items": "long"}], "default": null, "field-id": 135},
                {"name": "sort_order_id", "type": ["null","int"], "default": null, "field-id": 140}
              ]
            }, "field-id": 2}
          ]
        }
        "#;

        #[derive(Serialize)]
        struct AvroDataFile<'a> {
            content: i32,
            file_path: &'a str,
            file_format: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            partition: Option<String>,
            record_count: i64,
            file_size_in_bytes: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            key_metadata: Option<&'a Vec<u8>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            split_offsets: Option<&'a Vec<i64>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            equality_ids: Option<Vec<i32>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            sort_order_id: Option<i32>,
        }

        #[derive(Serialize)]
        struct AvroEntry<'a> {
            status: i32,
            #[serde(skip_serializing_if = "Option::is_none")]
            snapshot_id: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            sequence_number: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            file_sequence_number: Option<i64>,
            data_file: AvroDataFile<'a>,
        }

        let avro_schema = AvroSchema::parse_str(schema_json)
            .map_err(|e| format!("Avro schema parse error: {e}"))?;
        let mut writer = AvroWriter::new(&avro_schema, Vec::new());

        // Add user metadata per Iceberg spec
        let schema_json = serde_json::to_vec(&self.metadata.schema)
            .map_err(|e| format!("Fail to serialize table schema: {e}"))?;
        writer
            .add_user_metadata("schema".to_string(), schema_json)
            .map_err(|e| format!("Avro add_user_metadata error: {e}"))?;
        writer
            .add_user_metadata("schema-id".to_string(), self.metadata.schema_id.to_string())
            .map_err(|e| format!("Avro add_user_metadata error: {e}"))?;
        let part_fields = serde_json::to_vec(&self.metadata.partition_spec.fields())
            .map_err(|e| format!("Fail to serialize partition spec: {e}"))?;
        writer
            .add_user_metadata("partition-spec".to_string(), part_fields)
            .map_err(|e| format!("Avro add_user_metadata error: {e}"))?;
        writer
            .add_user_metadata(
                "partition-spec-id".to_string(),
                self.metadata.partition_spec.spec_id().to_string(),
            )
            .map_err(|e| format!("Avro add_user_metadata error: {e}"))?;
        writer
            .add_user_metadata(
                "format-version".to_string(),
                (self.metadata.format_version as u8).to_string(),
            )
            .map_err(|e| format!("Avro add_user_metadata error: {e}"))?;
        if self.metadata.format_version as u8 == 2 {
            let content_str = match self.metadata.content {
                ManifestContentType::Data => "data",
                ManifestContentType::Deletes => "deletes",
            };
            writer
                .add_user_metadata("content".to_string(), content_str)
                .map_err(|e| format!("Avro add_user_metadata error: {e}"))?;
        }

        for e in &self.entries {
            let status = match e.status {
                ManifestStatus::Added => 1,
                ManifestStatus::Deleted => 2,
                ManifestStatus::Existing => 0,
            };
            let df = &e.data_file;
            let content = match df.content {
                super::DataContentType::Data => 0,
                super::DataContentType::PositionDeletes => 1,
                super::DataContentType::EqualityDeletes => 2,
            };
            let file_format = match df.file_format {
                DataFileFormat::Parquet => "PARQUET",
                DataFileFormat::Avro => "AVRO",
                DataFileFormat::Orc => "ORC",
                DataFileFormat::Puffin => "PUFFIN",
            };
            let partition_str: Option<String> = None; // minimal encoding
            let equality_ids = if df.equality_ids.is_empty() {
                None
            } else {
                Some(df.equality_ids.clone())
            };
            let avro_df = AvroDataFile {
                content,
                file_path: &df.file_path,
                file_format,
                partition: partition_str,
                record_count: df.record_count as i64,
                file_size_in_bytes: df.file_size_in_bytes as i64,
                key_metadata: df.key_metadata.as_ref(),
                split_offsets: if df.split_offsets.is_empty() {
                    None
                } else {
                    Some(&df.split_offsets)
                },
                equality_ids,
                sort_order_id: df.sort_order_id,
            };
            let entry = AvroEntry {
                status,
                snapshot_id: e.snapshot_id,
                sequence_number: e.sequence_number,
                file_sequence_number: e.file_sequence_number,
                data_file: avro_df,
            };
            let value =
                apache_avro::to_value(entry).map_err(|e| format!("Avro to_value error: {e}"))?;
            let value = value
                .resolve(&avro_schema)
                .map_err(|e| format!("Avro resolve error: {e}"))?;
            writer
                .append(value)
                .map_err(|e| format!("Avro append error: {e}"))?;
        }

        writer
            .into_inner()
            .map_err(|e| format!("Avro writer finalize error: {e}"))
    }
}
