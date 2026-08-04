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

use apache_avro::{Writer as AvroWriter, to_value};

use super::{
    DataFile, Manifest, ManifestEntry, ManifestEntryRef, ManifestMetadata, ManifestStatus,
};
use crate::spec::FormatVersion;
use crate::spec::manifest_list::{ManifestContentType, ManifestFile};

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

    pub fn add_existing_entry(&mut self, mut entry: ManifestEntry) -> Result<(), String> {
        entry.status = ManifestStatus::Existing;
        self.add_rewritten_entry(entry)
    }

    pub fn add_deleted_entry(&mut self, mut entry: ManifestEntry) -> Result<(), String> {
        entry.status = ManifestStatus::Deleted;
        entry.snapshot_id = self.snapshot_id;
        self.add_rewritten_entry(entry)
    }

    fn add_rewritten_entry(&mut self, entry: ManifestEntry) -> Result<(), String> {
        if entry.snapshot_id.is_none()
            || entry.sequence_number.is_none()
            || entry.file_sequence_number.is_none()
        {
            return Err(
                "existing and deleted manifest entries require snapshot, data, and file sequence metadata"
                    .to_string(),
            );
        }
        self.entries.push(Arc::new(entry));
        Ok(())
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
        let added_rows = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, ManifestStatus::Added))
            .map(|e| e.data_file.record_count as i64)
            .sum();
        let existing_rows = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, ManifestStatus::Existing))
            .map(|e| e.data_file.record_count as i64)
            .sum();
        let deleted_rows = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, ManifestStatus::Deleted))
            .map(|e| e.data_file.record_count as i64)
            .sum();
        let min_sequence_number = self
            .entries
            .iter()
            .filter(|entry| !matches!(entry.status, ManifestStatus::Deleted))
            .filter_map(|entry| entry.sequence_number)
            .min()
            .unwrap_or(sequence_number);
        ManifestFile {
            manifest_path,
            manifest_length: 0,
            partition_spec_id: self.metadata.partition_spec.spec_id(),
            content: self.metadata.content,
            sequence_number,
            min_sequence_number,
            added_snapshot_id: snapshot_id,
            added_files_count: Some(added),
            existing_files_count: Some(existing),
            deleted_files_count: Some(deleted),
            added_rows_count: Some(added_rows),
            existing_rows_count: Some(existing_rows),
            deleted_rows_count: Some(deleted_rows),
            partitions: None,
            key_metadata: self.key_metadata,
            first_row_id: None,
        }
    }

    pub fn to_avro_bytes_v2(&self) -> Result<Vec<u8>, String> {
        // Build Avro schema from partition spec
        let partition_type = self
            .metadata
            .partition_spec
            .partition_type(&self.metadata.schema)
            .map_err(|e| format!("Partition type error: {e}"))?;
        let avro_schema = super::schema::manifest_entry_schema_v2(&partition_type);
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
        if self.metadata.format_version >= FormatVersion::V2 {
            let content_str = match self.metadata.content {
                ManifestContentType::Data => "data",
                ManifestContentType::Deletes => "deletes",
            };
            writer
                .add_user_metadata("content".to_string(), content_str)
                .map_err(|e| format!("Avro add_user_metadata error: {e}"))?;
        }

        for e in &self.entries {
            let serde_entry =
                super::_serde::ManifestEntryV2::from_entry((*e.clone()).clone(), &partition_type)?;
            let value = to_value(serde_entry)
                .map_err(|e| format!("Avro to_value error: {e}"))?
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

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::collections::HashMap;

    use super::*;
    use crate::spec::{
        DataContentType, DataFileFormat, ManifestContentType, PartitionSpec, Schema,
    };

    fn data_file(path: &str, record_count: u64) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count,
            file_size_in_bytes: 100,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn manifest_metadata() -> ManifestMetadata {
        let schema = Schema::builder().with_fields(vec![]).build().unwrap();
        ManifestMetadata::new(
            Arc::new(schema),
            0,
            PartitionSpec::builder().with_spec_id(0).build(),
            FormatVersion::V2,
            ManifestContentType::Data,
        )
    }

    #[test]
    fn existing_and_deleted_entries_preserve_sequence_metadata_and_counts() {
        let original = ManifestEntry::new(
            ManifestStatus::Added,
            Some(10),
            Some(1),
            Some(1),
            data_file("data/original.parquet", 3),
        );
        let mut writer = ManifestWriterBuilder::new(Some(20), None, manifest_metadata()).build();

        writer.add_existing_entry(original.clone()).unwrap();
        writer.add_deleted_entry(original).unwrap();

        let bytes = writer.to_avro_bytes_v2().unwrap();
        let manifest_file = writer.into_manifest_file("metadata/rewrite.avro".to_string(), 2, 20);
        let manifest = Manifest::parse_avro(&bytes).unwrap();

        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(manifest.entries[0].status, ManifestStatus::Existing);
        assert_eq!(manifest.entries[0].snapshot_id, Some(10));
        assert_eq!(manifest.entries[0].sequence_number, Some(1));
        assert_eq!(manifest.entries[0].file_sequence_number, Some(1));
        assert_eq!(manifest.entries[1].status, ManifestStatus::Deleted);
        assert_eq!(manifest.entries[1].snapshot_id, Some(20));
        assert_eq!(manifest.entries[1].sequence_number, Some(1));
        assert_eq!(manifest.entries[1].file_sequence_number, Some(1));

        assert_eq!(manifest_file.content, ManifestContentType::Data);
        assert_eq!(manifest_file.added_files_count, Some(0));
        assert_eq!(manifest_file.existing_files_count, Some(1));
        assert_eq!(manifest_file.deleted_files_count, Some(1));
        assert_eq!(manifest_file.added_rows_count, Some(0));
        assert_eq!(manifest_file.existing_rows_count, Some(3));
        assert_eq!(manifest_file.deleted_rows_count, Some(3));
        assert_eq!(manifest_file.min_sequence_number, 1);
    }

    #[test]
    fn existing_entries_require_their_original_snapshot_id() {
        let entry = ManifestEntry::new(
            ManifestStatus::Added,
            None,
            Some(1),
            Some(1),
            data_file("data/original.parquet", 3),
        );
        let mut writer = ManifestWriterBuilder::new(Some(20), None, manifest_metadata()).build();

        assert!(writer.add_existing_entry(entry).is_err());
    }
}
