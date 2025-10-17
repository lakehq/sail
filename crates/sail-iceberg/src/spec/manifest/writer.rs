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

use super::{
    DataFile, Manifest, ManifestEntry, ManifestEntryRef, ManifestMetadata, ManifestStatus,
};

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
    ) -> super::ManifestFile {
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
        super::ManifestFile {
            manifest_path,
            manifest_length: 0,
            partition_spec_id: self.metadata.partition_spec.spec_id(),
            content: super::ManifestContentType::Data,
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
}
