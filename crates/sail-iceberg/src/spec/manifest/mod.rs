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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest/mod.rs

use std::sync::Arc;

use apache_avro::{from_value as avro_from_value, Reader as AvroReader};

mod _serde;
mod data_file;
mod entry;
mod metadata;
mod schema;
mod writer;

// Provide data file avro helpers API surface
use apache_avro::{to_value, Writer as AvroWriter};
pub use data_file::*;
pub use entry::*;
pub use metadata::*;
pub use writer::*;

use crate::spec::metadata::format::FormatVersion;
use crate::spec::types::StructType;
use crate::spec::Schema as IcebergSchema;

/// Convert data files to avro bytes and write to writer. Return the bytes written.
pub fn write_data_files_to_avro<W: std::io::Write>(
    writer: &mut W,
    data_files: impl IntoIterator<Item = DataFile>,
    partition_type: &StructType,
    version: FormatVersion,
) -> Result<usize, String> {
    let avro_schema = match version {
        FormatVersion::V1 => schema::data_file_schema_v2(partition_type),
        FormatVersion::V2 => schema::data_file_schema_v2(partition_type),
    };
    let mut writer = AvroWriter::new(&avro_schema, writer);

    for data_file in data_files {
        let serde_df = _serde::DataFileSerde::from_data_file(data_file, partition_type)?;
        let value = to_value(serde_df)
            .map_err(|e| format!("Avro to_value error: {e}"))?
            .resolve(&avro_schema)
            .map_err(|e| format!("Avro resolve error: {e}"))?;
        writer
            .append(value)
            .map_err(|e| format!("Avro append error: {e}"))?;
    }

    writer.flush().map_err(|e| format!("Avro flush error: {e}"))
}

/// Parse data files from avro bytes.
pub fn read_data_files_from_avro<R: std::io::Read>(
    reader: &mut R,
    _schema: &IcebergSchema,
    partition_spec_id: i32,
    partition_type: &StructType,
    _version: FormatVersion,
) -> Result<Vec<DataFile>, String> {
    let avro_schema = schema::data_file_schema_v2(partition_type);
    let reader = AvroReader::with_schema(&avro_schema, reader)
        .map_err(|e| format!("Avro reader error: {e}"))?;
    reader
        .into_iter()
        .map(|value| {
            let value = value.map_err(|e| format!("Avro read error: {e}"))?;
            let serde_df: _serde::DataFileSerde =
                avro_from_value(&value).map_err(|e| format!("Avro decode DataFile error: {e}"))?;
            Ok(serde_df.into_data_file(partition_spec_id, partition_type))
        })
        .collect::<Result<Vec<_>, String>>()
}

/// Reference to [`ManifestEntry`].
pub type ManifestEntryRef = Arc<ManifestEntry>;

/// A manifest contains metadata and a list of entries.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Manifest {
    /// Metadata about the manifest.
    pub metadata: ManifestMetadata,
    /// Entries in the manifest.
    pub entries: Vec<ManifestEntryRef>,
}

impl Manifest {
    /// Create a new manifest.
    pub fn new(metadata: ManifestMetadata, entries: Vec<ManifestEntry>) -> Self {
        Self {
            metadata,
            entries: entries.into_iter().map(Arc::new).collect(),
        }
    }

    /// Get the entries in the manifest.
    pub fn entries(&self) -> &[ManifestEntryRef] {
        &self.entries
    }

    /// Get the metadata of the manifest.
    pub fn metadata(&self) -> &ManifestMetadata {
        &self.metadata
    }

    /// Consume this Manifest, returning its constituent parts
    pub fn into_parts(self) -> (Vec<ManifestEntryRef>, ManifestMetadata) {
        let Self { entries, metadata } = self;
        (entries, metadata)
    }

    /// Parse manifest metadata and entries from bytes of avro file.
    pub(crate) fn try_from_avro_bytes(
        bs: &[u8],
    ) -> Result<(ManifestMetadata, Vec<ManifestEntry>), String> {
        let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;

        // Parse manifest metadata from avro user metadata
        let meta = reader.user_metadata();
        let metadata = ManifestMetadata::parse_from_avro_meta(meta)?;

        // For entries, use typed serde model
        let mut entries = Vec::new();
        // Build partition type and schema for deterministic resolution of unions
        let partition_type = metadata
            .partition_spec
            .partition_type(&metadata.schema)
            .map_err(|e| format!("Partition type error: {e}"))?;
        let avro_schema = schema::manifest_entry_schema_v2(&partition_type);
        let mut cursor = std::io::Cursor::new(bs);
        let reader = AvroReader::with_schema(&avro_schema, &mut cursor)
            .map_err(|e| format!("Avro read error: {e}"))?;
        for value in reader {
            let value = value.map_err(|e| format!("Avro read value error: {e}"))?;
            let entry: _serde::ManifestEntryV2 =
                avro_from_value(&value).map_err(|e| format!("Avro decode entry error: {e}"))?;
            entries.push(entry.into_entry(metadata.partition_spec.spec_id(), &partition_type));
        }

        Ok((metadata, entries))
    }

    /// Parse a manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self, String> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs)?;
        Ok(Manifest::new(metadata, entries))
    }

    pub fn to_avro_bytes_v2(&self) -> Result<Vec<u8>, String> {
        let builder = ManifestWriterBuilder::new(None, None, self.metadata.clone());
        let mut w = builder.build();
        for e in &self.entries {
            w.add(e.data_file.clone());
        }
        w.to_avro_bytes_v2()
    }
}
