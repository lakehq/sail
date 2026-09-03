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

use std::cmp::Ordering;
use std::sync::Arc;

use apache_avro::{Writer as AvroWriter, to_value};

use super::{
    DataFile, Manifest, ManifestEntry, ManifestEntryRef, ManifestMetadata, ManifestStatus,
};
use crate::spec::FormatVersion;
use crate::spec::manifest_list::{FieldSummary, ManifestContentType, ManifestFile};
use crate::spec::types::{Datum, Literal, PrimitiveLiteral, PrimitiveType, Type};

#[derive(Debug, Default)]
struct PartitionFieldStats {
    contains_null: bool,
    contains_nan: bool,
    lower_bound: Option<PrimitiveLiteral>,
    upper_bound: Option<PrimitiveLiteral>,
}

fn compare_partition_literals(
    partition_type: &PrimitiveType,
    left: &PrimitiveLiteral,
    right: &PrimitiveLiteral,
) -> Ordering {
    match (partition_type, left, right) {
        (PrimitiveType::Float, PrimitiveLiteral::Float(left), PrimitiveLiteral::Float(right)) => {
            left.0.total_cmp(&right.0)
        }
        (
            PrimitiveType::Double,
            PrimitiveLiteral::Double(left),
            PrimitiveLiteral::Double(right),
        ) => left.0.total_cmp(&right.0),
        (
            PrimitiveType::Uuid,
            PrimitiveLiteral::UInt128(left),
            PrimitiveLiteral::UInt128(right),
        ) => {
            let left_high = (left >> 64) as u64 as i64;
            let right_high = (right >> 64) as u64 as i64;
            left_high.cmp(&right_high).then_with(|| {
                let left_low = *left as u64 as i64;
                let right_low = *right as u64 as i64;
                left_low.cmp(&right_low)
            })
        }
        _ => left.cmp(right),
    }
}

fn partition_summaries(
    metadata: &ManifestMetadata,
    entries: &[ManifestEntryRef],
) -> Result<Vec<FieldSummary>, String> {
    let partition_type = metadata
        .partition_spec
        .partition_type(&metadata.schema)
        .map_err(|error| format!("Partition type error: {error}"))?;
    let fields = partition_type.fields();
    let mut stats = fields
        .iter()
        .map(|_| PartitionFieldStats::default())
        .collect::<Vec<_>>();

    for entry in entries {
        let data_file = &entry.data_file;
        if data_file.partition_spec_id != metadata.partition_spec.spec_id() {
            return Err(format!(
                "Iceberg data file `{}` uses partition spec {}, but manifest uses partition spec {}",
                data_file.file_path,
                data_file.partition_spec_id,
                metadata.partition_spec.spec_id()
            ));
        }
        if data_file.partition.len() != fields.len() {
            return Err(format!(
                "Iceberg data file `{}` has {} partition values, but partition spec {} requires {}",
                data_file.file_path,
                data_file.partition.len(),
                metadata.partition_spec.spec_id(),
                fields.len()
            ));
        }

        for ((field, value), field_stats) in fields
            .iter()
            .zip(data_file.partition.iter())
            .zip(stats.iter_mut())
        {
            let Type::Primitive(primitive_type) = field.field_type.as_ref() else {
                return Err(format!(
                    "Iceberg partition field `{}` must have a primitive result type",
                    field.name
                ));
            };
            let Some(value) = value else {
                field_stats.contains_null = true;
                continue;
            };
            let Literal::Primitive(value) = value else {
                return Err(format!(
                    "Iceberg partition field `{}` must contain a primitive literal",
                    field.name
                ));
            };
            let value = primitive_type.promote_literal(value).ok_or_else(|| {
                format!(
                    "Iceberg partition field `{}` value is incompatible with type {primitive_type}",
                    field.name
                )
            })?;
            let value = value.as_ref();
            let is_nan = match value {
                PrimitiveLiteral::Float(value) => value.0.is_nan(),
                PrimitiveLiteral::Double(value) => value.0.is_nan(),
                _ => false,
            };
            if is_nan {
                field_stats.contains_nan = true;
                continue;
            }

            if field_stats.lower_bound.as_ref().is_none_or(|lower| {
                compare_partition_literals(primitive_type, value, lower).is_lt()
            }) {
                field_stats.lower_bound = Some(value.clone());
            }
            if field_stats.upper_bound.as_ref().is_none_or(|upper| {
                compare_partition_literals(primitive_type, value, upper).is_gt()
            }) {
                field_stats.upper_bound = Some(value.clone());
            }
        }
    }

    fields
        .iter()
        .zip(stats)
        .map(|(field, stats)| {
            let Type::Primitive(primitive_type) = field.field_type.as_ref() else {
                return Err(format!(
                    "Iceberg partition field `{}` must have a primitive result type",
                    field.name
                ));
            };
            let mut summary =
                FieldSummary::new(stats.contains_null).with_contains_nan(stats.contains_nan);
            if let Some(lower_bound) = stats.lower_bound {
                summary = summary.with_lower_bound_bytes(
                    Datum::new(primitive_type.clone(), lower_bound).to_bytes()?,
                );
            }
            if let Some(upper_bound) = stats.upper_bound {
                summary = summary.with_upper_bound_bytes(
                    Datum::new(primitive_type.clone(), upper_bound).to_bytes()?,
                );
            }
            Ok(summary)
        })
        .collect()
}

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

    pub fn add_entry(&mut self, entry: ManifestEntry) {
        self.entries.push(Arc::new(entry));
    }

    pub fn add_existing_entry(&mut self, mut entry: ManifestEntry) -> Result<(), String> {
        if entry.sequence_number.is_none() || entry.file_sequence_number.is_none() {
            return Err(
                "existing manifest entries require data and file sequence numbers".to_string(),
            );
        }
        entry.status = ManifestStatus::Existing;
        self.entries.push(Arc::new(entry));
        Ok(())
    }

    pub fn add_deleted_entry(&mut self, mut entry: ManifestEntry) -> Result<(), String> {
        if entry.sequence_number.is_none() || entry.file_sequence_number.is_none() {
            return Err(
                "deleted manifest entries require data and file sequence numbers".to_string(),
            );
        }
        entry.status = ManifestStatus::Deleted;
        entry.snapshot_id = self.snapshot_id;
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
    ) -> Result<ManifestFile, String> {
        let partitions = partition_summaries(&self.metadata, &self.entries)?;
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
            .filter(|entry| {
                matches!(
                    entry.status,
                    ManifestStatus::Added | ManifestStatus::Existing
                )
            })
            .map(|entry| entry.sequence_number.unwrap_or(sequence_number))
            .min()
            .unwrap_or(sequence_number);
        Ok(ManifestFile {
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
            partitions: Some(partitions),
            key_metadata: self.key_metadata,
            first_row_id: None,
        })
    }

    pub fn to_avro_bytes_v2(&self) -> Result<Vec<u8>, String> {
        // Build Avro schema from partition spec
        let partition_type = self
            .metadata
            .partition_spec
            .partition_type(&self.metadata.schema)
            .map_err(|e| format!("Partition type error: {e}"))?;
        let avro_schema = match self.metadata.format_version {
            FormatVersion::V1 => super::schema::manifest_entry_schema_v1(&partition_type),
            FormatVersion::V2 | FormatVersion::V3 => {
                super::schema::manifest_entry_schema_v2(&partition_type)
            }
        };
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

        for entry in &self.entries {
            let entry = entry.as_ref().clone();
            let value = match self.metadata.format_version {
                FormatVersion::V1 => to_value(super::_serde::ManifestEntryV1::from_entry(
                    entry,
                    &partition_type,
                )?),
                FormatVersion::V2 | FormatVersion::V3 => to_value(
                    super::_serde::ManifestEntryV2::from_entry(entry, &partition_type)?,
                ),
            }
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
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ordered_float::OrderedFloat;

    use super::{ManifestMetadata, ManifestWriterBuilder, compare_partition_literals};
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Datum, FormatVersion, Literal, Manifest,
        ManifestContentType, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema,
        Transform, Type,
    };

    fn partitioned_file(path: &str, partition: Vec<Option<Literal>>) -> DataFile {
        DataFile {
            content: DataContentType::PositionDeletes,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition,
            record_count: 1,
            file_size_in_bytes: 1,
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

    #[test]
    fn manifest_file_contains_partition_summaries_and_metadata_content() {
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                    false,
                )),
                Arc::new(NestedField::new(
                    2,
                    "ratio",
                    Type::Primitive(PrimitiveType::Float),
                    false,
                )),
            ])
            .build()
            .expect("partition schema");
        let partition_spec = PartitionSpec::builder()
            .add_field(1, "id", Transform::Identity)
            .add_field(2, "ratio", Transform::Identity)
            .build();
        let metadata = ManifestMetadata::new(
            Arc::new(schema),
            0,
            partition_spec,
            FormatVersion::V2,
            ManifestContentType::Deletes,
        );
        let mut writer = ManifestWriterBuilder::new(Some(7), None, metadata).build();
        writer.add(partitioned_file(
            "delete-1.parquet",
            vec![
                Some(Literal::Primitive(PrimitiveLiteral::Long(2))),
                Some(Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(
                    f32::NAN,
                )))),
            ],
        ));
        writer.add(partitioned_file(
            "delete-2.parquet",
            vec![
                None,
                Some(Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(
                    -0.0,
                )))),
            ],
        ));
        writer.add(partitioned_file(
            "delete-3.parquet",
            vec![
                Some(Literal::Primitive(PrimitiveLiteral::Long(1))),
                Some(Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(
                    3.5,
                )))),
            ],
        ));

        let manifest_file = writer
            .into_manifest_file("delete-manifest.avro".to_string(), 3, 7)
            .expect("manifest file");
        let summaries = manifest_file.partitions.expect("partition summaries");

        assert_eq!(manifest_file.content, ManifestContentType::Deletes);
        assert_eq!(summaries.len(), 2);
        assert!(summaries[0].contains_null);
        assert_eq!(summaries[0].contains_nan, Some(false));
        assert_eq!(
            PrimitiveType::Long
                .literal_from_bytes(
                    summaries[0]
                        .lower_bound_bytes
                        .as_deref()
                        .expect("id lower bound")
                )
                .expect("decode id lower bound"),
            PrimitiveLiteral::Long(1)
        );
        assert_eq!(
            PrimitiveType::Long
                .literal_from_bytes(
                    summaries[0]
                        .upper_bound_bytes
                        .as_deref()
                        .expect("id upper bound")
                )
                .expect("decode id upper bound"),
            PrimitiveLiteral::Long(2)
        );
        assert!(!summaries[1].contains_null);
        assert_eq!(summaries[1].contains_nan, Some(true));
        assert_eq!(
            summaries[1]
                .lower_bound_bytes
                .as_deref()
                .expect("ratio lower bound"),
            (-0.0_f32).to_le_bytes()
        );
        assert_eq!(
            PrimitiveType::Float
                .literal_from_bytes(
                    summaries[1]
                        .upper_bound_bytes
                        .as_deref()
                        .expect("ratio upper bound")
                )
                .expect("decode ratio upper bound"),
            PrimitiveLiteral::Float(OrderedFloat(3.5))
        );
    }

    #[test]
    fn uuid_partition_order_matches_java_uuid_order() {
        let signed_high_bit = PrimitiveLiteral::UInt128(1_u128 << 127);
        let positive_high_half = PrimitiveLiteral::UInt128(1_u128 << 126);

        assert!(
            compare_partition_literals(&PrimitiveType::Uuid, &signed_high_bit, &positive_high_half)
                .is_lt()
        );
    }

    #[test]
    fn manifest_roundtrip_preserves_binary_metrics_and_key_metadata() {
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::new(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
                false,
            ))])
            .build()
            .expect("table schema");
        let metadata = ManifestMetadata::new(
            Arc::new(schema),
            0,
            PartitionSpec::unpartitioned_spec(),
            FormatVersion::V2,
            ManifestContentType::Data,
        );
        let mut file = partitioned_file("data.parquet", vec![]);
        file.content = DataContentType::Data;
        file.lower_bounds.insert(
            1,
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::Long(10)),
        );
        file.upper_bounds.insert(
            1,
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::Long(20)),
        );
        file.key_metadata = Some(vec![1, 2, 3]);
        let mut writer = ManifestWriterBuilder::new(Some(7), None, metadata).build();
        writer.add(file);

        let bytes = writer.to_avro_bytes_v2().expect("manifest bytes");
        let manifest = Manifest::parse_avro(&bytes).expect("parsed manifest");
        let parsed = &manifest.entries()[0].data_file;

        assert_eq!(
            parsed.lower_bounds.get(&1),
            Some(&Datum::new(PrimitiveType::Long, PrimitiveLiteral::Long(10)))
        );
        assert_eq!(
            parsed.upper_bounds.get(&1),
            Some(&Datum::new(PrimitiveType::Long, PrimitiveLiteral::Long(20)))
        );
        assert_eq!(parsed.key_metadata.as_deref(), Some([1, 2, 3].as_slice()));
    }
}
