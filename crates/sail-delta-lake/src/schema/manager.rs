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

use std::collections::HashMap;

use super::mapping::{annotate_new_fields_for_column_mapping, compute_max_column_id};
use crate::spec::{ColumnMappingMode, DeltaResult, Metadata, Protocol, StructType, TableFeature};

/// Evolve table schema and update metadata according to column mapping mode.
pub fn evolve_schema(
    existing: &StructType,
    candidate: &StructType,
    metadata: &Metadata,
    mode: ColumnMappingMode,
) -> DeltaResult<(StructType, Metadata)> {
    let updated = if matches!(mode, ColumnMappingMode::Name | ColumnMappingMode::Id) {
        let next_id = metadata
            .configuration()
            .get("delta.columnMapping.maxColumnId")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_else(|| compute_max_column_id(existing));

        let (annotated, last_id) =
            annotate_new_fields_for_column_mapping(existing, candidate, next_id + 1);

        let meta_with_schema = metadata.clone().with_schema(&annotated)?;
        let meta_with_max = meta_with_schema.add_config_key(
            "delta.columnMapping.maxColumnId".to_string(),
            last_id.to_string(),
        );
        (annotated, meta_with_max)
    } else {
        let meta = metadata.clone().with_schema(candidate)?;
        (candidate.clone(), meta)
    };
    Ok(updated)
}

/// Build Metadata for table creation from an existing kernel StructType.
pub fn metadata_for_create_with_struct_type(
    schema: StructType,
    partition_columns: Vec<String>,
    created_time: i64,
    configuration: HashMap<String, String>,
) -> DeltaResult<Metadata> {
    Metadata::try_new(
        None,
        None,
        schema,
        partition_columns,
        created_time,
        configuration,
    )
}

/// Build Protocol for a create/write path based on required table features.
pub fn protocol_for_create(
    enable_column_mapping: bool,
    enable_timestamp_ntz: bool,
    enable_in_commit_timestamps: bool,
) -> DeltaResult<Protocol> {
    if !enable_column_mapping && !enable_timestamp_ntz && !enable_in_commit_timestamps {
        return Ok(Protocol::new(1, 2, None, None));
    }

    let mut reader_features = Vec::new();
    let mut writer_features = Vec::new();
    if enable_column_mapping {
        reader_features.push(TableFeature::ColumnMapping);
        writer_features.push(TableFeature::ColumnMapping);
    }
    if enable_timestamp_ntz {
        reader_features.push(TableFeature::TimestampWithoutTimezone);
        writer_features.push(TableFeature::TimestampWithoutTimezone);
    }
    if enable_in_commit_timestamps {
        writer_features.push(TableFeature::InCommitTimestamp);
    }

    let min_reader_version = if reader_features.is_empty() { 1 } else { 3 };

    Ok(Protocol::new(
        min_reader_version,
        7,
        Some(reader_features),
        Some(writer_features),
    ))
}

#[cfg(test)]
mod tests {
    use super::protocol_for_create;
    use crate::spec::{DeltaResult, TableFeature};

    #[test]
    fn protocol_for_create_treats_in_commit_timestamp_as_writer_only() -> DeltaResult<()> {
        let protocol = protocol_for_create(false, false, true)?;
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(protocol.reader_features(), None);
        assert_eq!(
            protocol.writer_features(),
            Some([TableFeature::InCommitTimestamp].as_slice())
        );
        Ok(())
    }
}
