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
use crate::spec::{
    ColumnMappingMode, DeltaError as DeltaTableError, DeltaResult, Metadata, Protocol, StructType,
    TableFeature,
};

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
///
/// In addition to the explicitly toggled features, this function scans the table
/// `configuration` for `delta.feature.<name> = "supported"` entries and includes
/// the corresponding [`TableFeature`] in the protocol.
pub fn protocol_for_create(
    enable_column_mapping: bool,
    enable_timestamp_ntz: bool,
    enable_in_commit_timestamps: bool,
    configuration: &HashMap<String, String>,
) -> DeltaResult<Protocol> {
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

    // Extract features from `delta.feature.<name> = "supported"|"enabled"` configuration entries.
    // Unknown feature names always produce an error regardless of value.
    for (key, value) in configuration {
        if let Some(name) = key.strip_prefix("delta.feature.") {
            let status = value.to_lowercase();
            if status != "supported" && status != "enabled" {
                return Err(DeltaTableError::generic(format!(
                    "invalid value `{value}` for table feature property `{key}`; \
                     expected \"supported\" or \"enabled\"",
                )));
            }
            match TableFeature::parse_str_name(name) {
                Ok(feature) => {
                    if feature.is_reader_feature() && !reader_features.contains(&feature) {
                        reader_features.push(feature.clone());
                    }
                    if !writer_features.contains(&feature) {
                        writer_features.push(feature);
                    }
                }
                Err(_) => {
                    return Err(DeltaTableError::generic(format!(
                        "unknown table feature `{name}` in `{key}` = `{value}`; \
                         check for typos in the feature name",
                    )));
                }
            }
        }
    }

    // `delta.checkpointPolicy = "v2"` implicitly activates V2Checkpoint
    if configuration
        .get("delta.checkpointPolicy")
        .map(|v| v.eq_ignore_ascii_case("v2"))
        .unwrap_or(false)
    {
        if !reader_features.contains(&TableFeature::V2Checkpoint) {
            reader_features.push(TableFeature::V2Checkpoint);
        }
        if !writer_features.contains(&TableFeature::V2Checkpoint) {
            writer_features.push(TableFeature::V2Checkpoint);
        }
    }

    if reader_features.is_empty() && writer_features.is_empty() {
        return Ok(Protocol::new(1, 2, None, None));
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
    use std::collections::HashMap;

    use super::protocol_for_create;
    use crate::spec::{DeltaResult, TableFeature};

    #[test]
    fn protocol_for_create_treats_in_commit_timestamp_as_writer_only() -> DeltaResult<()> {
        let protocol = protocol_for_create(false, false, true, &HashMap::new())?;
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(protocol.reader_features(), None);
        assert_eq!(
            protocol.writer_features(),
            Some([TableFeature::InCommitTimestamp].as_slice())
        );
        Ok(())
    }

    #[test]
    fn protocol_for_create_extracts_v2_checkpoint_from_configuration() -> DeltaResult<()> {
        // "enabled" (deprecated) still accepted for backward compatibility.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpoint".to_string(),
            "enabled".to_string(),
        );
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_extracts_v2_checkpoint_with_supported_value() -> DeltaResult<()> {
        // "supported" is the current/preferred value.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpoint".to_string(),
            "supported".to_string(),
        );
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_v2_checkpoint_from_checkpoint_policy() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.checkpointPolicy".to_string(), "v2".to_string());
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_classic_policy_does_not_activate_v2_checkpoint() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.checkpointPolicy".to_string(), "classic".to_string());
        let protocol = protocol_for_create(false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(protocol.min_writer_version(), 2);
        assert!(!protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(!protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    #[expect(clippy::panic)]
    fn protocol_for_create_errors_on_unknown_feature_name() {
        // Typo in the feature name must be caught instead of silently ignored.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpiont".to_string(), // intentional typo
            "supported".to_string(),
        );
        let Err(err) = protocol_for_create(false, false, false, &config) else {
            panic!("expected protocol_for_create to error on unknown feature name");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("v2Checkpiont"),
            "error message should include the bad feature name: {msg}"
        );
    }

    #[test]
    #[expect(clippy::panic)]
    fn protocol_for_create_errors_on_invalid_feature_value() {
        // Any value other than "supported" or "enabled" must produce an error.
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.v2Checkpoint".to_string(),
            "true".to_string(), // invalid
        );
        let Err(err) = protocol_for_create(false, false, false, &config) else {
            panic!("expected protocol_for_create to error on invalid feature value");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("true"),
            "error message should include the bad value: {msg}"
        );
    }
}
