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
    contains_timestampntz, contains_variant, ColumnMappingMode, ColumnMetadataKey,
    DeltaError as DeltaTableError, DeltaResult, Metadata, Protocol, StructType, TableFeature,
    TableProperties,
};

/// Check if a Delta StructType schema contains any columns with generation expressions.
pub fn schema_has_generated_columns(schema: &StructType) -> bool {
    schema.fields().any(|f| {
        f.get_config_value(&ColumnMetadataKey::GenerationExpression)
            .is_some()
    })
}

/// Check if a Delta StructType schema contains any columns with current default expressions.
pub fn schema_has_column_defaults(schema: &StructType) -> bool {
    schema.fields().any(|f| {
        f.get_config_value(&ColumnMetadataKey::CurrentDefault)
            .is_some()
    })
}

/// Check if a Delta metadata configuration contains table CHECK constraints.
pub fn configuration_has_check_constraints(configuration: &HashMap<String, String>) -> bool {
    const PREFIX: &str = "delta.constraints.";
    configuration.keys().any(|key| {
        key.len() > PREFIX.len()
            && key
                .get(..PREFIX.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
    })
}

pub fn schema_has_identity_columns(schema: &StructType) -> bool {
    schema.fields().any(|f| {
        f.get_config_value(&ColumnMetadataKey::IdentityStart)
            .is_some()
            && f.get_config_value(&ColumnMetadataKey::IdentityStep)
                .is_some()
            && f.get_config_value(&ColumnMetadataKey::IdentityAllowExplicitInsert)
                .is_some()
    })
}

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

/// Build Protocol for an existing metadata action by deriving required features from schema and configuration.
pub fn protocol_for_metadata(metadata: &Metadata) -> DeltaResult<Protocol> {
    let configuration = metadata.configuration();
    let table_properties = TableProperties::from(configuration.iter());
    let schema = metadata.parse_schema()?;
    let enable_column_mapping = table_properties
        .column_mapping_mode
        .is_some_and(|mode| !matches!(mode, ColumnMappingMode::None));

    protocol_for_create(
        enable_column_mapping,
        contains_timestampntz(schema.fields()),
        table_properties.enable_in_commit_timestamps(),
        schema_has_generated_columns(&schema),
        schema_has_column_defaults(&schema),
        schema_has_identity_columns(&schema),
        contains_variant(schema.fields()),
        configuration,
    )
}

fn push_feature(features: &mut Vec<TableFeature>, feature: TableFeature) {
    if !features.contains(&feature) {
        features.push(feature);
    }
}

fn enable_legacy_writer_features(writer_features: &mut Vec<TableFeature>) {
    push_feature(writer_features, TableFeature::AppendOnly);
    push_feature(writer_features, TableFeature::Invariants);
}

fn enable_variant_type_feature(
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
    feature: TableFeature,
) {
    push_feature(reader_features, feature.clone());
    push_feature(writer_features, feature);
    enable_legacy_writer_features(writer_features);
}

fn enable_variant_type_features_for_schema(
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
    explicit_features: &[TableFeature],
) {
    let feature = if explicit_features.contains(&TableFeature::VariantTypePreview)
        && !explicit_features.contains(&TableFeature::VariantType)
    {
        TableFeature::VariantTypePreview
    } else {
        TableFeature::VariantType
    };
    enable_variant_type_feature(reader_features, writer_features, feature);
}

fn has_variant_shredding_feature(
    reader_features: &[TableFeature],
    writer_features: &[TableFeature],
) -> bool {
    reader_features
        .iter()
        .chain(writer_features)
        .any(|feature| {
            matches!(
                feature,
                TableFeature::VariantShredding | TableFeature::VariantShreddingPreview
            )
        })
}

fn enable_variant_shredding_feature(
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
    feature: TableFeature,
) {
    push_feature(reader_features, feature.clone());
    push_feature(writer_features, feature);
    enable_legacy_writer_features(writer_features);
}

fn explicit_table_features(
    configuration: &HashMap<String, String>,
) -> DeltaResult<Vec<TableFeature>> {
    let mut features = Vec::new();
    for (key, value) in configuration {
        if let Some(name) = key.strip_prefix("delta.feature.") {
            let status = value.to_ascii_lowercase();
            if status != "supported" && status != "enabled" {
                return Err(DeltaTableError::generic(format!(
                    "invalid value `{value}` for table feature property `{key}`; \
                     expected \"supported\" or \"enabled\"",
                )));
            }
            let feature = TableFeature::parse_str_name(name).map_err(|_| {
                DeltaTableError::generic(format!(
                    "unknown table feature `{name}` in `{key}` = `{value}`; \
                     check for typos in the feature name",
                ))
            })?;
            features.push(feature);
        }
    }
    Ok(features)
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
    enable_generated_columns: bool,
    enable_column_defaults: bool,
    enable_identity_columns: bool,
    enable_variant: bool,
    configuration: &HashMap<String, String>,
) -> DeltaResult<Protocol> {
    let mut reader_features = Vec::new();
    let mut writer_features = Vec::new();
    let has_check_constraints = configuration_has_check_constraints(configuration);
    let table_properties = TableProperties::from(configuration.iter());
    let explicit_features = explicit_table_features(configuration)?;

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
    if enable_generated_columns {
        writer_features.push(TableFeature::GeneratedColumns);
    }
    if enable_column_defaults {
        writer_features.push(TableFeature::AllowColumnDefaults);
    }
    if enable_identity_columns {
        writer_features.push(TableFeature::IdentityColumns);
    }
    if enable_variant {
        enable_variant_type_features_for_schema(
            &mut reader_features,
            &mut writer_features,
            &explicit_features,
        );
    }

    // Extract features from `delta.feature.<name> = "supported"|"enabled"` configuration entries.
    // Unknown feature names always produce an error regardless of value.
    for feature in explicit_features {
        match feature {
            TableFeature::VariantShredding | TableFeature::VariantShreddingPreview => {
                enable_variant_shredding_feature(
                    &mut reader_features,
                    &mut writer_features,
                    feature,
                );
            }
            feature => {
                if feature.is_reader_feature() {
                    push_feature(&mut reader_features, feature.clone());
                }
                push_feature(&mut writer_features, feature);
            }
        }
    }

    // `delta.enableVariantShredding = "true"` activates the preview feature unless the
    // table explicitly selected a variant-shredding feature.
    if table_properties.enable_variant_shredding()
        && !has_variant_shredding_feature(&reader_features, &writer_features)
    {
        enable_variant_shredding_feature(
            &mut reader_features,
            &mut writer_features,
            TableFeature::VariantShreddingPreview,
        );
    }

    // `delta.enableDeletionVectors = "true"` implicitly activates DeletionVectors.
    // Setting the metadata property is sufficient—`delta.feature.deletionVectors` is
    // not also required.
    if configuration
        .get("delta.enableDeletionVectors")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        if !reader_features.contains(&TableFeature::DeletionVectors) {
            reader_features.push(TableFeature::DeletionVectors);
        }
        if !writer_features.contains(&TableFeature::DeletionVectors) {
            writer_features.push(TableFeature::DeletionVectors);
        }
    }

    // `delta.enableTypeWidening = "true"` enables the stable TypeWidening feature unless
    // the table explicitly uses the preview feature.
    if table_properties.enable_type_widening() {
        let preview_enabled = reader_features.contains(&TableFeature::TypeWideningPreview)
            || writer_features.contains(&TableFeature::TypeWideningPreview);
        if !preview_enabled && !reader_features.contains(&TableFeature::TypeWidening) {
            reader_features.push(TableFeature::TypeWidening);
        }
        if !preview_enabled && !writer_features.contains(&TableFeature::TypeWidening) {
            writer_features.push(TableFeature::TypeWidening);
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

    if has_check_constraints
        && !writer_features.is_empty()
        && !writer_features.contains(&TableFeature::CheckConstraints)
    {
        writer_features.push(TableFeature::CheckConstraints);
    }

    if reader_features.is_empty() && writer_features.is_empty() {
        let min_writer_version = if has_check_constraints { 3 } else { 2 };
        return Ok(Protocol::new(1, min_writer_version, None, None));
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

    use super::{protocol_for_create, protocol_for_metadata};
    use crate::spec::{
        ColumnMetadataKey, DataType, DeltaResult, Metadata, StructField, StructType, TableFeature,
    };

    #[test]
    fn protocol_for_create_treats_in_commit_timestamp_as_writer_only() -> DeltaResult<()> {
        let protocol = protocol_for_create(
            false,
            false,
            true,
            false,
            false,
            false,
            false,
            &HashMap::new(),
        )?;
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
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
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
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_variant_type_from_schema() -> DeltaResult<()> {
        let protocol = protocol_for_create(
            false,
            false,
            false,
            false,
            false,
            false,
            true,
            &HashMap::new(),
        )?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::VariantType));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_check_constraints_from_configuration() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.constraints.positive_id".to_string(),
            "id > 0".to_string(),
        );
        let protocol = protocol_for_create(false, false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(protocol.min_writer_version(), 3);
        assert_eq!(protocol.reader_features(), None);
        assert_eq!(protocol.writer_features(), None);
        Ok(())
    }

    #[test]
    fn protocol_for_create_adds_check_constraints_to_writer_features() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.constraints.positive_id".to_string(),
            "id > 0".to_string(),
        );
        config.insert("delta.checkpointPolicy".to_string(), "v2".to_string());
        let protocol = protocol_for_create(false, false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::CheckConstraints));
        Ok(())
    }

    #[test]
    fn protocol_for_create_respects_explicit_preview_variant_type() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.variantType-preview".to_string(),
            "supported".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, true, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::VariantTypePreview));
        assert!(protocol.has_writer_feature(&TableFeature::VariantTypePreview));
        assert!(!protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::AppendOnly));
        assert!(protocol.has_writer_feature(&TableFeature::Invariants));
        Ok(())
    }

    #[test]
    fn protocol_for_create_shredding_property_without_variant() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.enableVariantShredding".to_string(),
            "true".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(!protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantType));
        assert!(protocol.has_reader_feature(&TableFeature::VariantShreddingPreview));
        assert!(protocol.has_writer_feature(&TableFeature::VariantShreddingPreview));
        assert!(protocol.has_writer_feature(&TableFeature::AppendOnly));
        assert!(protocol.has_writer_feature(&TableFeature::Invariants));
        assert!(!protocol.has_reader_feature(&TableFeature::VariantShredding));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantShredding));
        Ok(())
    }

    #[test]
    fn protocol_for_create_shredding_property_with_variant() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.enableVariantShredding".to_string(),
            "true".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, true, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::VariantType));
        assert!(protocol.has_reader_feature(&TableFeature::VariantShreddingPreview));
        assert!(protocol.has_writer_feature(&TableFeature::VariantShreddingPreview));
        assert!(!protocol.has_reader_feature(&TableFeature::VariantShredding));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantShredding));
        Ok(())
    }

    #[test]
    fn protocol_for_create_stable_shredding_without_variant() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.variantShredding".to_string(),
            "supported".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(!protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantType));
        assert!(protocol.has_reader_feature(&TableFeature::VariantShredding));
        assert!(protocol.has_writer_feature(&TableFeature::VariantShredding));
        assert!(protocol.has_writer_feature(&TableFeature::AppendOnly));
        assert!(protocol.has_writer_feature(&TableFeature::Invariants));
        assert!(!protocol.has_reader_feature(&TableFeature::VariantShreddingPreview));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantShreddingPreview));
        Ok(())
    }

    #[test]
    fn protocol_for_create_stable_shredding_with_variant() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.enableVariantShredding".to_string(),
            "true".to_string(),
        );
        config.insert(
            "delta.feature.variantShredding".to_string(),
            "supported".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, true, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::VariantType));
        assert!(protocol.has_reader_feature(&TableFeature::VariantShredding));
        assert!(protocol.has_writer_feature(&TableFeature::VariantShredding));
        assert!(!protocol.has_reader_feature(&TableFeature::VariantShreddingPreview));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantShreddingPreview));
        Ok(())
    }

    #[test]
    fn protocol_for_create_preview_shredding_without_variant() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.variantShredding-preview".to_string(),
            "supported".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(!protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(!protocol.has_writer_feature(&TableFeature::VariantType));
        assert!(protocol.has_reader_feature(&TableFeature::VariantShreddingPreview));
        assert!(protocol.has_writer_feature(&TableFeature::VariantShreddingPreview));
        assert!(protocol.has_writer_feature(&TableFeature::AppendOnly));
        assert!(protocol.has_writer_feature(&TableFeature::Invariants));
        Ok(())
    }

    #[test]
    fn protocol_for_create_stable_shredding_with_variant_type() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.variantShredding".to_string(),
            "supported".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, true, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::VariantType));
        assert!(protocol.has_reader_feature(&TableFeature::VariantShredding));
        assert!(protocol.has_writer_feature(&TableFeature::VariantShredding));
        Ok(())
    }

    #[test]
    fn protocol_for_metadata_activates_schema_and_property_features() -> DeltaResult<()> {
        let schema = StructType::try_new([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("event_time", DataType::TIMESTAMP_NTZ),
            StructField::nullable("payload", DataType::unshredded_variant()),
            StructField::nullable("generated_id", DataType::INTEGER)
                .with_metadata([(ColumnMetadataKey::GenerationExpression.as_ref(), "id + 1")]),
        ])?;
        let mut configuration = HashMap::new();
        configuration.insert("delta.columnMapping.mode".to_string(), "name".to_string());
        configuration.insert(
            "delta.enableInCommitTimestamps".to_string(),
            "true".to_string(),
        );
        configuration.insert(
            "delta.constraints.positive_id".to_string(),
            "id > 0".to_string(),
        );
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, configuration)?;

        let protocol = protocol_for_metadata(&metadata)?;

        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::ColumnMapping));
        assert!(protocol.has_writer_feature(&TableFeature::ColumnMapping));
        assert!(protocol.has_reader_feature(&TableFeature::TimestampWithoutTimezone));
        assert!(protocol.has_writer_feature(&TableFeature::TimestampWithoutTimezone));
        assert!(protocol.has_writer_feature(&TableFeature::InCommitTimestamp));
        assert!(protocol.has_writer_feature(&TableFeature::GeneratedColumns));
        assert!(protocol.has_writer_feature(&TableFeature::CheckConstraints));
        assert!(protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::VariantType));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_v2_checkpoint_from_checkpoint_policy() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.checkpointPolicy".to_string(), "v2".to_string());
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
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
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
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
        let Err(err) =
            protocol_for_create(false, false, false, false, false, false, false, &config)
        else {
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
        let Err(err) =
            protocol_for_create(false, false, false, false, false, false, false, &config)
        else {
            panic!("expected protocol_for_create to error on invalid feature value");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("true"),
            "error message should include the bad value: {msg}"
        );
    }

    #[test]
    fn protocol_for_create_activates_deletion_vectors_from_enable_property() -> DeltaResult<()> {
        // `delta.enableDeletionVectors = true` alone must register the DeletionVectors feature
        // in both reader and writer features.
        let mut config = HashMap::new();
        config.insert(
            "delta.enableDeletionVectors".to_string(),
            "true".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::DeletionVectors));
        assert!(protocol.has_writer_feature(&TableFeature::DeletionVectors));
        Ok(())
    }

    #[test]
    fn protocol_for_create_deletion_vectors_not_activated_when_disabled() -> DeltaResult<()> {
        // `delta.enableDeletionVectors = false` must NOT register the feature.
        let mut config = HashMap::new();
        config.insert(
            "delta.enableDeletionVectors".to_string(),
            "false".to_string(),
        );
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
        assert!(!protocol.has_reader_feature(&TableFeature::DeletionVectors));
        assert!(!protocol.has_writer_feature(&TableFeature::DeletionVectors));
        Ok(())
    }
}
