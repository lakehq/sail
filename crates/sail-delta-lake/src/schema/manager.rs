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

pub const ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME_KEY: &str =
    "delta.rowTracking.materializedRowIdColumnName";
pub const ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME_KEY: &str =
    "delta.rowTracking.materializedRowCommitVersionColumnName";
pub const ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_PREFIX: &str = "_row-id-col-";
pub const ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_PREFIX: &str =
    "_row-commit-version-col-";

fn configuration_enabled(configuration: &HashMap<String, String>, key: &str) -> bool {
    configuration
        .get(key)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

fn configuration_supports_row_tracking(configuration: &HashMap<String, String>) -> bool {
    configuration_enabled(configuration, "delta.enableRowTracking")
        || configuration_enabled(configuration, "delta.rowTrackingSuspended")
        || configuration
            .get("delta.feature.rowTracking")
            .is_some_and(|v| {
                v.eq_ignore_ascii_case("supported") || v.eq_ignore_ascii_case("enabled")
            })
}

/// Check if a Delta StructType schema contains any columns with generation expressions.
pub fn schema_has_generated_columns(schema: &StructType) -> bool {
    schema.fields().any(|f| {
        f.get_config_value(&ColumnMetadataKey::GenerationExpression)
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
        contains_variant(schema.fields()),
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
    enable_generated_columns: bool,
    enable_variant: bool,
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
    if enable_generated_columns {
        writer_features.push(TableFeature::GeneratedColumns);
    }
    if enable_variant {
        reader_features.push(TableFeature::VariantType);
        writer_features.push(TableFeature::VariantType);
        if !writer_features.contains(&TableFeature::AppendOnly) {
            writer_features.push(TableFeature::AppendOnly);
        }
        if !writer_features.contains(&TableFeature::Invariants) {
            writer_features.push(TableFeature::Invariants);
        }
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

    // RowTracking requires the DomainMetadata writer feature even when requested
    // explicitly through `delta.feature.rowTracking`.
    if configuration_supports_row_tracking(configuration) {
        if !writer_features.contains(&TableFeature::RowTracking) {
            writer_features.push(TableFeature::RowTracking);
        }
        if !writer_features.contains(&TableFeature::DomainMetadata) {
            writer_features.push(TableFeature::DomainMetadata);
        }
        if let (Some(row_tracking_index), Some(domain_metadata_index)) = (
            writer_features
                .iter()
                .position(|feature| feature == &TableFeature::RowTracking),
            writer_features
                .iter()
                .position(|feature| feature == &TableFeature::DomainMetadata),
        ) {
            if row_tracking_index < domain_metadata_index {
                let feature = writer_features.remove(domain_metadata_index);
                writer_features.insert(row_tracking_index, feature);
            }
        }
    }

    // `delta.enableTypeWidening = "true"` enables the stable TypeWidening feature unless
    // the table explicitly uses the preview feature.
    if TableProperties::from(configuration.iter()).enable_type_widening() {
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

/// Auto-assign Row Tracking materialized column names when Row Tracking is supported.
///
/// Delta Spark assigns `delta.rowTracking.materializedRowIdColumnName` and
/// `delta.rowTracking.materializedRowCommitVersionColumnName` as soon as the RowTracking feature is
/// present, even if `delta.enableRowTracking` is not true. Existing values are preserved (e.g.
/// carried over on ALTER TABLE) to keep parquet on-disk column names stable.
pub fn ensure_row_tracking_materialized_column_names(
    configuration: &mut HashMap<String, String>,
    existing: Option<&HashMap<String, String>>,
) {
    if !configuration_supports_row_tracking(configuration) {
        return;
    }
    for (key, prefix) in [
        (
            ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME_KEY,
            ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_PREFIX,
        ),
        (
            ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME_KEY,
            ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_PREFIX,
        ),
    ] {
        if configuration.get(key).map(|v| v.is_empty()).unwrap_or(true) {
            let value = existing
                .and_then(|cfg| cfg.get(key))
                .filter(|v| !v.is_empty())
                .cloned()
                .unwrap_or_else(|| format!("{prefix}{}", uuid::Uuid::new_v4()));
            configuration.insert(key.to_string(), value);
        }
    }
}

pub fn validate_row_tracking_materialized_column_names(
    schema: &StructType,
    configuration: &HashMap<String, String>,
    mode: ColumnMappingMode,
) -> DeltaResult<()> {
    let Some(row_id_name) = configuration
        .get(ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME_KEY)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(row_commit_version_name) = configuration
        .get(ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME_KEY)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };

    if row_id_name.eq_ignore_ascii_case(row_commit_version_name) {
        return Err(DeltaTableError::generic(format!(
            "Row Tracking materialized column names must be unique: {row_id_name}"
        )));
    }

    for materialized_name in [row_id_name, row_commit_version_name] {
        for field in schema.fields() {
            if field.name().eq_ignore_ascii_case(materialized_name) {
                return Err(DeltaTableError::generic(format!(
                    "Row Tracking materialized column name '{materialized_name}' conflicts with table column '{}'",
                    field.name()
                )));
            }
            if !matches!(mode, ColumnMappingMode::None)
                && field
                    .physical_name(mode)
                    .eq_ignore_ascii_case(materialized_name)
            {
                return Err(DeltaTableError::generic(format!(
                    "Row Tracking materialized column name '{materialized_name}' conflicts with physical column name '{}'",
                    field.physical_name(mode)
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        ensure_row_tracking_materialized_column_names, protocol_for_create, protocol_for_metadata,
        validate_row_tracking_materialized_column_names,
    };
    use crate::spec::{
        ColumnMappingMode, ColumnMetadataKey, DataType, DeltaError as DeltaTableError, DeltaResult,
        Metadata, MetadataValue, StructField, StructType, TableFeature,
    };

    #[test]
    fn protocol_for_create_treats_in_commit_timestamp_as_writer_only() -> DeltaResult<()> {
        let protocol = protocol_for_create(false, false, true, false, false, &HashMap::new())?;
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
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
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
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::V2Checkpoint));
        assert!(protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_variant_type_from_schema() -> DeltaResult<()> {
        let protocol = protocol_for_create(false, false, false, false, true, &HashMap::new())?;
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert!(protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::VariantType));
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
        assert!(protocol.has_reader_feature(&TableFeature::VariantType));
        assert!(protocol.has_writer_feature(&TableFeature::VariantType));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_v2_checkpoint_from_checkpoint_policy() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.checkpointPolicy".to_string(), "v2".to_string());
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
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
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
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
        let Err(err) = protocol_for_create(false, false, false, false, false, &config) else {
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
        let Err(err) = protocol_for_create(false, false, false, false, false, &config) else {
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
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
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
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
        assert!(!protocol.has_reader_feature(&TableFeature::DeletionVectors));
        assert!(!protocol.has_writer_feature(&TableFeature::DeletionVectors));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_row_tracking_from_enable_row_tracking() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.enableRowTracking".to_string(), "true".to_string());
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
        assert!(protocol.has_writer_feature(&TableFeature::RowTracking));
        assert!(protocol.has_writer_feature(&TableFeature::DomainMetadata));
        Ok(())
    }

    #[test]
    fn protocol_for_create_activates_row_tracking_from_suspended_flag() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert("delta.rowTrackingSuspended".to_string(), "true".to_string());
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
        assert!(protocol.has_writer_feature(&TableFeature::RowTracking));
        assert!(protocol.has_writer_feature(&TableFeature::DomainMetadata));
        Ok(())
    }

    #[test]
    fn protocol_for_create_adds_domain_metadata_for_explicit_row_tracking() -> DeltaResult<()> {
        let mut config = HashMap::new();
        config.insert(
            "delta.feature.rowTracking".to_string(),
            "supported".to_string(),
        );
        let protocol = protocol_for_create(false, false, false, false, false, &config)?;
        assert!(protocol.has_writer_feature(&TableFeature::RowTracking));
        assert!(protocol.has_writer_feature(&TableFeature::DomainMetadata));
        Ok(())
    }

    #[test]
    fn ensure_materialized_column_names_noop_without_row_tracking_feature() {
        let mut cfg = HashMap::new();
        ensure_row_tracking_materialized_column_names(&mut cfg, None);
        assert!(cfg.is_empty());
    }

    #[test]
    fn ensure_materialized_column_names_generates_when_missing() {
        let mut cfg = HashMap::new();
        cfg.insert("delta.enableRowTracking".to_string(), "true".to_string());
        ensure_row_tracking_materialized_column_names(&mut cfg, None);
        let id = cfg
            .get("delta.rowTracking.materializedRowIdColumnName")
            .cloned()
            .unwrap_or_default();
        let cv = cfg
            .get("delta.rowTracking.materializedRowCommitVersionColumnName")
            .cloned()
            .unwrap_or_default();
        assert!(id.starts_with("_row-id-col-"));
        assert!(cv.starts_with("_row-commit-version-col-"));
        assert_ne!(id, cv);
    }

    #[test]
    fn ensure_materialized_column_names_generates_for_explicit_row_tracking_feature() {
        let mut cfg = HashMap::new();
        cfg.insert(
            "delta.feature.rowTracking".to_string(),
            "supported".to_string(),
        );
        ensure_row_tracking_materialized_column_names(&mut cfg, None);
        let id = cfg
            .get("delta.rowTracking.materializedRowIdColumnName")
            .cloned()
            .unwrap_or_default();
        let cv = cfg
            .get("delta.rowTracking.materializedRowCommitVersionColumnName")
            .cloned()
            .unwrap_or_default();
        assert!(id.starts_with("_row-id-col-"));
        assert!(cv.starts_with("_row-commit-version-col-"));
        assert_ne!(id, cv);
    }

    #[test]
    fn ensure_materialized_column_names_preserves_existing() {
        let mut cfg = HashMap::new();
        cfg.insert("delta.enableRowTracking".to_string(), "true".to_string());
        let mut existing = HashMap::new();
        existing.insert(
            "delta.rowTracking.materializedRowIdColumnName".to_string(),
            "_row-id-col-kept".to_string(),
        );
        existing.insert(
            "delta.rowTracking.materializedRowCommitVersionColumnName".to_string(),
            "_row-commit-version-col-kept".to_string(),
        );
        ensure_row_tracking_materialized_column_names(&mut cfg, Some(&existing));
        assert_eq!(
            cfg.get("delta.rowTracking.materializedRowIdColumnName")
                .map(String::as_str),
            Some("_row-id-col-kept")
        );
        assert_eq!(
            cfg.get("delta.rowTracking.materializedRowCommitVersionColumnName")
                .map(String::as_str),
            Some("_row-commit-version-col-kept")
        );
    }

    #[test]
    fn validate_materialized_column_names_rejects_logical_name_conflict() -> DeltaResult<()> {
        let schema = StructType::try_new([StructField::not_null("id", DataType::LONG)])?;
        let mut cfg = HashMap::new();
        cfg.insert(
            "delta.rowTracking.materializedRowIdColumnName".to_string(),
            "id".to_string(),
        );
        cfg.insert(
            "delta.rowTracking.materializedRowCommitVersionColumnName".to_string(),
            "_row-commit-version-col-test".to_string(),
        );

        let err = match validate_row_tracking_materialized_column_names(
            &schema,
            &cfg,
            ColumnMappingMode::None,
        ) {
            Ok(()) => {
                return Err(DeltaTableError::generic(
                    "expected materialized row ID column name conflict",
                ));
            }
            Err(err) => err,
        };
        assert!(err.to_string().contains("conflicts with table column"));
        Ok(())
    }

    #[test]
    fn validate_materialized_column_names_rejects_physical_name_conflict() -> DeltaResult<()> {
        let schema = StructType::try_new([StructField::not_null("id", DataType::LONG)
            .with_metadata([(
                "delta.columnMapping.physicalName",
                MetadataValue::String("phys_id".to_string()),
            )])])?;
        let mut cfg = HashMap::new();
        cfg.insert(
            "delta.rowTracking.materializedRowIdColumnName".to_string(),
            "phys_id".to_string(),
        );
        cfg.insert(
            "delta.rowTracking.materializedRowCommitVersionColumnName".to_string(),
            "_row-commit-version-col-test".to_string(),
        );

        let err = match validate_row_tracking_materialized_column_names(
            &schema,
            &cfg,
            ColumnMappingMode::Name,
        ) {
            Ok(()) => {
                return Err(DeltaTableError::generic(
                    "expected materialized row ID physical column name conflict",
                ));
            }
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("conflicts with physical column name"));
        Ok(())
    }
}
