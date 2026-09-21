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
use std::sync::LazyLock;

use icu_casemap::CaseMapper;
use indexmap::IndexSet;
use regex::Regex;
use sail_common_datafusion::catalog::CatalogTableColumnIdentity;

use super::mapping::{annotate_new_fields_for_column_mapping, compute_max_column_id};
use crate::spec::{
    CheckpointPolicy, ColumnMappingMode, ColumnMetadataKey, DataType,
    DeltaError as DeltaTableError, DeltaResult, Metadata, MetadataValue, Protocol, StructField,
    StructType, TableFeature, TableProperties, contains_timestampntz, contains_variant,
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
    configuration
        .keys()
        .any(|key| is_check_constraint_property(key))
}

pub(crate) fn is_check_constraint_property(key: &str) -> bool {
    const PREFIX: &str = "delta.constraints.";
    key.len() > PREFIX.len()
        && key
            .get(..PREFIX.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
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

fn inject_string_column_metadata(
    schema: StructType,
    expressions: &HashMap<String, String>,
    key: ColumnMetadataKey,
) -> StructType {
    let metadata_key = key.as_ref();
    let fields = schema.into_fields().map(|field| {
        let Some(expr) = expressions.get(&field.name) else {
            return field;
        };
        if matches!(
            field.metadata.get(metadata_key),
            Some(MetadataValue::String(existing)) if existing == expr
        ) {
            return field;
        }
        let StructField {
            name,
            data_type,
            nullable,
            mut metadata,
        } = field;
        metadata.insert(
            metadata_key.to_string(),
            MetadataValue::String(expr.clone()),
        );
        StructField {
            name,
            data_type,
            nullable,
            metadata,
        }
    });
    StructType::new_unchecked(fields)
}

pub(crate) fn inject_generation_expressions(
    schema: StructType,
    generation_expressions: &HashMap<String, String>,
) -> StructType {
    inject_string_column_metadata(
        schema,
        generation_expressions,
        ColumnMetadataKey::GenerationExpression,
    )
}

pub(crate) fn inject_default_expressions(
    schema: StructType,
    default_expressions: &HashMap<String, String>,
) -> StructType {
    inject_string_column_metadata(
        schema,
        default_expressions,
        ColumnMetadataKey::CurrentDefault,
    )
}

pub(crate) fn inject_identity_columns(
    schema: StructType,
    identity_columns: &HashMap<String, CatalogTableColumnIdentity>,
) -> StructType {
    let fields = schema.into_fields().map(|field| {
        if let Some(identity) = identity_columns.get(&field.name) {
            let StructField {
                name,
                data_type,
                nullable,
                mut metadata,
            } = field;
            metadata.insert(
                ColumnMetadataKey::IdentityStart.as_ref().to_string(),
                MetadataValue::Number(identity.start),
            );
            metadata.insert(
                ColumnMetadataKey::IdentityStep.as_ref().to_string(),
                MetadataValue::Number(identity.step),
            );
            metadata.insert(
                ColumnMetadataKey::IdentityAllowExplicitInsert
                    .as_ref()
                    .to_string(),
                MetadataValue::Boolean(identity.allow_explicit_insert),
            );
            if let Some(high_water_mark) = identity.high_water_mark {
                metadata.insert(
                    ColumnMetadataKey::IdentityHighWaterMark
                        .as_ref()
                        .to_string(),
                    MetadataValue::Number(high_water_mark),
                );
            }
            StructField {
                name,
                data_type,
                nullable,
                metadata,
            }
        } else {
            field
        }
    });
    StructType::new_unchecked(fields)
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

// OpenJDK 17 uses Unicode 13, so newer characters must keep identity mappings.
#[expect(clippy::expect_used)]
static JDK_17_ASSIGNED_CHARACTER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\p{Age:13.0}$").expect("JDK 17 Unicode age pattern should be valid")
});

fn spark_case_insensitive_name_eq(case_mapper: &CaseMapper, left: &str, right: &str) -> bool {
    let mut left_chars = left.chars();
    let mut right_chars = right.chars();

    loop {
        match (left_chars.next(), right_chars.next()) {
            (None, None) => return true,
            (Some(left), Some(right)) if java_char_eq_ignore_case(case_mapper, left, right) => {}
            _ => return false,
        }
    }
}

fn java_char_eq_ignore_case(case_mapper: &CaseMapper, left: char, right: char) -> bool {
    if left == right {
        return true;
    }
    let mut left_buffer = [0; 4];
    let mut right_buffer = [0; 4];
    if !(JDK_17_ASSIGNED_CHARACTER.is_match(left.encode_utf8(&mut left_buffer))
        && JDK_17_ASSIGNED_CHARACTER.is_match(right.encode_utf8(&mut right_buffer)))
    {
        return false;
    }

    let left_upper = case_mapper.simple_uppercase(left);
    let right_upper = case_mapper.simple_uppercase(right);
    left_upper == right_upper
        || case_mapper.simple_lowercase(left_upper) == case_mapper.simple_lowercase(right_upper)
}

pub(crate) fn canonicalize_partition_columns(
    schema: &StructType,
    partition_columns: Vec<String>,
) -> DeltaResult<Vec<String>> {
    let case_mapper = CaseMapper::new();
    let mut resolved_partition_columns = Vec::with_capacity(partition_columns.len());
    for partition_column in partition_columns {
        let mut matches = schema.fields().filter(|field| {
            spark_case_insensitive_name_eq(&case_mapper, field.name(), &partition_column)
        });
        let field = matches.next().ok_or_else(|| {
            DeltaTableError::schema(format!(
                "partition column `{partition_column}` is not present in the table schema"
            ))
        })?;
        if matches.next().is_some() {
            return Err(DeltaTableError::schema(format!(
                "partition column `{partition_column}` is ambiguous under case-insensitive resolution"
            )));
        }
        if matches!(field.data_type(), DataType::Variant(_)) {
            return Err(DeltaTableError::schema(format!(
                "VARIANT column `{}` cannot be used as a partition column",
                field.name()
            )));
        }
        resolved_partition_columns.push(field.name().to_string());
    }
    Ok(resolved_partition_columns)
}

/// Build Metadata for table creation from an existing kernel StructType.
pub fn metadata_for_create_with_struct_type(
    schema: StructType,
    partition_columns: Vec<String>,
    created_time: i64,
    configuration: HashMap<String, String>,
) -> DeltaResult<Metadata> {
    let resolved_partition_columns = canonicalize_partition_columns(&schema, partition_columns)?;
    Metadata::try_new(
        None,
        None,
        schema,
        resolved_partition_columns,
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

fn enable_legacy_writer_features(writer_features: &mut IndexSet<TableFeature>) {
    writer_features.insert(TableFeature::AppendOnly);
    writer_features.insert(TableFeature::Invariants);
}

fn enable_variant_type_feature(
    reader_features: &mut IndexSet<TableFeature>,
    writer_features: &mut IndexSet<TableFeature>,
    feature: TableFeature,
) {
    reader_features.insert(feature.clone());
    writer_features.insert(feature);
}

fn enable_variant_type_features_for_schema(
    reader_features: &mut IndexSet<TableFeature>,
    writer_features: &mut IndexSet<TableFeature>,
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
    reader_features: &IndexSet<TableFeature>,
    writer_features: &IndexSet<TableFeature>,
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
    reader_features: &mut IndexSet<TableFeature>,
    writer_features: &mut IndexSet<TableFeature>,
    feature: TableFeature,
) {
    if feature == TableFeature::VariantShredding {
        enable_variant_type_feature(reader_features, writer_features, TableFeature::VariantType);
    }
    reader_features.insert(feature.clone());
    writer_features.insert(feature);
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
    let mut reader_features = IndexSet::new();
    let mut writer_features = IndexSet::new();
    let has_check_constraints = configuration_has_check_constraints(configuration);
    let table_properties = TableProperties::from(configuration.iter());
    let explicit_features = explicit_table_features(configuration)?;

    if enable_column_mapping {
        reader_features.insert(TableFeature::ColumnMapping);
        writer_features.insert(TableFeature::ColumnMapping);
    }
    if enable_timestamp_ntz {
        reader_features.insert(TableFeature::TimestampWithoutTimezone);
        writer_features.insert(TableFeature::TimestampWithoutTimezone);
    }
    if enable_in_commit_timestamps {
        writer_features.insert(TableFeature::InCommitTimestamp);
    }
    if enable_generated_columns {
        writer_features.insert(TableFeature::GeneratedColumns);
    }
    if enable_column_defaults {
        writer_features.insert(TableFeature::AllowColumnDefaults);
    }
    if enable_identity_columns {
        writer_features.insert(TableFeature::IdentityColumns);
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
                    reader_features.insert(feature.clone());
                }
                writer_features.insert(feature);
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
        reader_features.insert(TableFeature::DeletionVectors);
        writer_features.insert(TableFeature::DeletionVectors);
    }

    // `delta.enableTypeWidening = "true"` enables the stable TypeWidening feature unless
    // the table explicitly uses the preview feature.
    if table_properties.enable_type_widening() {
        let preview_enabled = reader_features.contains(&TableFeature::TypeWideningPreview)
            || writer_features.contains(&TableFeature::TypeWideningPreview);
        if !preview_enabled {
            reader_features.insert(TableFeature::TypeWidening);
            writer_features.insert(TableFeature::TypeWidening);
        }
    }

    // `delta.checkpointPolicy = "v2"` implicitly activates V2Checkpoint
    if table_properties.checkpoint_policy() == CheckpointPolicy::V2 {
        reader_features.insert(TableFeature::V2Checkpoint);
        writer_features.insert(TableFeature::V2Checkpoint);
    }

    // appendOnly is a legacy writer-v2 feature. It is listed explicitly only when another
    // requirement already places the table on the writer-v7 table-features protocol.
    if table_properties.append_only()
        && (!reader_features.is_empty() || !writer_features.is_empty())
    {
        writer_features.insert(TableFeature::AppendOnly);
    }

    if has_check_constraints && !writer_features.is_empty() {
        writer_features.insert(TableFeature::CheckConstraints);
    }

    if reader_features.is_empty() && writer_features.is_empty() {
        let min_writer_version = if has_check_constraints { 3 } else { 2 };
        return Ok(Protocol::new(1, min_writer_version, None, None));
    }

    enable_legacy_writer_features(&mut writer_features);

    let min_reader_version = if reader_features.is_empty() { 1 } else { 3 };
    let reader_features = (min_reader_version == 3).then(|| reader_features.into_iter().collect());

    Ok(Protocol::new(
        min_reader_version,
        7,
        reader_features,
        Some(writer_features.into_iter().collect()),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use icu_casemap::CaseMapper;

    use super::{
        metadata_for_create_with_struct_type, protocol_for_create, protocol_for_metadata,
        spark_case_insensitive_name_eq,
    };
    use crate::spec::{
        ColumnMetadataKey, DataType, DeltaResult, Metadata, StructField, StructType, TableFeature,
    };

    #[test]
    fn spark_case_insensitive_name_eq_matches_jdk_17_unicode_oracle() {
        let case_mapper = CaseMapper::new();
        for (left, right, expected) in [
            ("Σ", "ς", true),
            ("I", "ı", true),
            ("İ", "i", true),
            ("ß", "ẞ", true),
            ("K", "K", true),
            ("S", "ſ", true),
            ("ß", "ss", false),
            ("\u{10570}", "\u{10597}", false),
        ] {
            assert_eq!(
                spark_case_insensitive_name_eq(&case_mapper, left, right),
                expected,
                "unexpected JDK 17 case-insensitive comparison for {left:?} and {right:?}"
            );
            assert_eq!(
                spark_case_insensitive_name_eq(&case_mapper, right, left),
                expected,
                "unexpected JDK 17 case-insensitive comparison for {right:?} and {left:?}"
            );
        }
    }

    #[test]
    fn metadata_for_create_rejects_invalid_partition_columns() -> DeltaResult<()> {
        let cases = [
            (
                "missing column",
                StructType::try_new([StructField::nullable("id", DataType::INTEGER)])?,
                "missing",
                "partition column `missing` is not present in the table schema",
            ),
            (
                "ASCII case-insensitive ambiguity",
                StructType::try_new([
                    StructField::nullable("Category", DataType::STRING),
                    StructField::nullable("category", DataType::STRING),
                ])?,
                "CATEGORY",
                "partition column `CATEGORY` is ambiguous under case-insensitive resolution",
            ),
            (
                "Greek sigma case-insensitive ambiguity",
                StructType::try_new([
                    StructField::nullable("ΣDate", DataType::DATE),
                    StructField::nullable("ςDate", DataType::DATE),
                ])?,
                "σdate",
                "partition column `σdate` is ambiguous under case-insensitive resolution",
            ),
        ];

        for (case, schema, partition_column, expected_message) in cases {
            let result = metadata_for_create_with_struct_type(
                schema,
                vec![partition_column.to_string()],
                0,
                HashMap::new(),
            );
            assert!(
                matches!(&result, Err(error) if error.to_string().contains(expected_message)),
                "{case}: expected {expected_message:?}, got {result:?}"
            );
        }
        Ok(())
    }

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
        assert!(protocol.has_writer_feature(&TableFeature::InCommitTimestamp));
        assert!(protocol.has_writer_feature(&TableFeature::AppendOnly));
        assert!(protocol.has_writer_feature(&TableFeature::Invariants));
        Ok(())
    }

    #[test]
    fn protocol_for_create_extracts_explicit_table_features() -> DeltaResult<()> {
        let cases = [
            (
                "deprecated enabled status",
                &[("delta.feature.v2Checkpoint", "enabled")] as &[(&str, &str)],
                TableFeature::V2Checkpoint,
                true,
            ),
            (
                "supported reader-writer feature",
                &[("delta.feature.v2Checkpoint", "supported")],
                TableFeature::V2Checkpoint,
                true,
            ),
            (
                "supported writer-only feature",
                &[
                    ("delta.appendOnly", "true"),
                    ("delta.feature.appendOnly", "supported"),
                ],
                TableFeature::AppendOnly,
                false,
            ),
        ];

        for (case, properties, feature, is_reader_feature) in cases {
            let configuration = properties
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect::<HashMap<_, _>>();
            let protocol = protocol_for_create(
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                &configuration,
            )?;

            assert_eq!(
                protocol.min_reader_version(),
                if is_reader_feature { 3 } else { 1 },
                "{case}"
            );
            assert_eq!(protocol.min_writer_version(), 7, "{case}");
            assert_eq!(
                protocol.reader_features().is_some(),
                is_reader_feature,
                "{case}"
            );

            let reader_features = protocol.reader_features().unwrap_or(&[]);
            let writer_features = protocol.writer_features().unwrap_or(&[]);
            let reader_feature_set = reader_features.iter().cloned().collect::<HashSet<_>>();
            let writer_feature_set = writer_features.iter().cloned().collect::<HashSet<_>>();
            let expected_reader_features = if is_reader_feature {
                HashSet::from([feature.clone()])
            } else {
                HashSet::new()
            };
            let mut expected_writer_features =
                HashSet::from([TableFeature::AppendOnly, TableFeature::Invariants]);
            expected_writer_features.insert(feature);

            assert_eq!(
                reader_features.len(),
                expected_reader_features.len(),
                "{case}"
            );
            assert_eq!(
                writer_features.len(),
                expected_writer_features.len(),
                "{case}"
            );
            assert_eq!(reader_feature_set, expected_reader_features, "{case}");
            assert_eq!(writer_feature_set, expected_writer_features, "{case}");
        }
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
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
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
        let protocol =
            protocol_for_create(false, false, false, false, false, false, false, &config)?;
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
    fn protocol_for_create_stable_shredding_enforces_variant_type_dependency() -> DeltaResult<()> {
        let cases = [
            ("stable shredding", &[] as &[(&str, &str)], false, false),
            (
                "stable shredding with preview VariantType",
                &[("delta.feature.variantType-preview", "supported")],
                true,
                false,
            ),
            (
                "stable and preview shredding",
                &[("delta.feature.variantShredding-preview", "supported")],
                false,
                true,
            ),
        ];

        for (
            case,
            additional_properties,
            includes_preview_variant_type,
            includes_preview_shredding,
        ) in cases
        {
            let mut configuration = HashMap::from([(
                "delta.feature.variantShredding".to_string(),
                "supported".to_string(),
            )]);
            configuration.extend(
                additional_properties
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
            );
            let protocol = protocol_for_create(
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                &configuration,
            )?;

            assert_eq!(protocol.min_reader_version(), 3, "{case}");
            assert_eq!(protocol.min_writer_version(), 7, "{case}");

            let reader_features = protocol.reader_features().unwrap_or(&[]);
            let writer_features = protocol.writer_features().unwrap_or(&[]);
            let reader_feature_set = reader_features.iter().cloned().collect::<HashSet<_>>();
            let writer_feature_set = writer_features.iter().cloned().collect::<HashSet<_>>();
            let mut expected_reader_features =
                HashSet::from([TableFeature::VariantType, TableFeature::VariantShredding]);
            if includes_preview_variant_type {
                expected_reader_features.insert(TableFeature::VariantTypePreview);
            }
            if includes_preview_shredding {
                expected_reader_features.insert(TableFeature::VariantShreddingPreview);
            }
            let mut expected_writer_features = expected_reader_features.clone();
            expected_writer_features.extend([TableFeature::AppendOnly, TableFeature::Invariants]);

            assert_eq!(
                reader_features.len(),
                expected_reader_features.len(),
                "{case}"
            );
            assert_eq!(
                writer_features.len(),
                expected_writer_features.len(),
                "{case}"
            );
            assert_eq!(reader_feature_set, expected_reader_features, "{case}");
            assert_eq!(writer_feature_set, expected_writer_features, "{case}");
        }
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
    fn protocol_for_metadata_keeps_append_only_property_states_on_legacy_protocol()
    -> DeltaResult<()> {
        for append_only_value in [None, Some("false"), Some("true")] {
            let configuration = append_only_value
                .map(|value| ("delta.appendOnly".to_string(), value.to_string()))
                .into_iter()
                .collect::<HashMap<_, _>>();
            let schema = StructType::try_new([StructField::nullable("id", DataType::INTEGER)])?;
            let metadata = Metadata::try_new(None, None, schema, vec![], 0, configuration)?;
            let protocol = protocol_for_metadata(&metadata)?;

            assert_eq!(
                protocol.min_reader_version(),
                1,
                "append-only value {append_only_value:?}"
            );
            assert_eq!(
                protocol.min_writer_version(),
                2,
                "append-only value {append_only_value:?}"
            );
            assert_eq!(
                protocol.reader_features(),
                None,
                "append-only value {append_only_value:?}"
            );
            assert_eq!(
                protocol.writer_features(),
                None,
                "append-only value {append_only_value:?}"
            );
        }
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
        assert!(protocol.has_writer_feature(&TableFeature::AppendOnly));
        assert!(protocol.has_writer_feature(&TableFeature::Invariants));
        Ok(())
    }

    #[test]
    fn protocol_for_create_non_v2_checkpoint_policy_does_not_activate_v2_checkpoint()
    -> DeltaResult<()> {
        for checkpoint_policy in ["classic", "V2"] {
            let configuration = HashMap::from([(
                "delta.checkpointPolicy".to_string(),
                checkpoint_policy.to_string(),
            )]);
            let protocol = protocol_for_create(
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                &configuration,
            )?;

            assert_eq!(protocol.min_reader_version(), 1, "{checkpoint_policy}");
            assert_eq!(protocol.min_writer_version(), 2, "{checkpoint_policy}");
            assert_eq!(protocol.reader_features(), None, "{checkpoint_policy}");
            assert_eq!(protocol.writer_features(), None, "{checkpoint_policy}");
            assert!(!protocol.has_reader_feature(&TableFeature::V2Checkpoint));
            assert!(!protocol.has_writer_feature(&TableFeature::V2Checkpoint));
        }
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
