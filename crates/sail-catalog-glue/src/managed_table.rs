use std::collections::HashMap;

use aws_sdk_glue::types::TableInput;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::AlterTableOptions;
use sail_common_datafusion::catalog::managed::{
    metadata_location_update, metadata_location_value, previous_metadata_location_update,
};
use sail_common_datafusion::column_features::ColumnFeatureKey;

pub(crate) fn validate_column_path(name: &[String]) -> CatalogResult<()> {
    if name.len() != 1 {
        return Err(CatalogError::NotSupported(
            "Glue ALTER COLUMN requires a top-level column".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn apply_alter_table_input(
    database: &str,
    table: &str,
    input: &mut TableInput,
    options: AlterTableOptions,
) -> CatalogResult<()> {
    if let AlterTableOptions::SetLocation { location } = &options {
        let storage = input.storage_descriptor.as_mut().ok_or_else(|| {
            CatalogError::InvalidArgument("Missing Glue storage descriptor".into())
        })?;
        storage.location = Some(location.clone());
        if let Some(serde) = storage.serde_info.as_mut()
            && let Some(parameters) = serde.parameters.as_mut()
            && parameters.contains_key("path")
        {
            parameters.insert("path".into(), location.clone());
        }
        return Ok(());
    }
    if let AlterTableOptions::ReplaceColumns { fields, .. } = &options {
        return replace_columns(input, &arrow::datatypes::Schema::new(fields.clone()));
    }
    if let AlterTableOptions::AlterColumns {
        changes,
        case_sensitive,
    } = &options
    {
        return alter_columns(input, changes, *case_sensitive);
    }
    let name = match &options {
        AlterTableOptions::AlterColumnType { name, .. }
        | AlterTableOptions::AlterColumnDefault { name, .. } => name,
        _ => {
            let comment = match &options {
                AlterTableOptions::SetTableProperties { properties } => properties
                    .iter()
                    .rev()
                    .find(|(key, _)| key == "comment")
                    .map(|(_, value)| Some(value.clone())),
                AlterTableOptions::UnsetTableProperties { keys, .. }
                    if keys.iter().any(|key| key == "comment") =>
                {
                    Some(None)
                }
                _ => None,
            };
            input.parameters = Some(apply_alter_table_options(
                database,
                table,
                input.parameters.take().unwrap_or_default(),
                options,
            )?);
            if let Some(comment) = comment {
                input.description = comment;
            }
            return Ok(());
        }
    };
    validate_column_path(name)?;
    let is_delta = input.parameters.as_ref().is_some_and(|parameters| {
        ["spark.sql.sources.provider", "classification"]
            .iter()
            .any(|key| {
                parameters
                    .get(*key)
                    .is_some_and(|value| value.eq_ignore_ascii_case("delta"))
            })
    });
    let column = input
        .storage_descriptor
        .iter_mut()
        .flat_map(|storage| storage.columns.iter_mut().flatten())
        .chain(input.partition_keys.iter_mut().flatten())
        .find(|column| column.name.eq_ignore_ascii_case(&name[0]));
    let Some(column) = column else {
        if is_delta {
            return Ok(());
        }
        return Err(CatalogError::InvalidArgument(format!(
            "Column '{}' does not exist on '{database}.{table}'",
            name[0]
        )));
    };
    match options {
        AlterTableOptions::AlterColumnType { data_type, .. } => {
            column.r#type = Some(crate::data_type::arrow_to_glue_type(&data_type)?);
        }
        AlterTableOptions::AlterColumnDefault { default, .. } => {
            let parameters = column.parameters.get_or_insert_with(HashMap::new);
            let key = ColumnFeatureKey::CurrentDefault.as_str();
            if let Some(default) = default {
                parameters.insert(key.to_string(), default);
            } else {
                parameters.remove(key);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

pub(crate) fn apply_alter_table_options(
    database: &str,
    table: &str,
    mut parameters: HashMap<String, String>,
    options: AlterTableOptions,
) -> CatalogResult<HashMap<String, String>> {
    match options {
        // Iceberg partition specs live in format metadata; Glue partition keys stay empty.
        AlterTableOptions::AlterSnapshotReference { .. }
        | AlterTableOptions::AlterIdentifierFields { .. }
        | AlterTableOptions::AlterPartitionSpec { .. }
        | AlterTableOptions::SetPartitioning { .. } => {}
        AlterTableOptions::SetWriteOrder { distribution, .. } => {
            if let Some(value) = distribution {
                parameters.insert("write.distribution-mode".into(), value);
            }
        }
        AlterTableOptions::SetLocation { .. }
        | AlterTableOptions::ReplaceColumns { .. }
        | AlterTableOptions::AlterColumns { .. } => {
            return Err(CatalogError::NotSupported(
                "Column schema changes require catalog schema synchronization".into(),
            ));
        }

        AlterTableOptions::SetTableProperties { properties } => {
            validate_metadata_location_precondition(database, table, &parameters, &properties)?;
            parameters.extend(properties);
        }
        AlterTableOptions::UnsetTableProperties { keys, if_exists } => {
            for key in keys {
                if parameters.remove(&key).is_none() && !if_exists {
                    return Err(CatalogError::InvalidArgument(format!(
                        "Table property '{key}' does not exist on '{database}.{table}'"
                    )));
                }
            }
        }
        AlterTableOptions::AlterColumnType { .. } => {
            return Err(CatalogError::NotSupported(
                "AWS Glue catalog does not support ALTER COLUMN TYPE".to_string(),
            ));
        }
        AlterTableOptions::AlterColumnDefault { .. } => {
            return Err(CatalogError::NotSupported(
                "AWS Glue catalog does not support ALTER COLUMN DEFAULT".to_string(),
            ));
        }
        AlterTableOptions::AddCheckConstraint { .. }
        | AlterTableOptions::DropCheckConstraint { .. } => {
            return Err(CatalogError::NotSupported(
                "CHECK constraints are handled by lake sources".to_string(),
            ));
        }
    }
    Ok(parameters)
}

fn validate_metadata_location_precondition(
    database: &str,
    table: &str,
    parameters: &HashMap<String, String>,
    properties: &[(String, String)],
) -> CatalogResult<()> {
    let Some(expected) = previous_metadata_location_update(properties) else {
        return Ok(());
    };
    if metadata_location_update(properties).is_none() {
        return Ok(());
    }
    let current = metadata_location_value(
        parameters
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .map(ToString::to_string);
    if current.as_deref() != Some(expected) {
        return Err(CatalogError::Conflict(format!(
            "Cannot commit catalog-managed table '{database}.{table}' because base metadata location '{expected}' does not match current Glue metadata location '{}'",
            current.unwrap_or_else(|| "<none>".to_string())
        )));
    }
    Ok(())
}

fn alter_columns(
    input: &mut TableInput,
    changes: &[sail_catalog::provider::ColumnChange<arrow::datatypes::DataType>],
    case_sensitive: bool,
) -> CatalogResult<()> {
    use arrow::datatypes::{Field, Schema};

    use crate::data_type::glue_type_to_arrow;
    let original = input
        .storage_descriptor
        .as_ref()
        .and_then(|s| s.columns.as_ref())
        .into_iter()
        .flatten()
        .chain(input.partition_keys.iter().flatten())
        .cloned()
        .collect::<Vec<_>>();
    let fields = original
        .iter()
        .map(|column| {
            let mut field = Field::new(
                &column.name,
                glue_type_to_arrow(column.r#type.as_deref().unwrap_or("string"))?,
                true,
            );
            if let Some(comment) = &column.comment {
                field
                    .metadata_mut()
                    .insert("comment".into(), comment.clone());
            }
            field
                .metadata_mut()
                .extend(column.parameters.clone().unwrap_or_default());
            Ok(field)
        })
        .collect::<CatalogResult<Vec<_>>>()?;
    let updated = sail_common_datafusion::column_change::alter_schema(
        &Schema::new(fields),
        changes,
        case_sensitive,
    )
    .map_err(|e| CatalogError::InvalidArgument(e.to_string()))?;
    let mut partition_names = input
        .partition_keys
        .iter()
        .flatten()
        .map(|c| c.name.clone())
        .collect::<Vec<_>>();
    for change in changes {
        if let sail_catalog::provider::ColumnChange::Rename { path, name } = change
            && let [old] = path.as_slice()
        {
            for partition in &mut partition_names {
                if partition.eq_ignore_ascii_case(old) {
                    *partition = name.clone();
                }
            }
        }
    }
    replace_columns_with_partitions(input, &updated, partition_names)
}

pub(crate) fn replace_columns(
    input: &mut TableInput,
    schema: &arrow::datatypes::Schema,
) -> CatalogResult<()> {
    let partition_names = input
        .partition_keys
        .iter()
        .flatten()
        .map(|c| c.name.clone())
        .collect();
    replace_columns_with_partitions(input, schema, partition_names)
}

fn replace_columns_with_partitions(
    input: &mut TableInput,
    updated: &arrow::datatypes::Schema,
    partition_names: Vec<String>,
) -> CatalogResult<()> {
    use aws_sdk_glue::types::Column;

    use crate::data_type::arrow_to_glue_type;
    let mut columns = Vec::new();
    let mut partitions = Vec::new();
    for field in updated.fields() {
        let column = Column::builder()
            .name(field.name())
            .r#type(arrow_to_glue_type(field.data_type())?)
            .set_comment(field.metadata().get("comment").cloned())
            .set_parameters(Some(
                field
                    .metadata()
                    .iter()
                    .filter(|(key, _)| !matches!(key.as_str(), "comment" | "doc"))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ))
            .build()
            .map_err(|e| CatalogError::InvalidArgument(e.to_string()))?;
        if partition_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(field.name()))
        {
            partitions.push(column);
        } else {
            columns.push(column);
        }
    }
    let storage = input
        .storage_descriptor
        .as_mut()
        .ok_or_else(|| CatalogError::InvalidArgument("Missing Glue storage descriptor".into()))?;
    storage.columns = Some(columns);
    input.partition_keys = Some(partitions);
    Ok(())
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::collections::HashMap;

    use sail_catalog::error::CatalogError;
    use sail_catalog::provider::AlterTableOptions;

    #[test]
    fn test_apply_alter_table_options_rejects_stale_metadata_location() {
        let parameters = HashMap::from([(
            "metadata-location".to_string(),
            "s3://warehouse/items/metadata/00001-current.metadata.json".to_string(),
        )]);

        let error = super::apply_alter_table_options(
            "default",
            "items",
            parameters,
            AlterTableOptions::SetTableProperties {
                properties: vec![
                    (
                        "metadata-location".to_string(),
                        "s3://warehouse/items/metadata/00002-new.metadata.json".to_string(),
                    ),
                    (
                        "previous_metadata_location".to_string(),
                        "s3://warehouse/items/metadata/00000-stale.metadata.json".to_string(),
                    ),
                ],
            },
        )
        .unwrap_err();

        assert!(matches!(error, CatalogError::Conflict(_)));
    }

    #[test]
    fn test_apply_alter_table_options_accepts_matching_metadata_location() {
        let parameters = HashMap::from([(
            "metadata_location".to_string(),
            "s3://warehouse/items/metadata/00001-current.metadata.json".to_string(),
        )]);

        let parameters = super::apply_alter_table_options(
            "default",
            "items",
            parameters,
            AlterTableOptions::SetTableProperties {
                properties: vec![
                    (
                        "metadata_location".to_string(),
                        "s3://warehouse/items/metadata/00002-new.metadata.json".to_string(),
                    ),
                    (
                        "previous_metadata_location".to_string(),
                        "s3://warehouse/items/metadata/00001-current.metadata.json".to_string(),
                    ),
                ],
            },
        )
        .unwrap();

        assert_eq!(
            parameters.get("metadata_location"),
            Some(&"s3://warehouse/items/metadata/00002-new.metadata.json".to_string())
        );
    }
}
