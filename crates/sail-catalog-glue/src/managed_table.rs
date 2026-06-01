use std::collections::HashMap;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::AlterTableOptions;
use sail_common_datafusion::catalog::managed::{
    metadata_location_update, metadata_location_value, previous_metadata_location_update,
};

pub(crate) fn apply_alter_table_options(
    database: &str,
    table: &str,
    mut parameters: HashMap<String, String>,
    options: AlterTableOptions,
) -> CatalogResult<HashMap<String, String>> {
    match options {
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
