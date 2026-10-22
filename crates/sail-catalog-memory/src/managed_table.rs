use sail_catalog::error::{CatalogError, CatalogResult};
use sail_common_datafusion::catalog::managed::{
    metadata_location_update, metadata_location_value, previous_metadata_location_update,
};

pub(crate) fn validate_metadata_location_precondition(
    table_name: &str,
    current_properties: &[(String, String)],
    new_properties: &[(String, String)],
) -> CatalogResult<()> {
    let Some(expected) = previous_metadata_location_update(new_properties) else {
        return Ok(());
    };
    if metadata_location_update(new_properties).is_none() {
        return Ok(());
    }
    let current = metadata_location_value(
        current_properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .map(ToString::to_string);
    if current.as_deref() != Some(expected) {
        return Err(CatalogError::Conflict(format!(
            "Cannot commit catalog-managed table '{table_name}' because base metadata location '{expected}' does not match current memory catalog metadata location '{}'",
            current.unwrap_or_else(|| "<none>".to_string())
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use sail_catalog::error::CatalogError;

    #[test]
    fn test_rejects_stale_metadata_location_update() {
        let current = vec![(
            "metadata-location".to_string(),
            "s3://warehouse/items/metadata/00001-current.metadata.json".to_string(),
        )];
        let update = vec![
            (
                "metadata-location".to_string(),
                "s3://warehouse/items/metadata/00002-new.metadata.json".to_string(),
            ),
            (
                "previous_metadata_location".to_string(),
                "s3://warehouse/items/metadata/00000-stale.metadata.json".to_string(),
            ),
        ];

        let error =
            super::validate_metadata_location_precondition("items", &current, &update).unwrap_err();

        assert!(matches!(error, CatalogError::Conflict(_)));
    }
}
