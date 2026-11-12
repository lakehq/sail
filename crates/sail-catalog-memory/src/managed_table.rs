use sail_catalog::error::{CatalogError, CatalogResult};
use sail_common_datafusion::catalog::managed::{
    METADATA_LOCATION_UNDERSCORE_KEY, PREVIOUS_METADATA_LOCATION_KEY, is_metadata_location_key,
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

/// Validate every alias before the caller merges any properties.
pub(crate) fn validate_iceberg_metadata_update<'a>(
    current: impl IntoIterator<Item = (&'a str, &'a str)>,
    incoming: &[(String, String)],
) -> CatalogResult<Option<(String, Option<String>)>> {
    fn unique<'a>(values: impl Iterator<Item = &'a str>) -> CatalogResult<Option<&'a str>> {
        let mut values = values;
        let first = values.next();
        if values.any(|value| Some(value) != first) {
            return Err(CatalogError::CommitRejected(
                "Inconsistent Iceberg metadata pointer aliases".into(),
            ));
        }
        Ok(first)
    }
    let replacement = unique(incoming.iter().filter_map(|(key, value)| {
        is_metadata_location_key(key.trim()).then_some(value.as_str())
    }))?;
    let Some(replacement) = replacement else {
        return Ok(None);
    };
    let expected = unique(incoming.iter().filter_map(|(key, value)| {
        key.trim()
            .eq_ignore_ascii_case(PREVIOUS_METADATA_LOCATION_KEY)
            .then_some(value.as_str())
    }))?;
    let current = unique(
        current
            .into_iter()
            .filter_map(|(key, value)| is_metadata_location_key(key.trim()).then_some(value)),
    )?;
    if let Some(expected) = expected
        && current != Some(expected)
    {
        return Err(CatalogError::Conflict(
            "Iceberg metadata pointer changed".into(),
        ));
    }
    Ok(Some((replacement.into(), expected.map(Into::into))))
}

pub(crate) fn update_metadata_properties(
    properties: &mut Vec<(String, String)>,
    replacement: &str,
    previous: Option<&str>,
) {
    properties.retain(|(key, _)| {
        !is_metadata_location_key(key.trim())
            && !(previous.is_some()
                && key
                    .trim()
                    .eq_ignore_ascii_case(PREVIOUS_METADATA_LOCATION_KEY))
    });
    properties.push((METADATA_LOCATION_UNDERSCORE_KEY.into(), replacement.into()));
    if let Some(previous) = previous {
        properties.push((PREVIOUS_METADATA_LOCATION_KEY.into(), previous.into()));
    }
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
