pub const TABLE_TYPE_KEY: &str = "table_type";
pub const CLASSIFICATION_KEY: &str = "classification";
pub const METADATA_LOCATION_KEY: &str = "metadata-location";
pub const METADATA_LOCATION_UNDERSCORE_KEY: &str = "metadata_location";
pub const METADATA_LOCATION_KEYS: &[&str] =
    &[METADATA_LOCATION_KEY, METADATA_LOCATION_UNDERSCORE_KEY];
pub const PREVIOUS_METADATA_LOCATION_KEY: &str = "previous_metadata_location";

pub fn is_table_format_marker(key: &str, value: &str, format: &str) -> bool {
    (key.eq_ignore_ascii_case(TABLE_TYPE_KEY) || key.eq_ignore_ascii_case(CLASSIFICATION_KEY))
        && value.eq_ignore_ascii_case(format)
}

pub fn is_metadata_location_key(key: &str) -> bool {
    METADATA_LOCATION_KEYS
        .iter()
        .any(|candidate| key.eq_ignore_ascii_case(candidate))
}

pub fn is_table_format_properties<'a, I>(properties: I, format: &str) -> bool
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    properties
        .into_iter()
        .any(|(key, value)| is_table_format_marker(key.trim(), value.trim(), format))
}

pub fn property_value<'a, I>(properties: I, key: &str) -> Option<&'a str>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    properties
        .into_iter()
        .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(key))
        .map(|(_, value)| value)
}

pub fn metadata_location_value<'a, I>(properties: I) -> Option<&'a str>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
    I::IntoIter: Clone,
{
    let properties = properties.into_iter();
    METADATA_LOCATION_KEYS
        .iter()
        .find_map(|key| property_value(properties.clone(), key))
}

pub fn metadata_location_update(properties: &[(String, String)]) -> Option<&str> {
    properties
        .iter()
        .find(|(key, _)| is_metadata_location_key(key))
        .map(|(_, value)| value.as_str())
}

pub fn previous_metadata_location_update(properties: &[(String, String)]) -> Option<&str> {
    properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(PREVIOUS_METADATA_LOCATION_KEY))
        .map(|(_, value)| value.as_str())
}

pub fn existing_metadata_location_key(properties: &[(String, String)]) -> Option<&str> {
    properties
        .iter()
        .find(|(key, _)| is_metadata_location_key(key))
        .map(|(key, _)| key.as_str())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_table_format_marker_is_format_agnostic() {
        assert!(super::is_table_format_marker(
            "TABLE_TYPE",
            "DELTA",
            "delta"
        ));
        assert!(super::is_table_format_marker(
            "classification",
            "ICEBERG",
            "iceberg"
        ));
        assert!(!super::is_table_format_marker(
            "table_type",
            "iceberg",
            "delta"
        ));
    }

    #[test]
    fn test_metadata_location_helpers_accept_dash_and_underscore_keys() {
        let properties = [
            ("metadata_location", "s3://bucket/table/metadata/00001.json"),
            ("table_type", "delta"),
        ];

        assert_eq!(
            super::metadata_location_value(properties),
            Some("s3://bucket/table/metadata/00001.json")
        );
        assert!(super::is_table_format_properties(properties, "delta"));
        assert!(!super::is_table_format_properties(
            [("metadata_location", "s3://bucket/table/metadata/00001.json")],
            "delta"
        ));
    }
}
