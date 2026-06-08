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
