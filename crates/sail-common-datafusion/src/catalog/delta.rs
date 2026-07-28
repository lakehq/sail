use super::managed;

pub const DELTA_TABLE_TYPE_VALUE: &str = "delta";
pub const DELTA_UNITY_TABLE_ID_KEY: &str = "io.unitycatalog.tableId";
pub const DELTA_UNITY_TABLE_ID_LEGACY_KEY: &str = "table_id";

pub fn is_delta_table_properties<'a, I>(properties: I) -> bool
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    managed::is_table_format_properties(properties, DELTA_TABLE_TYPE_VALUE)
}

pub fn unity_table_id_value<'a, I>(properties: I) -> Option<&'a str>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
    I::IntoIter: Clone,
{
    let properties = properties.into_iter();
    managed::property_value(properties.clone(), DELTA_UNITY_TABLE_ID_KEY)
        .or_else(|| managed::property_value(properties, DELTA_UNITY_TABLE_ID_LEGACY_KEY))
}
