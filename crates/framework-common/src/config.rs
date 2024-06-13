use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimestampType {
    TimestampLtz,
    TimestampNtz,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigEntry {
    pub key: &'static str,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkUdfConfig {
    pub timezone: ConfigEntry,
    pub pandas_window_bound_types: ConfigEntry,
    pub pandas_grouped_map_assign_columns_by_name: ConfigEntry,
    pub pandas_convert_to_arrow_array_safely: ConfigEntry,
    pub arrow_max_records_per_batch: ConfigEntry,
}
