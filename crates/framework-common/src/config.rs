use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigKeyValue {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkUdfConfig {
    pub timezone: ConfigKeyValue,
    pub pandas_window_bound_types: ConfigKeyValue,
    pub pandas_grouped_map_assign_columns_by_name: ConfigKeyValue,
    pub pandas_convert_to_arrow_array_safely: ConfigKeyValue,
    pub arrow_max_records_per_batch: ConfigKeyValue,
}
