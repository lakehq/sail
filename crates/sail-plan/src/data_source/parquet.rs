use std::collections::HashMap;
use std::convert::TryFrom;

use sail_common::config::PARQUET_READ_CONFIG;
use serde::Deserialize;

use crate::data_source::{
    parse_bool, parse_non_empty_string, parse_non_zero_usize, DataSourceOptions,
};
use crate::error::{PlanError, PlanResult};

/// Datasource Options that control the reading of Parquet files.
#[derive(Debug, Deserialize)]
// Serde bypasses any individual field deserializers and instead uses the `TryFrom` implementation.
#[serde(try_from = "HashMap<String, String>")]
pub struct ParquetReadOptions {
    pub enable_page_index: bool,
    pub pruning: bool,
    pub skip_metadata: bool,
    pub metadata_size_hint: Option<usize>,
    pub pushdown_filters: bool,
    pub reorder_filters: bool,
    pub schema_force_view_types: bool,
    pub binary_as_string: bool,
    pub coerce_int96: String,
    pub bloom_filter_on_read: bool,
}

impl TryFrom<HashMap<String, String>> for ParquetReadOptions {
    type Error = PlanError;

    // The options HashMap should already contain all supported keys with their resolved values
    fn try_from(mut options: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(ParquetReadOptions {
            enable_page_index: options
                .remove("enable_page_index")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `enable_page_index` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            pruning: options
                .remove("pruning")
                .ok_or_else(|| PlanError::missing("Parquet `pruning` read option is required"))
                .and_then(|v| parse_bool(&v))?,
            skip_metadata: options
                .remove("skip_metadata")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `skip_metadata` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            metadata_size_hint: options
                .remove("metadata_size_hint")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `metadata_size_hint` read option is required")
                })
                .and_then(|v| parse_non_zero_usize(&v))?,
            pushdown_filters: options
                .remove("pushdown_filters")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `pushdown_filters` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            reorder_filters: options
                .remove("reorder_filters")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `reorder_filters` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            schema_force_view_types: options
                .remove("schema_force_view_types")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `schema_force_view_types` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            binary_as_string: options
                .remove("binary_as_string")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `binary_as_string` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            coerce_int96: options
                .remove("coerce_int96")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `coerce_int96` read option is required")
                })
                .map(parse_non_empty_string)?
                .ok_or_else(|| {
                    PlanError::internal("Parquet `coerce_int96` read option cannot be empty")
                })?,
            bloom_filter_on_read: options
                .remove("bloom_filter_on_read")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `bloom_filter_on_read` read option is required")
                })
                .and_then(|v| parse_bool(&v))?,
        })
    }
}

impl DataSourceOptions for ParquetReadOptions {
    const SOURCE_CONFIG: &'static str = PARQUET_READ_CONFIG;
    fn try_from_options(options: HashMap<String, String>) -> PlanResult<Self> {
        Self::try_from(options)
    }
}
