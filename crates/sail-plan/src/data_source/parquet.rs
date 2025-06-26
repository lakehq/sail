use std::collections::HashMap;
use std::convert::TryFrom;

use sail_common::config::{PARQUET_READ_CONFIG, PARQUET_WRITE_CONFIG};
use serde::Deserialize;

use crate::data_source::{
    parse_bool, parse_f64, parse_non_empty_string, parse_non_empty_usize, parse_u64, parse_usize,
    DataSourceOptions,
};
use crate::error::{PlanError, PlanResult};

// TODO:
//  1. Spark global: https://spark.apache.org/docs/4.0.0/sql-data-sources-parquet.html#configuration
//  2. DataFusion `column_specific_options` and  `key_value_metadata`

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
                .and_then(|v| parse_non_empty_usize(&v))?,
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

/// Datasource Options that control the writing of Parquet files.
#[derive(Debug, Deserialize)]
// Serde bypasses any individual field deserializers and instead uses the `TryFrom` implementation.
#[serde(try_from = "HashMap<String, String>")]
pub struct ParquetWriteOptions {
    pub data_page_size_limit: usize,
    pub write_batch_size: usize,
    pub writer_version: String,
    pub skip_arrow_metadata: bool,
    pub compression: String,
    pub dictionary_enabled: bool,
    pub dictionary_page_size_limit: usize,
    pub statistics_enabled: String,
    pub max_row_group_size: usize,
    pub column_index_truncate_length: Option<usize>,
    pub statistics_truncate_length: Option<usize>,
    pub data_page_row_count_limit: usize,
    pub encoding: Option<String>,
    pub bloom_filter_on_write: bool,
    pub bloom_filter_fpp: f64,
    pub bloom_filter_ndv: u64,
    pub allow_single_file_parallelism: bool,
    pub maximum_parallel_row_group_writers: usize,
    pub maximum_buffered_record_batches_per_stream: usize,
}

impl TryFrom<HashMap<String, String>> for ParquetWriteOptions {
    type Error = PlanError;

    // The options HashMap should already contain all supported keys with their resolved values
    fn try_from(mut options: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(ParquetWriteOptions {
            data_page_size_limit: options
                .remove("data_page_size_limit")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `data_page_size_limit` write option is required")
                })
                .and_then(|v| parse_usize(&v))?,
            write_batch_size: options
                .remove("write_batch_size")
                .ok_or_else(|| PlanError::missing("Parquet `write_batch_size` write option is required"))
                .and_then(|v| parse_usize(&v))?,
            writer_version: options
                .remove("writer_version")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `writer_version` write option is required")
                })
                .map(parse_non_empty_string)?
                .ok_or_else(|| {
                    PlanError::internal("Parquet `writer_version` write option cannot be empty")
                })?,
            skip_arrow_metadata: options
                .remove("skip_arrow_metadata")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `skip_arrow_metadata` write option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            compression: options
                .remove("compression")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `compression` write option is required")
                })
                .map(parse_non_empty_string)?
                .ok_or_else(|| {
                    PlanError::internal("Parquet `compression` write option cannot be empty")
                })?,
            dictionary_enabled: options
                .remove("dictionary_enabled")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `dictionary_enabled` write option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            dictionary_page_size_limit: options
                .remove("dictionary_page_size_limit")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `dictionary_page_size_limit` write option is required")
                })
                .and_then(|v| parse_usize(&v))?,
            statistics_enabled: options
                .remove("statistics_enabled")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `statistics_enabled` write option is required")
                })
                .map(parse_non_empty_string)?
                .ok_or_else(|| {
                    PlanError::internal("Parquet `statistics_enabled` write option cannot be empty")
                })?,
            max_row_group_size: options
                .remove("max_row_group_size")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `max_row_group_size` write option is required")
                })
                .and_then(|v| parse_usize(&v))?,
            column_index_truncate_length: options
                .remove("column_index_truncate_length")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `column_index_truncate_length` write option is required")
                })
                .and_then(|v| parse_non_empty_usize(&v))?,
            statistics_truncate_length: options
                .remove("statistics_truncate_length")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `statistics_truncate_length` write option is required")
                })
                .and_then(|v| parse_non_empty_usize(&v))?,
            data_page_row_count_limit: options
                .remove("data_page_row_count_limit")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `data_page_row_count_limit` write option is required")
                })
                .and_then(|v| parse_usize(&v))?,
            encoding: options
                .remove("encoding")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `encoding` write option is required")
                })
                .map(parse_non_empty_string)?,
            bloom_filter_on_write: options
                .remove("bloom_filter_on_write")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `bloom_filter_on_write` write option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            bloom_filter_fpp: options
                .remove("bloom_filter_fpp")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `bloom_filter_fpp` write option is required")
                })
                .and_then(|v| parse_f64(&v))?,
            bloom_filter_ndv: options
                .remove("bloom_filter_ndv")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `bloom_filter_ndv` write option is required")
                })
                .and_then(|v| parse_u64(&v))?,
            allow_single_file_parallelism: options
                .remove("allow_single_file_parallelism")
                .ok_or_else(|| {
                    PlanError::internal("Parquet `allow_single_file_parallelism` write option is required")
                })
                .and_then(|v| parse_bool(&v))?,
            maximum_parallel_row_group_writers: options
                .remove("maximum_parallel_row_group_writers")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `maximum_parallel_row_group_writers` write option is required")
                })
                .and_then(|v| parse_usize(&v))?,
            maximum_buffered_record_batches_per_stream: options
                .remove("maximum_buffered_record_batches_per_stream")
                .ok_or_else(|| {
                    PlanError::missing("Parquet `maximum_buffered_record_batches_per_stream` write option is required")
                })
                .and_then(|v| parse_usize(&v))?,
        })
    }
}

impl DataSourceOptions for ParquetWriteOptions {
    const SOURCE_CONFIG: &'static str = PARQUET_WRITE_CONFIG;
    fn try_from_options(options: HashMap<String, String>) -> PlanResult<Self> {
        Self::try_from(options)
    }
}
