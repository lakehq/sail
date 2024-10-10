use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use sail_common::config::{ConfigKeyValue, SparkUdfConfig};

use crate::formatter::{DefaultPlanFormatter, PlanFormatter};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimestampType {
    TimestampLtz,
    TimestampNtz,
}

// The generic type parameter is used to work around the issue deriving `PartialEq` for `dyn` trait.
// See also: https://github.com/rust-lang/rust/issues/78808
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanConfig<F: ?Sized = dyn PlanFormatter> {
    /// The time zone of the session.
    pub time_zone: String,
    /// The default timestamp type.
    pub timestamp_type: TimestampType,
    /// The plan formatter.
    pub plan_formatter: Arc<F>,
    // TODO: Revisit how to handle spark_udf_config
    //  https://github.com/lakehq/sail/pull/53#discussion_r1643683600
    pub spark_udf_config: SparkUdfConfig,
    /// The default file format for bounded tables.
    pub default_bounded_table_file_format: String,
    /// The default file format for unbounded tables.
    pub default_unbounded_table_file_format: String,
    /// The default location for managed databases and tables.
    pub default_warehouse_directory: String,
    /// The database name for the global temporary views.
    pub global_temp_database: String,
    pub session_user_id: String,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            time_zone: "UTC".to_string(),
            timestamp_type: TimestampType::TimestampLtz,
            plan_formatter: Arc::new(DefaultPlanFormatter),
            spark_udf_config: SparkUdfConfig {
                timezone: ConfigKeyValue {
                    key: "spark.sql.session.timeZone".to_string(),
                    value: Some("UTC".to_string()),
                },
                pandas_window_bound_types: ConfigKeyValue {
                    key: "pandas_window_bound_types".to_string(),
                    value: None,
                },
                pandas_grouped_map_assign_columns_by_name: ConfigKeyValue {
                    key: "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName"
                        .to_string(),
                    value: None,
                },
                pandas_convert_to_arrow_array_safely: ConfigKeyValue {
                    key: "spark.sql.execution.pandas.convertToArrowArraySafely".to_string(),
                    value: None,
                },
                arrow_max_records_per_batch: ConfigKeyValue {
                    key: "spark.sql.execution.arrow.maxRecordsPerBatch".to_string(),
                    value: None,
                },
            },
            default_bounded_table_file_format: "PARQUET".to_string(),
            default_unbounded_table_file_format: "ARROW".to_string(),
            default_warehouse_directory: "spark-warehouse".to_string(),
            global_temp_database: "global_temp".to_string(),
            session_user_id: "sail".to_string(),
        }
    }
}
