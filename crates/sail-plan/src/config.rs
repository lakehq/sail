use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use sail_python_udf::config::SparkUdfConfig;

use crate::formatter::{DefaultPlanFormatter, PlanFormatter};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum TimestampType {
    TimestampLtz,
    TimestampNtz,
}

// The generic type parameter is used to work around the issue deriving `PartialEq` for `dyn` trait.
// See also: https://github.com/rust-lang/rust/issues/78808
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PlanConfig<F: ?Sized = dyn PlanFormatter> {
    /// The time zone of the session.
    pub time_zone: String,
    /// The default timestamp type.
    pub timestamp_type: TimestampType,
    /// Whether to use large variable types in Arrow.
    pub arrow_use_large_var_types: bool,
    /// The plan formatter.
    pub plan_formatter: Arc<F>,
    /// The Spark UDF configuration.
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
            arrow_use_large_var_types: false,
            plan_formatter: Arc::new(DefaultPlanFormatter),
            spark_udf_config: SparkUdfConfig::default(),
            default_bounded_table_file_format: "PARQUET".to_string(),
            default_unbounded_table_file_format: "ARROW".to_string(),
            default_warehouse_directory: "spark-warehouse".to_string(),
            global_temp_database: "global_temp".to_string(),
            session_user_id: "sail".to_string(),
        }
    }
}
