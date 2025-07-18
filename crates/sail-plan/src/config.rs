use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::Result;
use sail_catalog::manager::CatalogManagerConfig;
use sail_python_udf::config::PySparkUdfConfig;

use crate::error::PlanResult;
use crate::formatter::{DefaultPlanFormatter, PlanFormatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum DefaultTimestampType {
    TimestampLtz,
    TimestampNtz,
}

// The generic type parameter is used to work around the issue deriving `PartialEq` for `dyn` trait.
// See also: https://github.com/rust-lang/rust/issues/78808
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PlanConfig<F: ?Sized = dyn PlanFormatter> {
    /// The time zone of the session.
    pub session_timezone: Arc<str>,
    /// The default timestamp type.
    pub default_timestamp_type: DefaultTimestampType,
    /// Whether to use large variable types in Arrow.
    pub arrow_use_large_var_types: bool,
    /// The plan formatter.
    pub plan_formatter: Arc<F>,
    /// The Spark UDF configuration.
    pub pyspark_udf_config: Arc<PySparkUdfConfig>,
    /// The default file format for bounded tables.
    pub default_bounded_table_file_format: String,
    /// The default file format for unbounded tables.
    pub default_unbounded_table_file_format: String,
    /// The default location for managed databases and tables.
    pub default_warehouse_directory: String,
    /// The database name for the global temporary views.
    pub global_temp_database: String,
    pub session_user_id: String,
    pub ansi_mode: bool,
}

impl PlanConfig {
    pub fn new() -> PlanResult<Self> {
        Ok(Self {
            pyspark_udf_config: Arc::new(PySparkUdfConfig::default()),
            ..Default::default()
        })
    }
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            session_timezone: Arc::from("UTC"),
            default_timestamp_type: DefaultTimestampType::TimestampLtz,
            arrow_use_large_var_types: false,
            plan_formatter: Arc::new(DefaultPlanFormatter),
            pyspark_udf_config: Arc::new(PySparkUdfConfig::default()),
            default_bounded_table_file_format: "PARQUET".to_string(),
            default_unbounded_table_file_format: "ARROW".to_string(),
            default_warehouse_directory: "spark-warehouse".to_string(),
            global_temp_database: "global_temp".to_string(),
            session_user_id: "sail".to_string(),
            ansi_mode: false,
        }
    }
}

impl CatalogManagerConfig for PlanConfig {
    fn data_type_to_simple_string(&self, data_type: &DataType) -> Result<String> {
        self.plan_formatter.data_type_to_simple_string(data_type)
    }

    fn global_temporary_database(&self) -> &str {
        self.global_temp_database.as_str()
    }
}
