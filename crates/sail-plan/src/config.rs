use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use sail_python_udf::config::PySparkUdfConfig;

use crate::error::PlanResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum DefaultTimestampType {
    TimestampLtz,
    TimestampNtz,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PlanConfig {
    /// The time zone of the session.
    pub session_timezone: Arc<str>,
    /// The default timestamp type.
    pub default_timestamp_type: DefaultTimestampType,
    /// Whether to use large variable types in Arrow.
    pub arrow_use_large_var_types: bool,
    /// The Spark UDF configuration.
    pub pyspark_udf_config: Arc<PySparkUdfConfig>,
    /// The default table file format.
    pub default_table_file_format: String,
    /// The default location for managed databases and tables.
    pub default_warehouse_directory: String,
    pub session_user_id: String,
    pub ansi_mode: bool,
    /// Whether to allow cartesian products (cross joins) without explicit `CROSS JOIN` syntax.
    pub cross_join_enabled: bool,
    /// Whether identifiers (e.g. column names) are matched case-sensitively.
    /// Spark defaults to case-insensitive matching (`spark.sql.caseSensitive=false`).
    pub case_sensitive: bool,
    /// The maximum number of distinct values collected for a pivot without an explicit
    /// value list (`spark.sql.pivotMaxValues`, default 10000). Exceeding it is an error.
    pub pivot_max_values: usize,
    /// Whether a table-valued function may receive more than one `TABLE (...)` argument
    /// (`spark.sql.tvf.allowMultipleTableArguments.enabled`, default false). Multiple table
    /// arguments produce the cartesian product of their rows.
    pub tvf_allow_multiple_table_arguments: bool,
    /// Whether a decimal result whose precision exceeds 38 keeps its integer digits by
    /// reducing the scale (`spark.sql.decimalOperations.allowPrecisionLoss`, default true).
    /// When false, the precision and scale are merely clamped, so an unrepresentable result
    /// becomes NULL instead of being rounded.
    pub decimal_operations_allow_precision_loss: bool,
    /// Whether an integer literal combined with a decimal narrows to the minimal decimal that
    /// holds its *value* (`spark.sql.legacy.literal.pickMinimumPrecision`, default true).
    /// When false, the literal widens to its *type*-based decimal, as any integer column does.
    pub literal_pick_minimum_precision: bool,
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
            pyspark_udf_config: Arc::new(PySparkUdfConfig::default()),
            default_table_file_format: "PARQUET".to_string(),
            default_warehouse_directory: "spark-warehouse".to_string(),
            session_user_id: "".to_string(),
            ansi_mode: true,
            cross_join_enabled: true,
            case_sensitive: false,
            pivot_max_values: 10000,
            tvf_allow_multiple_table_arguments: false,
            decimal_operations_allow_precision_loss: true,
            literal_pick_minimum_precision: true,
        }
    }
}
