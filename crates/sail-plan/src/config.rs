use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

pub use sail_common::path::{
    qualify_absolute_table_location, qualify_database_location, qualify_table_location,
    qualify_warehouse_directory,
};
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
    ///
    /// This is always an absolute path or a fully qualified URL.
    /// Relative values from `spark.sql.warehouse.dir` are resolved against
    /// the current working directory at session initialization time,
    /// matching Spark's `SharedState` behavior.
    pub default_warehouse_directory: String,
    pub session_user_id: String,
    pub ansi_mode: bool,
    /// Whether to allow cartesian products (cross joins) without explicit `CROSS JOIN` syntax.
    pub cross_join_enabled: bool,
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
            default_warehouse_directory: qualify_warehouse_directory("spark-warehouse"),
            session_user_id: "".to_string(),
            ansi_mode: false,
            cross_join_enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn qualify_warehouse_directory_resolves_relative_path() {
        let result = qualify_warehouse_directory("spark-warehouse");
        let path = Path::new(&result);
        assert!(
            path.is_absolute(),
            "relative path should be resolved to absolute: {result}"
        );
        assert!(
            result.ends_with("spark-warehouse"),
            "resolved path should end with the relative name: {result}"
        );
    }

    #[test]
    fn qualify_warehouse_directory_preserves_absolute_path() {
        let result = qualify_warehouse_directory("/tmp/my-warehouse");
        assert_eq!(result, "/tmp/my-warehouse");
    }

    #[test]
    fn qualify_warehouse_directory_preserves_url_schemes() {
        assert_eq!(
            qualify_warehouse_directory("s3://bucket/warehouse"),
            "s3://bucket/warehouse"
        );
        assert_eq!(
            qualify_warehouse_directory("file:///tmp/wh"),
            "file:///tmp/wh"
        );
        assert_eq!(
            qualify_warehouse_directory("gs://bucket/path"),
            "gs://bucket/path"
        );
    }

    #[test]
    fn default_plan_config_has_absolute_warehouse_directory() {
        let config = PlanConfig::default();
        let path = Path::new(&config.default_warehouse_directory);
        assert!(
            path.is_absolute(),
            "default warehouse directory should be absolute: {}",
            config.default_warehouse_directory
        );
    }

    #[test]
    fn qualify_table_location_resolves_relative_path_against_database_location() {
        assert_eq!(
            qualify_table_location(
                "nested/table",
                Some("s3://bucket/database"),
                "/tmp/warehouse",
            ),
            "s3://bucket/database/nested/table"
        );
    }

    #[test]
    fn qualify_table_location_resolves_relative_path_against_warehouse() {
        assert_eq!(
            qualify_table_location("nested/table", None, "/tmp/warehouse"),
            "/tmp/warehouse/nested/table"
        );
    }
}
