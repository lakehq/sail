use std::fmt::Debug;
use std::sync::Arc;

use sail_common::spec;
use sail_python_udf::config::PySparkUdfConfig;

use crate::error::{PlanError, PlanResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum DefaultTimestampType {
    TimestampLtz,
    TimestampNtz,
}

#[derive(Debug, Clone)]
pub struct CachedLocalRelationData {
    pub data: Option<Vec<u8>>,
    pub schema: Option<spec::Schema>,
}

pub trait LocalRelationCache: Debug + Send + Sync {
    fn read_cached_local_relation(&self, hash: &str) -> PlanResult<CachedLocalRelationData>;

    fn read_chunked_cached_local_relation_data(&self, hash: &str) -> PlanResult<Vec<u8>>;

    fn read_chunked_cached_local_relation_schema(&self, hash: &str) -> PlanResult<spec::Schema>;
}

#[derive(Debug)]
struct EmptyLocalRelationCache;

impl LocalRelationCache for EmptyLocalRelationCache {
    fn read_cached_local_relation(&self, hash: &str) -> PlanResult<CachedLocalRelationData> {
        Err(PlanError::invalid(format!(
            "cached local relation not found: {hash}"
        )))
    }

    fn read_chunked_cached_local_relation_data(&self, hash: &str) -> PlanResult<Vec<u8>> {
        Err(PlanError::invalid(format!(
            "chunked cached local relation data block not found: {hash}"
        )))
    }

    fn read_chunked_cached_local_relation_schema(&self, hash: &str) -> PlanResult<spec::Schema> {
        Err(PlanError::invalid(format!(
            "chunked cached local relation schema block not found: {hash}"
        )))
    }
}

#[derive(Debug, Clone)]
pub struct PlanConfig {
    /// The time zone of the session.
    pub session_timezone: Arc<str>,
    /// The default timestamp type.
    pub default_timestamp_type: DefaultTimestampType,
    /// Whether to use large variable types in Arrow.
    pub arrow_use_large_var_types: bool,
    /// The Spark UDF configuration.
    pub pyspark_udf_config: Arc<PySparkUdfConfig>,
    /// Session-local cache for Spark Connect local relation artifacts.
    pub local_relation_cache: Arc<dyn LocalRelationCache>,
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
            local_relation_cache: Arc::new(EmptyLocalRelationCache),
            default_table_file_format: "PARQUET".to_string(),
            default_warehouse_directory: "spark-warehouse".to_string(),
            session_user_id: "".to_string(),
            ansi_mode: true,
            cross_join_enabled: true,
            case_sensitive: false,
            pivot_max_values: 10000,
        }
    }
}
