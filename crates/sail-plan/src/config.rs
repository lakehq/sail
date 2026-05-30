use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;
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

/// Expands a leading `~` to the value of the `HOME` environment variable.
/// Returns the value unchanged if it does not start with `~` or if `HOME` is not set.
fn expand_tilde(value: &str) -> String {
    if let Some(rest) = value.strip_prefix('~') {
        if rest.is_empty() || rest.starts_with('/') {
            if let Ok(home) = std::env::var("HOME") {
                return format!("{home}{rest}");
            }
        }
    }
    value.to_string()
}

/// Normalizes `..` and `.` components in an absolute local path.
/// Returns the value unchanged if it is not absolute or contains a URL scheme.
fn normalize_path(value: &str) -> String {
    if value.contains("://") {
        return value.to_string();
    }
    let path = Path::new(value);
    if !path.is_absolute() {
        return value.to_string();
    }
    // Collect components, resolving `..` by popping the previous component.
    let mut normalized = Vec::new();
    for comp in path.components() {
        match comp {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(c) => normalized.push(c),
            std::path::Component::RootDir => {}
            std::path::Component::Prefix(p) => normalized.push(p.as_os_str()),
        }
    }
    let mut result = std::path::PathBuf::new();
    result.push("/");
    for comp in normalized {
        result.push(comp);
    }
    result.to_string_lossy().to_string()
}

/// Qualifies a warehouse directory path to an absolute form.
///
/// If the value is already a fully qualified URL (e.g., `s3://`, `file://`)
/// or an absolute filesystem path, it is returned unchanged. Otherwise,
/// the relative path is resolved against the current working directory.
/// Leading `~` is expanded to `$HOME` and `..` components are normalized.
///
/// This mirrors Spark's `SharedState` initialization, which qualifies
/// the `spark.sql.warehouse.dir` value via the Hadoop FileSystem API
/// at `SparkSession` creation time.
pub fn qualify_warehouse_directory(value: &str) -> String {
    let value = expand_tilde(value);
    if value.contains("://") {
        return value;
    }
    let path = Path::new(&value);
    if path.is_absolute() {
        return normalize_path(&value);
    }
    // Resolve relative path against CWD, matching Spark's SharedState behavior.
    match std::env::current_dir() {
        Ok(cwd) => normalize_path(&cwd.join(path).to_string_lossy()),
        Err(_) => value,
    }
}

/// Qualifies an explicit table `LOCATION`.
///
/// Spark qualifies relative table locations against the database location when
/// present, and otherwise against the warehouse directory. Absolute filesystem
/// paths and fully qualified URLs are preserved. `..` components are normalized.
pub fn qualify_table_location(
    value: &str,
    database_location: Option<&str>,
    warehouse_directory: &str,
) -> String {
    let value = expand_tilde(value);
    if value.contains("://") || Path::new(&value).is_absolute() {
        return normalize_path(&value);
    }
    let base = database_location.unwrap_or(warehouse_directory);
    if base.contains("://") {
        let base = base.trim_end_matches('/');
        let value = value.trim_start_matches('/');
        format!("{base}/{value}")
    } else {
        let base = qualify_warehouse_directory(base);
        normalize_path(&Path::new(&base).join(&value).to_string_lossy())
    }
}

/// Qualifies a database `LOCATION` for `CREATE DATABASE`.
///
/// Matches Spark's `makeQualifiedDBObjectPath`:
/// - Absolute paths and URLs are preserved.
/// - Relative paths are joined with the warehouse directory and normalized.
pub fn qualify_database_location(
    location: Option<&str>,
    warehouse_directory: &str,
) -> Option<String> {
    location.map(|loc| {
        let loc = expand_tilde(loc);
        if loc.contains("://") || Path::new(&loc).is_absolute() {
            normalize_path(&loc)
        } else {
            let base = qualify_warehouse_directory(warehouse_directory);
            normalize_path(&Path::new(&base).join(&loc).to_string_lossy())
        }
    })
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
