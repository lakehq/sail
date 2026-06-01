use std::ffi::OsString;
use std::path::{Path, PathBuf};

use url::Url;

fn is_qualified_uri(value: &str) -> bool {
    // Scheme length > 1 excludes single-letter Windows drive prefixes (e.g., `C:\`).
    Url::parse(value)
        .map(|url| url.scheme().len() > 1)
        .unwrap_or(false)
}

fn join_uri_path(base: &str, value: &str) -> String {
    let base = base.trim_end_matches('/');
    let value = value.trim_start_matches('/');
    format!("{base}/{value}")
}

/// Normalizes `..` and `.` components in an absolute local path.
/// Returns the value unchanged if it is not absolute or contains a URL scheme.
pub fn normalize_path(value: &str) -> String {
    if is_qualified_uri(value) {
        return value.to_string();
    }
    let path = Path::new(value);
    if !path.is_absolute() {
        return value.to_string();
    }
    // Collect components, resolving `..` by popping the previous component.
    let mut prefix = None::<OsString>;
    let mut has_root = false;
    let mut normalized = Vec::<OsString>::new();
    for comp in path.components() {
        match comp {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(c) => normalized.push(c.to_os_string()),
            std::path::Component::RootDir => has_root = true,
            std::path::Component::Prefix(p) => prefix = Some(p.as_os_str().to_os_string()),
        }
    }
    let mut result = PathBuf::new();
    if let Some(prefix) = prefix {
        result.push(prefix);
    }
    if has_root {
        result.push(std::path::MAIN_SEPARATOR_STR);
    }
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
/// `..` components are normalized.
pub fn qualify_warehouse_directory(value: &str) -> String {
    if is_qualified_uri(value) {
        return value.to_string();
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return normalize_path(value);
    }
    // Resolve relative path against CWD, matching Spark's SharedState behavior.
    match std::env::current_dir() {
        Ok(cwd) => normalize_path(&cwd.join(path).to_string_lossy()),
        Err(_) => value.to_string(),
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
    if is_qualified_uri(value) || Path::new(value).is_absolute() {
        return normalize_path(value);
    }
    let base = database_location.unwrap_or(warehouse_directory);
    if is_qualified_uri(base) {
        join_uri_path(base, value)
    } else {
        let base = qualify_warehouse_directory(base);
        normalize_path(&Path::new(&base).join(value).to_string_lossy())
    }
}

/// Qualifies only absolute explicit table `LOCATION` values.
///
/// Spark V2 leaves relative table locations to the catalog implementation, but
/// still normalizes absolute filesystem paths and preserves fully qualified URIs.
pub fn qualify_absolute_table_location(value: &str) -> String {
    if is_qualified_uri(value) || Path::new(value).is_absolute() {
        normalize_path(value)
    } else {
        value.to_string()
    }
}

/// Qualifies only absolute explicit database `LOCATION` values.
///
/// Catalog-owned namespace providers should receive relative database
/// locations unchanged, but still benefit from normalization of absolute
/// filesystem paths while preserving fully qualified URIs.
pub fn qualify_absolute_database_location(value: &str) -> String {
    if is_qualified_uri(value) || Path::new(value).is_absolute() {
        normalize_path(value)
    } else {
        value.to_string()
    }
}

/// Qualifies a database `LOCATION` for `CREATE DATABASE`.
///
/// Matches Spark's `makeQualifiedDBObjectPath`:
/// - Absolute paths and URLs are preserved.
/// - Relative paths are joined with the warehouse directory and normalized.
pub fn qualify_database_location(
    location: Option<&str>,
    database_name: &str,
    warehouse_directory: &str,
) -> String {
    if let Some(loc) = location {
        if is_qualified_uri(loc) || Path::new(loc).is_absolute() {
            normalize_path(loc)
        } else {
            let base = qualify_warehouse_directory(warehouse_directory);
            if is_qualified_uri(&base) {
                join_uri_path(&base, loc)
            } else {
                normalize_path(&Path::new(&base).join(loc).to_string_lossy())
            }
        }
    } else {
        let base = qualify_warehouse_directory(warehouse_directory);
        let name_with_db = format!("{}.db", database_name);
        if is_qualified_uri(&base) {
            join_uri_path(&base, &name_with_db)
        } else {
            normalize_path(&Path::new(&base).join(&name_with_db).to_string_lossy())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/a/b/../c"), "/a/c");
        assert_eq!(normalize_path("/a/./b"), "/a/b");
        assert_eq!(normalize_path("s3://bucket/a/../b"), "s3://bucket/a/../b");
    }

    #[cfg(windows)]
    #[test]
    fn test_normalize_path_preserves_windows_prefixes() {
        assert_eq!(normalize_path(r"C:\tmp\a\..\b"), r"C:\tmp\b");
        assert_eq!(
            normalize_path(r"\\server\share\a\..\b"),
            r"\\server\share\b"
        );
    }

    #[test]
    fn test_qualify_warehouse_directory() {
        let cwd = std::env::current_dir();
        assert!(
            cwd.is_ok(),
            "current_dir should be available for path tests"
        );
        let cwd = cwd.unwrap_or_default().to_string_lossy().to_string();
        assert_eq!(
            qualify_warehouse_directory("wh"),
            normalize_path(&format!("{cwd}/wh"))
        );
        assert_eq!(qualify_warehouse_directory("/abs/wh"), "/abs/wh");
        assert_eq!(
            qualify_warehouse_directory("s3://bucket/wh"),
            "s3://bucket/wh"
        );
    }

    #[test]
    fn test_qualify_warehouse_directory_resolves_relative_path() {
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
    fn test_qualify_warehouse_directory_treats_tilde_as_relative_path() {
        let cwd = std::env::current_dir();
        assert!(
            cwd.is_ok(),
            "current_dir should be available for path tests"
        );
        let cwd = cwd.unwrap_or_default().to_string_lossy().to_string();
        assert_eq!(
            qualify_warehouse_directory("~/spark-warehouse"),
            normalize_path(&format!("{cwd}/~/spark-warehouse"))
        );
    }

    #[test]
    fn test_qualify_warehouse_directory_preserves_absolute_path() {
        let result = qualify_warehouse_directory("/tmp/my-warehouse");
        assert_eq!(result, "/tmp/my-warehouse");
    }

    #[test]
    fn test_qualify_warehouse_directory_preserves_url_schemes() {
        assert_eq!(
            qualify_warehouse_directory("s3://bucket/warehouse"),
            "s3://bucket/warehouse"
        );
        assert_eq!(qualify_warehouse_directory("file:/tmp/wh"), "file:/tmp/wh");
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
    fn test_qualify_table_location_resolves_relative_path_against_database_location() {
        assert_eq!(
            qualify_table_location(
                "nested/table",
                Some("s3://bucket/database"),
                "/tmp/warehouse",
            ),
            "s3://bucket/database/nested/table"
        );
        assert_eq!(
            qualify_table_location("child", Some("file:/tmp/database"), "/tmp/warehouse"),
            "file:/tmp/database/child"
        );
        assert_eq!(
            qualify_table_location("child", Some("file:///tmp/database"), "/tmp/warehouse"),
            "file:///tmp/database/child"
        );
    }

    #[test]
    fn test_qualify_table_location_resolves_relative_path_against_warehouse() {
        assert_eq!(
            qualify_table_location("nested/table", None, "/tmp/warehouse"),
            "/tmp/warehouse/nested/table"
        );
    }

    #[test]
    fn test_qualify_table_location_treats_tilde_as_relative_path() {
        assert_eq!(
            qualify_table_location("~/table", Some("/tmp/database"), "/tmp/warehouse"),
            "/tmp/database/~/table"
        );
    }

    #[test]
    fn test_qualify_absolute_table_location_preserves_relative_path() {
        assert_eq!(
            qualify_absolute_table_location("nested/table"),
            "nested/table"
        );
        assert_eq!(qualify_absolute_table_location("~/table"), "~/table");
    }

    #[test]
    fn test_qualify_absolute_table_location_normalizes_absolute_path() {
        assert_eq!(
            qualify_absolute_table_location("/tmp/database/../table"),
            "/tmp/table"
        );
        assert_eq!(
            qualify_absolute_table_location("s3://bucket/database/../table"),
            "s3://bucket/database/../table"
        );
    }

    #[test]
    fn test_qualify_absolute_database_location_preserves_relative_path() {
        assert_eq!(
            qualify_absolute_database_location("nested/database"),
            "nested/database"
        );
        assert_eq!(qualify_absolute_database_location("~/db"), "~/db");
    }

    #[test]
    fn test_qualify_absolute_database_location_normalizes_absolute_path() {
        assert_eq!(
            qualify_absolute_database_location("/tmp/root/../database"),
            "/tmp/database"
        );
        assert_eq!(
            qualify_absolute_database_location("s3://bucket/root/../database"),
            "s3://bucket/root/../database"
        );
    }

    #[test]
    fn test_qualify_database_location_resolves_relative_path_against_file_uri_warehouse() {
        assert_eq!(
            qualify_database_location(Some("relative/db"), "my_db", "file:/tmp/warehouse"),
            "file:/tmp/warehouse/relative/db"
        );
    }

    #[test]
    fn test_qualify_database_location_treats_tilde_as_relative_path() {
        assert_eq!(
            qualify_database_location(Some("~/db"), "my_db", "/tmp/warehouse"),
            "/tmp/warehouse/~/db"
        );
    }

    #[test]
    fn test_qualify_database_location_uses_db_suffix_for_default_location() {
        assert_eq!(
            qualify_database_location(None, "fallback_db", "/tmp/warehouse"),
            "/tmp/warehouse/fallback_db.db"
        );
    }

    #[test]
    fn test_qualify_table_location_preserves_special_characters() {
        assert_eq!(
            qualify_table_location(
                "my@table",
                Some("/tmp/warehouse/fallback_db.db"),
                "/tmp/warehouse"
            ),
            "/tmp/warehouse/fallback_db.db/my@table"
        );
    }

    #[test]
    fn test_path_qualification_combinations() {
        let wh_options = ["/wh", "s3://wh", "wh"];
        let db_options = [Some("/db"), Some("s3://db"), Some("db"), None];
        let tbl_options = ["/tbl", "s3://tbl", "tbl"];

        for wh in wh_options {
            for db in db_options {
                let _qualified_db = qualify_database_location(db, "my_db", wh);
                for tbl in tbl_options {
                    let qualified_tbl = qualify_table_location(tbl, db, wh);

                    assert!(
                        !qualified_tbl.is_empty(),
                        "Table location should not be empty"
                    );

                    if tbl.starts_with('/') || is_qualified_uri(tbl) {
                        assert!(qualified_tbl.contains("tbl"));
                    } else {
                        let expected_base = db.unwrap_or(wh);
                        if is_qualified_uri(expected_base) {
                            assert!(qualified_tbl.starts_with(expected_base.trim_end_matches('/')));
                        }
                    }
                }
            }
        }
    }
}
