mod schema_of_csv;
pub mod spark_from_csv;

pub use schema_of_csv::SparkSchemaOfCsv;

/// Converts a Spark/Java-style timestamp format string (e.g., `"yyyy-MM-dd HH:mm:ss"`)
/// into a format string compatible with the `chrono` crate (e.g., `"%Y-%m-%d %H:%M:%S"`).
///
/// Note: `MM` (Java month) maps to `%m` (chrono month) and
/// `mm` (Java minute) maps to `%M` (chrono minute). The ordering of replacements
/// intentionally processes `"MM"` before `"mm"` so the two patterns do not overlap.
pub(super) fn convert_java_timestamp_format(fmt: &str) -> String {
    fmt.replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
}
