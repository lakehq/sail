/// Escape meta characters in a string.
/// This function is used when displaying schema field names.
/// Reference: org.apache.spark.util.SparkSchemaUtils#escapeMetaCharacters
pub fn escape_meta_characters(s: &str) -> String {
    s.replace('\n', "\\\\n")
        .replace('\r', "\\\\r")
        .replace('\t', "\\\\t")
        .replace('\x07', "\\\\a")
        .replace('\x08', "\\\\b")
        .replace('\x0b', "\\\\v")
        .replace('\x0c', "\\\\f")
}
