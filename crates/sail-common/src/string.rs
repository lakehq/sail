/// Escape meta characters in a string.
/// This function is used when displaying schema field names.
/// Note: Scala's `replaceAll` uses regex-based replacement, but we use simple string replacement.
/// Reference: org.apache.spark.util.SparkSchemaUtils#escapeMetaCharacters
/// https://github.com/apache/spark/blob/fd77ec6a2af21032ec5498775f4cd496f67cf229/common/utils/src/main/scala/org/apache/spark/util/SparkSchemaUtils.scala#L27
pub fn escape_meta_characters(s: &str) -> String {
    s.replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
        .replace('\x07', "\\a")
        .replace('\x08', "\\b")
        .replace('\x0b', "\\v")
        .replace('\x0c', "\\f")
}
