/// The ASCII bytes Spark's `UTF8String.isWhitespaceOrISOControl` trims from parsed values.
/// Spark applies the Java predicate to signed UTF-8 bytes, so the effective set is `0x00..=0x20`
/// plus `0x7f`; Rust's Unicode whitespace predicates are not equivalent.
pub const SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS: &str = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20\x7f";

pub const fn is_spark_whitespace_or_iso_control(byte: u8) -> bool {
    byte <= b' ' || byte == 0x7f
}

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
