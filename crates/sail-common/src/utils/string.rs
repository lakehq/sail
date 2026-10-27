// The characters Spark strips from both ends of a string before parsing it as an integer,
// a date, a timestamp, a boolean, or an interval.
//
// Where the set comes from:
//  Spark has no such literal. It has a single predicate,
//  `org.apache.spark.unsafe.types.UTF8String#isWhitespaceOrISOControl`,
//  which is `Character.isWhitespace(codePoint) || Character.isISOControl(codePoint)`:
//  <https://github.com/apache/spark/blob/v4.2.0/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java#L199>
//
// Every one of its nine call sites passes `getByte(i)` (or an `Array[Byte]` element on the Scala
// side), i.e. a signed byte, so `0x80..=0xFF` sign-extend to negative values and never match.
// The C1 controls `0x80..=0x9F` are therefore NOT in the effective set even though
// `Character.isISOControl` covers them. Evaluating the predicate over all 256 byte values yields
// exactly `0x00..=0x20` (`isISOControl` for `0x00..=0x1F`, `isWhitespace` for `0x20`) plus `0x7F`,
// which is the 34-character set spelled out below.
//
// Every byte of a multi-byte UTF-8 sequence is `>= 0x80`, so no member of this set can ever occur
// inside a multi-byte character. Trimming these characters char-wise (for example with DataFusion
// `btrim`) is therefore exactly equivalent to Spark's byte-wise trim.
//
// Do NOT reach for Rust's `str::trim` or `char::is_whitespace` instead: they strip `U+0085`,
// `U+00A0`, `U+1680`, `U+2000..=U+200A`, `U+2028`, `U+2029`, `U+202F`, `U+205F` and `U+3000`,
// which Spark keeps, and they miss `0x00..=0x08`, `0x0E..=0x1F` and `0x7F`, which Spark strips.
//
// The following sites diverge from Spark and should be migrated to this constant (or to a byte
// predicate derived from it). Each needs its own parity tests, so they are deliberately left for
// follow-up work rather than folded into an unrelated change:
// TODO: `sail-function/src/scalar/json/schema_of_json.rs` — unify the private
//  `is_whitespace_or_iso_control` byte predicate with this constant so the set is defined once.
//
// TODO: `sail-plan/src/resolver/expression/cast.rs` — the catch-all `cast(expr, to)`
//  arm trims nothing, so string to integral casts miss all 34 characters.
//  This is the widest blast radius: every explicit `CAST` plus every implicit coercion.
//
// TODO: `sail-plan/src/function/scalar/conversion.rs` — `int()`, `bigint()`, `smallint()` and `tinyint()` trim nothing.
//
// TODO: `sail-function/src/scalar/datetime/spark_date.rs`, `spark_timestamp.rs` and `spark_date_format.rs` —
//  the temporal parsers trim nothing, so `CAST(' 2024-01-01 ' AS DATE)` diverges from Spark.
//
// TODO: `sail-function/src/scalar/variant/spark_variant_get.rs` — trims nothing.
//
// TODO: `sail-plan/src/function/scalar/window.rs`, `sail-plan/src/resolver/expression/misc.rs` and the generator paths —
//  literal-only positions that trim nothing; low impact.
//
// FIXME: `sail-function/src/scalar/math/spark_bin.rs` —  uses Rust `str::trim`,
//  which both over-trims (`U+00A0`, `U+2028`, `U+3000`, ...) and under-trims (`0x00..=0x08`, `0x0E..=0x1F`, `0x7F`).
//  `sail-function/src/scalar/string/spark_to_number.rs` has the same problem,
//  and also applies the wrong position policy (Spark ignores whitespace anywhere in the digit run).
//
// FIXME: `sail-function/src/scalar/csv/spark_from_csv.rs` trims every field, but Spark's
//  `from_csv` trims nothing. It needs the trim removed, not this constant.
//
// FIXME: `sail-function/src/scalar/math/spark_conv.rs` — `conv()` must use Spark's narrower
//  `UTF8String#trim` (ASCII space only), so it must NOT adopt this constant.
pub const SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS: &str = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20\x7f";

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
