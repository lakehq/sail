use std::sync::LazyLock;

use icu_casemap::{CaseMapper, CaseMapperBorrowed};
use regex::Regex;

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

// OpenJDK 17 uses Unicode 13, so newer characters must keep identity mappings.
#[expect(clippy::expect_used)]
static JDK_17_ASSIGNED_CHARACTER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\p{Age:13.0}$").expect("JDK 17 Unicode age pattern should be valid")
});

/// Compares names using Spark's `String.equalsIgnoreCase` resolver on OpenJDK 17.
/// Java uses simple upper/lower mappings, not Unicode full case folding.
pub fn spark_case_insensitive_name_eq(left: &str, right: &str) -> bool {
    let case_mapper = CaseMapper::new();
    let mut left_chars = left.chars();
    let mut right_chars = right.chars();

    loop {
        match (left_chars.next(), right_chars.next()) {
            (None, None) => return true,
            (Some(left), Some(right)) if java_char_eq_ignore_case(&case_mapper, left, right) => {}
            _ => return false,
        }
    }
}

fn java_char_eq_ignore_case(case_mapper: &CaseMapperBorrowed<'_>, left: char, right: char) -> bool {
    if left == right {
        return true;
    }
    let mut left_buffer = [0; 4];
    let mut right_buffer = [0; 4];
    if !(JDK_17_ASSIGNED_CHARACTER.is_match(left.encode_utf8(&mut left_buffer))
        && JDK_17_ASSIGNED_CHARACTER.is_match(right.encode_utf8(&mut right_buffer)))
    {
        return false;
    }

    let left_upper = case_mapper.simple_uppercase(left);
    let right_upper = case_mapper.simple_uppercase(right);
    left_upper == right_upper
        || case_mapper.simple_lowercase(left_upper) == case_mapper.simple_lowercase(right_upper)
}

#[cfg(test)]
mod tests {
    use super::spark_case_insensitive_name_eq;

    #[test]
    fn spark_case_insensitive_name_eq_matches_jdk_17_unicode_oracle() {
        for (left, right, expected) in [
            ("É", "é", true),
            ("Σ", "ς", true),
            ("I", "ı", true),
            ("İ", "i", true),
            ("ß", "ẞ", true),
            ("K", "K", true),
            ("S", "ſ", true),
            ("ß", "ss", false),
            ("\u{10570}", "\u{10597}", false),
        ] {
            assert_eq!(
                spark_case_insensitive_name_eq(left, right),
                expected,
                "unexpected JDK 17 case-insensitive comparison for {left:?} and {right:?}"
            );
            assert_eq!(
                spark_case_insensitive_name_eq(right, left),
                expected,
                "unexpected JDK 17 case-insensitive comparison for {right:?} and {left:?}"
            );
        }
    }
}
