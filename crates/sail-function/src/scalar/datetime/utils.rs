use datafusion::arrow::array::types::{Decimal128Type, Int32Type, Time64MicrosecondType};
use datafusion::arrow::array::{AsArray, Int32Array, PrimitiveArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use lazy_static::lazy_static;
use regex::Regex;

/// [Credit]: <https://github.com/apache/datafusion/blob/d8e4e92daf7f20eef9af6919a8061192f7505043/datafusion/functions/src/datetime/common.rs#L45-L67>
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str, skip: usize) -> Result<()> {
    for (idx, a) in args.iter().skip(skip).enumerate() {
        match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // all good
            }
            _ => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                );
            }
        }
    }

    Ok(())
}

lazy_static! {
    /// Precomputed (regex, replacement) pairs for converting Spark datetime
    /// pattern strings to chrono `strftime` format.
    ///
    /// The `(?P<pre>^|[^%])` prefix in each regex prevents re-matching a
    /// letter that is immediately preceded by `%`. This protects the
    /// canonical chrono directives (`%Y`, `%m`, `%H`, ...) from being
    /// re-substituted as Spark tokens. It does NOT protect letters produced
    /// via the short `%-X` forms (`%-m`, `%-d`, `%-H`, ...), where the
    /// meaningful letter sits after a `-` and remains eligible for later
    /// matching. See the note inside the
    /// `longest_pattern_first_ordering_for_safe_families` test for the
    /// M-family contamination case that arises from this gap.
    ///
    /// IMPORTANT: pattern order is load-bearing. Longer patterns within each
    /// family (e.g., `SSSSSSSSS` before `S`, `MMMM` before `M`) MUST come
    /// first because `replace_all` is applied sequentially per regex.
    /// Do not alphabetize.
    static ref CHRONO_REPLACEMENTS: Vec<(Regex, String)> = {
        let patterns: &[(&str, &str)] = &[
            // Fractional seconds patterns (from nanoseconds to deciseconds)
            ("SSSSSSSSS", "%.9f"), // Nanoseconds
            ("SSSSSSSS", "%.8f"),
            ("SSSSSSS", "%.7f"),
            ("SSSSSS", "%.6f"), // Microseconds
            ("SSSSS", "%.5f"),
            ("SSSS", "%.4f"),
            ("SSS", "%.3f"), // Milliseconds
            ("SS", "%.2f"),  // Centiseconds
            ("S", "%.1f"),   // Deciseconds
            // Year patterns
            ("yyyy", "%Y"),
            ("yyy", "%Y"),
            ("yy", "%y"),
            ("y", "%Y"),
            // Day-of-year pattern
            ("D", "%j"),
            // Month patterns
            ("MMMM", "%B"),
            ("MMM", "%b"),
            ("MM", "%m"),
            ("M", "%-m"),
            ("LLLL", "%B"),
            ("LLL", "%b"),
            ("LL", "%m"),
            ("L", "%-m"),
            // Day-of-month patterns
            ("dd", "%d"),
            ("d", "%-d"),
            // Weekday patterns
            ("EEEE", "%A"),
            ("EEE", "%a"),
            ("E", "%a"),
            // Hour patterns
            ("hh", "%I"), // 12-hour clock (01–12)
            ("h", "%-I"), // 12-hour clock (1–12)
            ("HH", "%H"), // 24-hour clock (00–23)
            ("H", "%-H"), // 24-hour clock (0–23)
            ("KK", "%I"), // 12-hour clock (01–12), but Spark's 'K' is 0–11
            ("K", "%l"),  // 12-hour clock (1–12), space-padded
            // Minute patterns
            ("mm", "%M"),
            ("m", "%-M"),
            // Second patterns
            ("ss", "%S"),
            ("s", "%-S"),
            // AM/PM
            ("a", "%p"),
            // Timezone patterns
            ("XXXXX", "%::z"), // ±HH:MM:SS
            ("XXXX", "%z"),    // ±HHMM
            ("XXX", "%:z"),    // ±HH:MM
            ("XX", "%z"),      // ±HHMM
            ("X", "%z"),       // ±HHMM
            ("xxxxx", "%::z"), // ±HH:MM:SS
            ("xxxx", "%z"),    // ±HHMM
            ("xxx", "%:z"),    // ±HH:MM
            ("xx", "%z"),      // ±HHMM
            ("x", "%z"),       // ±HHMM
            ("ZZZZZ", "%::z"), // ±HH:MM:SS
            ("ZZZZ", "%:z"),   // ±HH:MM
            ("ZZZ", "%z"),     // ±HHMM
            ("ZZ", "%z"),      // ±HHMM
            ("Z", "%z"),       // ±HHMM
            ("zzzz", "%Z"),
            ("zzz", "%Z"),
            ("zz", "%Z"),
            ("z", "%Z"),
            ("OOOO", "%Z"),
            ("OO", "%Z"),
            ("VV", "%Z"),
        ];
        patterns
            .iter()
            .map(|(pattern, replacement)| {
                // All patterns are compile-time `&str` constants, so the regex
                // construction below cannot fail at runtime. `.unwrap()` is
                // safe; an invalid edit to the `patterns` list above would be
                // caught immediately by the unit tests below. `.map_err` adds
                // the offending pattern to the panic message so a future
                // breakage is easy to diagnose.
                let regex_pattern = format!(r"(?P<pre>^|[^%]){}", regex::escape(pattern));
                #[expect(clippy::unwrap_used)]
                let re = Regex::new(&regex_pattern)
                    .map_err(|e| {
                        format!("failed to compile static spark datetime pattern '{pattern}': {e}")
                    })
                    .unwrap();
                // Precompute the FULL replacement string including the
                // `${pre}` capture group reference to avoid per-call allocation.
                let full_replacement = format!("${{pre}}{replacement}");
                (re, full_replacement)
            })
            .collect()
    };
}

pub fn spark_datetime_format_to_chrono_strftime(format: &str) -> Result<String> {
    // TODO: This doesn't cover everything.
    //  https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    //  https://docs.rs/chrono/latest/chrono/format/strftime/index.html#specifiers

    let mut result = format.to_string();
    for (re, replacement) in CHRONO_REPLACEMENTS.iter() {
        // `replace_all` returns `Cow::Borrowed` when nothing matched; in that
        // case the input is unchanged and there is no need to allocate a new
        // `String`. Only reassign `result` when an actual substitution
        // happened (`Cow::Owned`).
        if let std::borrow::Cow::Owned(new_result) = re.replace_all(&result, replacement.as_str()) {
            result = new_result;
        }
    }

    // Fix double-dot issue: chrono's %.Nf already includes a leading dot,
    // so when the Spark format has a literal '.' before S-patterns (e.g., "ss.SSS"),
    // the result would have ".%.Nf" which produces "..NNN". Remove the extra dot.
    result = result.replace(".%.", "%.");

    Ok(result)
}

// Shared array conversion helpers for make_timestamp functions

pub(crate) fn to_time64_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<PrimitiveArray<Time64MicrosecondType>> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Time64MicrosecondType>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(value))) => {
            Ok(PrimitiveArray::<Time64MicrosecondType>::from_value(
                *value,
                number_rows,
            ))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

/// Reads a `Decimal128` column as its raw unscaled `i128` values.
pub(crate) fn to_decimal128_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<PrimitiveArray<Decimal128Type>> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Decimal128Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), _, _)) => {
            Ok(PrimitiveArray::<Decimal128Type>::from_value(
                *value,
                number_rows,
            ))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

pub(crate) fn to_int32_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<Int32Array> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Int32Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
            Ok(Int32Array::from_value(*value, number_rows))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_spark_datetime_format_to_chrono_happy_path() -> Result<()> {
        assert_eq!(
            spark_datetime_format_to_chrono_strftime("yyyy-MM-dd HH:mm:ss")?,
            "%Y-%m-%d %H:%M:%S"
        );
        Ok(())
    }

    #[test]
    fn applies_double_dot_fix_for_subsecond_patterns() -> Result<()> {
        // The trailing `result.replace(".%.", "%.")` corrects for chrono's
        // `%.Nf` already including a leading dot. Zero coverage prior to this
        // test.
        assert_eq!(
            spark_datetime_format_to_chrono_strftime("ss.SSS")?,
            "%S%.3f"
        );
        Ok(())
    }

    #[test]
    fn maps_all_subsecond_patterns_to_correct_precision() -> Result<()> {
        let cases = [
            ("SSSSSSSSS", "%.9f"),
            ("SSSSSSSS", "%.8f"),
            ("SSSSSSS", "%.7f"),
            ("SSSSSS", "%.6f"),
            ("SSSSS", "%.5f"),
            ("SSSS", "%.4f"),
            ("SSS", "%.3f"),
            ("SS", "%.2f"),
            ("S", "%.1f"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                spark_datetime_format_to_chrono_strftime(input)?,
                expected,
                "subsecond pattern `{input}` should map to `{expected}`",
            );
        }
        Ok(())
    }

    #[test]
    fn pre_guard_prevents_cross_pattern_contamination() -> Result<()> {
        // After `MM` substitutes to `%m`, the `m` produced must NOT be
        // re-matched by the Spark minute pattern `m`. This is the real
        // reason the `(?P<pre>^|[^%])` guard exists.
        assert_eq!(spark_datetime_format_to_chrono_strftime("MM mm")?, "%m %M");
        Ok(())
    }

    #[test]
    fn longest_pattern_first_ordering_for_safe_families() -> Result<()> {
        // Locks in the longest-match-first ordering invariant. A future
        // refactor that re-sorts the patterns array alphabetically would
        // fail these assertions immediately.
        //
        // Weekday family (E): outputs `%A`, `%a`, `%a`. The `(?P<pre>^|[^%])`
        // guard prevents the AM/PM `a` pattern from later re-matching the
        // `a` in `%a` (because the preceding `%` does not match `[^%]`).
        assert_eq!(
            spark_datetime_format_to_chrono_strftime("EEEE EEE E")?,
            "%A %a %a"
        );

        // Subsecond family (S, 9 levels): if the shorter pattern matched
        // first, "SSSSSSSSS" would become "%.1f%.8f" or similar garbage.
        assert_eq!(spark_datetime_format_to_chrono_strftime("SSSSS")?, "%.5f");

        // NOTE: M-family ordering (MMMM-MMM-MM-M) is intentionally NOT
        // asserted here. Sail's current output is "%B-%b-%m-%-%-M" because
        // the `m` produced by Spark `M -> %-m` gets re-matched by the later
        // Spark `m -> %-M` minute substitution. The `[^%]` guard does not
        // protect output letters produced via the `%-X` short forms. This
        // is pre-existing behavior, out of scope for this perf refactor;
        // a follow-up could fix the patterns table or replace the
        // sequential-substitution algorithm.
        Ok(())
    }

    /// Reference implementation: byte-for-byte copy of the pre-refactor
    /// function body. Used by the property test below to prove the cached
    /// version produces identical output across a large input corpus.
    ///
    /// Do not "clean up" this function. It is intentionally a frozen
    /// snapshot of the previous behavior.
    fn reference_impl_pre_cache(format: &str) -> Result<String> {
        use datafusion_common::plan_datafusion_err;
        let patterns = [
            ("SSSSSSSSS", "%.9f"),
            ("SSSSSSSS", "%.8f"),
            ("SSSSSSS", "%.7f"),
            ("SSSSSS", "%.6f"),
            ("SSSSS", "%.5f"),
            ("SSSS", "%.4f"),
            ("SSS", "%.3f"),
            ("SS", "%.2f"),
            ("S", "%.1f"),
            ("yyyy", "%Y"),
            ("yyy", "%Y"),
            ("yy", "%y"),
            ("y", "%Y"),
            ("D", "%j"),
            ("MMMM", "%B"),
            ("MMM", "%b"),
            ("MM", "%m"),
            ("M", "%-m"),
            ("LLLL", "%B"),
            ("LLL", "%b"),
            ("LL", "%m"),
            ("L", "%-m"),
            ("dd", "%d"),
            ("d", "%-d"),
            ("EEEE", "%A"),
            ("EEE", "%a"),
            ("E", "%a"),
            ("hh", "%I"),
            ("h", "%-I"),
            ("HH", "%H"),
            ("H", "%-H"),
            ("KK", "%I"),
            ("K", "%l"),
            ("mm", "%M"),
            ("m", "%-M"),
            ("ss", "%S"),
            ("s", "%-S"),
            ("a", "%p"),
            ("XXXXX", "%::z"),
            ("XXXX", "%z"),
            ("XXX", "%:z"),
            ("XX", "%z"),
            ("X", "%z"),
            ("xxxxx", "%::z"),
            ("xxxx", "%z"),
            ("xxx", "%:z"),
            ("xx", "%z"),
            ("x", "%z"),
            ("ZZZZZ", "%::z"),
            ("ZZZZ", "%:z"),
            ("ZZZ", "%z"),
            ("ZZ", "%z"),
            ("Z", "%z"),
            ("zzzz", "%Z"),
            ("zzz", "%Z"),
            ("zz", "%Z"),
            ("z", "%Z"),
            ("OOOO", "%Z"),
            ("OO", "%Z"),
            ("VV", "%Z"),
        ];
        let mut result = format.to_string();
        for &(pattern, replacement) in &patterns {
            let regex_pattern = format!(r"(?P<pre>^|[^%]){}", regex::escape(pattern));
            let re = Regex::new(&regex_pattern).map_err(|e| {
                plan_datafusion_err!("failed to create regex pattern for '{pattern}': {e}")
            })?;
            let replacement_str = format!("${{pre}}{replacement}");
            result = re
                .replace_all(&result, replacement_str.as_str())
                .to_string()
        }
        result = result.replace(".%.", "%.");
        Ok(result)
    }

    /// Regression test against a pinned baseline. Runs both the new cached
    /// implementation and the embedded byte-identical copy of the pre-refactor
    /// function body (`reference_impl_pre_cache`) over a fixed corpus of
    /// Spark format strings and asserts the outputs are byte-identical for
    /// every input. This is not generated/property testing; the corpus is
    /// hand-picked to exercise every supported token and representative
    /// documented shapes, plus the known cross-pattern edge cases.
    #[test]
    fn cached_impl_matches_pre_refactor_baseline() -> Result<()> {
        let long_string = "yyyy-MM-dd HH:mm:ss ".repeat(20);
        let corpus: Vec<&str> = vec![
            // Empty / whitespace
            "",
            " ",
            "   ",
            // Single patterns, every Spark token
            "yyyy",
            "yyy",
            "yy",
            "y",
            "MMMM",
            "MMM",
            "MM",
            "M",
            "LLLL",
            "LLL",
            "LL",
            "L",
            "dd",
            "d",
            "D",
            "EEEE",
            "EEE",
            "E",
            "hh",
            "h",
            "HH",
            "H",
            "KK",
            "K",
            "mm",
            "m",
            "ss",
            "s",
            "a",
            "S",
            "SS",
            "SSS",
            "SSSS",
            "SSSSS",
            "SSSSSS",
            "SSSSSSS",
            "SSSSSSSS",
            "SSSSSSSSS",
            "X",
            "XX",
            "XXX",
            "XXXX",
            "XXXXX",
            "x",
            "xx",
            "xxx",
            "xxxx",
            "xxxxx",
            "Z",
            "ZZ",
            "ZZZ",
            "ZZZZ",
            "ZZZZZ",
            "z",
            "zz",
            "zzz",
            "zzzz",
            "OO",
            "OOOO",
            "VV",
            // Spark documentation examples
            "yyyy-MM-dd",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss",
            "MMMM dd, yyyy",
            "EEE, MMM d, yyyy",
            "yyyy/MM/dd",
            "dd/MM/yyyy",
            "yyyyMMdd",
            "HH:mm:ss",
            "h:mm a",
            "yyyy-MM-dd HH:mm:ss z",
            "yyyy-MM-dd HH:mm:ss XXX",
            "yyyy-MM-dd HH:mm:ss.SSSSSS",
            // Adjacent patterns that exercise the [^%] guard
            "MM mm",
            "MMmm",
            "yy MM dd hh mm ss",
            "MMMMMMM",
            // Repeated patterns
            "yyyy yyyy yyyy",
            "MM-MM-MM-MM",
            "yyyy yyyy yyyy yyyy yyyy yyyy yyyy yyyy yyyy yyyy",
            // Strings already containing chrono-style %
            "%y %M",
            "%Y-%m-%d",
            "%Y MM",
            // Punctuation-heavy
            "yyyy.MM.dd",
            "[yyyy-MM-dd]",
            "(HH:mm:ss)",
            "yyyy_MM_dd_HH_mm_ss",
            // Subsecond combos that exercise the .%. fix
            "ss.SSS",
            "ss.SSSSSS",
            "ss.S",
            "ss.SSSSSSSSS",
            "HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSSSSSSSS",
            // Long string
            &long_string,
            // Spark patterns from real-world contexts
            "M/d/yy",
            "M/d/yyyy",
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "yyyy-MM-dd'T'HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            // Non-pattern letters mixed in
            "qq yyyy",
            "FOO yyyy BAR",
            // Unicode
            "yyyy年MM月dd日",
            "yyyy-MM-dd 🦀",
        ];

        let mut mismatches = Vec::new();
        for input in &corpus {
            let cached = spark_datetime_format_to_chrono_strftime(input)?;
            let reference = reference_impl_pre_cache(input)?;
            if cached != reference {
                mismatches.push(format!(
                    "input={input:?} cached={cached:?} reference={reference:?}"
                ));
            }
        }
        assert!(
            mismatches.is_empty(),
            "cached implementation diverged from reference on {} inputs:\n{}",
            mismatches.len(),
            mismatches.join("\n"),
        );
        Ok(())
    }

    /// One-shot performance measurement, NOT a CI regression test.
    ///
    /// Compares the cached implementation against `reference_impl_pre_cache`
    /// in the same process under identical conditions and prints elapsed
    /// time, per-call time, and the ratio. The numbers it produces are cited
    /// in the PR description; this code is retained so future contributors
    /// can reproduce or re-measure after related changes (e.g., regex crate
    /// upgrades, algorithm consolidation, or input-level memoization).
    ///
    /// Marked `#[ignore]` because the pre-cache reference path is genuinely
    /// slow (~1 minute for the configured workload on a modern laptop) and
    /// would dominate normal test cycles. Run on demand with:
    ///
    /// ```
    /// cargo test -p sail-function --release --lib \
    ///   datetime::utils::tests::bench -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn bench_cached_vs_reference_impl() -> Result<()> {
        use std::time::Instant;

        let formats = [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy",
            "ss.SSS",
            "HH:mm",
            "yyyy-MM-dd HH:mm:ss.SSSSSS",
            "MMMM dd, yyyy",
            "EEE, MMM d, yyyy",
            "yyyyMMdd",
        ];
        let iterations = 10_000;

        // Warm up both paths.
        for f in &formats {
            let _ = spark_datetime_format_to_chrono_strftime(f)?;
            let _ = reference_impl_pre_cache(f)?;
        }

        // Time the cached implementation.
        let start = Instant::now();
        for _ in 0..iterations {
            for f in &formats {
                let _ = spark_datetime_format_to_chrono_strftime(f)?;
            }
        }
        let cached_elapsed = start.elapsed();

        // Time the reference (pre-refactor) implementation.
        let start = Instant::now();
        for _ in 0..iterations {
            for f in &formats {
                let _ = reference_impl_pre_cache(f)?;
            }
        }
        let reference_elapsed = start.elapsed();

        let total_calls = iterations * formats.len();
        let cached_per_call = cached_elapsed.as_nanos() as f64 / total_calls as f64;
        let reference_per_call = reference_elapsed.as_nanos() as f64 / total_calls as f64;
        let speedup = reference_per_call / cached_per_call;

        println!();
        println!("=== datetime format conversion benchmark ===");
        println!(
            "iterations: {iterations} x {} formats = {total_calls} calls each",
            formats.len()
        );
        println!(
            "reference (pre-cache):  {:>10.3?}  ({:.0} ns/call)",
            reference_elapsed, reference_per_call
        );
        println!(
            "cached    (post-cache): {:>10.3?}  ({:.0} ns/call)",
            cached_elapsed, cached_per_call
        );
        println!("speedup: {speedup:.1}x");
        println!();

        Ok(())
    }

    #[test]
    fn cache_handles_concurrent_reads() -> Result<()> {
        // Spawn many threads that hammer the function simultaneously and
        // verify each thread sees correct output on every call. Note: in a
        // multi-test run, other tests in this module typically trigger the
        // `lazy_static!` initialization before this test starts; this is
        // therefore a concurrent-read stress, not a guaranteed first-call
        // race. Either way, a regression in cache thread-safety would
        // surface as wrong output or a panic, which is what we assert
        // against.
        use std::sync::Arc;
        use std::thread;

        let formats: Arc<Vec<(&'static str, &'static str)>> = Arc::new(vec![
            ("yyyy-MM-dd HH:mm:ss", "%Y-%m-%d %H:%M:%S"),
            ("ss.SSS", "%S%.3f"),
            ("HH:mm", "%H:%M"),
            ("yyyy", "%Y"),
        ]);

        let handles: Vec<_> = (0..32)
            .map(|i| {
                let formats = Arc::clone(&formats);
                thread::spawn(move || -> Result<()> {
                    for j in 0..1000 {
                        let (input, expected) = formats[(i + j) % formats.len()];
                        let got = spark_datetime_format_to_chrono_strftime(input)?;
                        assert_eq!(got, expected, "thread {i} iter {j}");
                    }
                    Ok(())
                })
            })
            .collect();

        for handle in handles {
            // Convert thread panic / error into a Result we can `?` so the
            // test fails cleanly without violating workspace `unwrap_used`
            // lint. A panic here would be a real test failure (wrong output
            // under concurrent reads), so we surface it as a DataFusion
            // error.
            handle.join().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "thread panicked during concurrent stress test".to_string(),
                )
            })??;
        }
        Ok(())
    }
}
