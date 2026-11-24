// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_string_array;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

/// Scaling factor applied to the common prefix length.
const SCALING_FACTOR: f64 = 0.1;
/// Maximum common prefix length considered for the boost.
const MAX_PREFIX: usize = 4;

/// Spark `jaro_winkler_similarity(str1, str2)`: similarity in `[0.0, 1.0]`, where `1.0` means
/// the strings are identical.
///
/// The implementation follows Spark's `ExpressionImplUtils.jaroWinklerSimilarity` rather than
/// Apache Commons Text: strings are compared as UTF-16 code units, the first string is the one
/// scanned for matches, and the prefix boost is applied to every non-zero similarity.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JaroWinklerSimilarity {
    signature: Signature,
}

impl Default for JaroWinklerSimilarity {
    fn default() -> Self {
        Self::new()
    }
}

impl JaroWinklerSimilarity {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JaroWinklerSimilarity {
    fn name(&self) -> &str {
        "jaro_winkler_similarity"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return exec_err!(
                "`jaro_winkler_similarity` function requires 2 arguments, got {}",
                args.len()
            );
        }
        make_scalar_function(jaro_winkler_similarity, vec![])(&args)
    }
}

fn jaro_winkler_similarity(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = cast(&args[0], &DataType::Utf8)?;
    let right = cast(&args[1], &DataType::Utf8)?;
    let left = as_string_array(&left)?;
    let right = as_string_array(&right)?;
    let result = left
        .iter()
        .zip(right.iter())
        .map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => Some(compute_jaro_winkler(a, b)),
            _ => None,
        })
        .collect::<Float64Array>();
    Ok(Arc::new(result))
}

fn compute_jaro_winkler(left: &str, right: &str) -> f64 {
    // Spark compares UTF-16 code units (`String.charAt`), so a supplementary character counts as
    // two units. Do the same to match its results.
    let s1: Vec<u16> = left.encode_utf16().collect();
    let s2: Vec<u16> = right.encode_utf16().collect();
    let (len1, len2) = (s1.len(), s2.len());

    if len1 == 0 && len2 == 0 {
        return 1.0;
    }
    if len1 == 0 || len2 == 0 {
        return 0.0;
    }

    let match_window = (len1.max(len2) / 2).saturating_sub(1);
    let mut s1_matched = vec![false; len1];
    let mut s2_matched = vec![false; len2];
    let mut matches = 0usize;
    for i in 0..len1 {
        let start = i.saturating_sub(match_window);
        let end = (i + match_window + 1).min(len2);
        for j in start..end {
            if s2_matched[j] || s1[i] != s2[j] {
                continue;
            }
            s1_matched[i] = true;
            s2_matched[j] = true;
            matches += 1;
            break;
        }
    }
    if matches == 0 {
        return 0.0;
    }

    let mut transpositions = 0usize;
    let mut k = 0usize;
    for i in 0..len1 {
        if !s1_matched[i] {
            continue;
        }
        while !s2_matched[k] {
            k += 1;
        }
        if s1[i] != s2[k] {
            transpositions += 1;
        }
        k += 1;
    }

    let m = matches as f64;
    let jaro = (m / len1 as f64 + m / len2 as f64 + (m - transpositions as f64 / 2.0) / m) / 3.0;

    let prefix = s1
        .iter()
        .zip(s2.iter())
        .take(MAX_PREFIX)
        .take_while(|(a, b)| a == b)
        .count();

    // Spark applies the boost to every non-zero similarity (Commons Text only above 0.7).
    jaro + prefix as f64 * SCALING_FACTOR * (1.0 - jaro)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn test_spark_documented_examples() {
        assert_close(compute_jaro_winkler("ABC", "XYZ"), 0.0);
        assert_close(compute_jaro_winkler("MARTHA", "MARHTA"), 0.9611111111111111);
        assert_close(compute_jaro_winkler("kitten", "sitting"), 0.746031746031746);
    }

    /// Expected values and result type recorded from Spark in the gold data
    /// (`tests/gold_data/function/string.json`), run through the UDF itself.
    #[test]
    fn test_udf_matches_spark_gold_data() -> Result<()> {
        use datafusion::arrow::datatypes::Field;
        use datafusion_common::ScalarValue;
        use datafusion_common::config::ConfigOptions;

        let udf = JaroWinklerSimilarity::new();
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Utf8])?,
            DataType::Float64
        );
        for (a, b, expected) in [
            ("ABC", "XYZ", 0.0),
            ("MARTHA", "MARHTA", 0.9611111111111111),
            ("kitten", "sitting", 0.746031746031746),
        ] {
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(a.to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(b.to_string()))),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("a", DataType::Utf8, true)),
                    Arc::new(Field::new("b", DataType::Utf8, true)),
                ],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", DataType::Float64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })?;
            let ColumnarValue::Scalar(ScalarValue::Float64(Some(actual))) = result else {
                return exec_err!("expected a Float64 scalar, got {result:?}");
            };
            // Exact equality: Spark's recorded values are printed with full precision.
            assert_eq!(actual, expected, "jaro_winkler_similarity({a}, {b})");
        }
        Ok(())
    }

    #[test]
    fn test_identical_and_empty() {
        assert_close(compute_jaro_winkler("", ""), 1.0);
        assert_close(compute_jaro_winkler("abc", "abc"), 1.0);
        assert_close(compute_jaro_winkler("abc", ""), 0.0);
        assert_close(compute_jaro_winkler("", "abc"), 0.0);
    }

    #[test]
    fn test_symmetric() {
        for (a, b) in [("DIXON", "DICKSONX"), ("JELLYFISH", "SMELLYFISH")] {
            assert_close(compute_jaro_winkler(a, b), compute_jaro_winkler(b, a));
        }
    }

    /// Values recorded from Spark 4.3.0 (`SELECT jaro_winkler_similarity(...)`). They include
    /// cases where Spark differs from Apache Commons Text: the prefix boost is applied even when
    /// the Jaro score is below 0.7, and strings are compared as UTF-16 code units.
    #[test]
    fn test_matches_spark_4_3_0() {
        for (a, b, expected) in [
            ("ab", "ac", 0.7),
            ("abc", "abd", 0.8222222222222222),
            ("abcd", "abxy", 0.7333333333333333),
            ("DIXON", "DICKSONX", 0.8133333333333332),
            ("DICKSONX", "DIXON", 0.8133333333333332),
            ("JELLYFISH", "SMELLYFISH", 0.8962962962962964),
            ("ab", "ba", 0.0),
            ("crate", "trace", 0.7333333333333334),
            ("dwayne", "duane", 0.8400000000000001),
            ("abcdef", "azcdxf", 0.7999999999999999),
            ("café", "cafe", 0.8833333333333334),
            // A supplementary character is two UTF-16 code units.
            ("\u{1F600}a", "\u{1F600}b", 0.8222222222222222),
            ("\u{1F600}", "\u{1F601}", 0.7),
            ("a\u{1F600}b", "a\u{1F601}b", 0.8666666666666667),
        ] {
            assert_eq!(compute_jaro_winkler(a, b), expected, "({a:?}, {b:?})");
        }
    }
}
