// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_string_array;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

/// Jaro similarity below this value gets no prefix boost.
const BOOST_THRESHOLD: f64 = 0.7;
/// Scaling factor applied to the common prefix length.
const SCALING_FACTOR: f64 = 0.1;
/// Maximum common prefix length considered for the boost.
const MAX_PREFIX: usize = 4;

/// Spark `jaro_winkler_similarity(str1, str2)`: similarity in `[0.0, 1.0]`, where `1.0` means
/// the strings are identical.
///
/// The implementation follows Apache Commons Text's `JaroWinklerSimilarity`.
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
    if left == right {
        return 1.0;
    }
    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();

    // Work with the shorter string as `short` and the longer one as `long`.
    let (short, long) = if left.len() > right.len() {
        (&right, &left)
    } else {
        (&left, &right)
    };

    let range = (long.len() / 2).saturating_sub(1);
    let mut short_match: Vec<Option<usize>> = vec![None; short.len()];
    let mut long_matched = vec![false; long.len()];
    let mut matches = 0usize;
    for (i, c) in short.iter().enumerate() {
        let start = i.saturating_sub(range);
        let end = (i + range + 1).min(long.len());
        for j in start..end {
            if !long_matched[j] && long[j] == *c {
                short_match[i] = Some(j);
                long_matched[j] = true;
                matches += 1;
                break;
            }
        }
    }
    if matches == 0 {
        return 0.0;
    }

    let short_chars = short_match
        .iter()
        .enumerate()
        .filter(|(_, m)| m.is_some())
        .map(|(i, _)| short[i]);
    let long_chars = long
        .iter()
        .zip(&long_matched)
        .filter(|(_, matched)| **matched)
        .map(|(c, _)| *c);
    // Half the number of out-of-order matched characters. Commons Text keeps this as a
    // fractional value (it does not round down), so the same is done here.
    let half_transpositions = short_chars.zip(long_chars).filter(|(a, b)| a != b).count();

    let prefix = left
        .iter()
        .zip(right.iter())
        .take(MAX_PREFIX)
        .take_while(|(a, b)| a == b)
        .count();

    let m = matches as f64;
    let jaro = (m / left.len() as f64
        + m / right.len() as f64
        + (m - half_transpositions as f64 / 2.0) / m)
        / 3.0;
    if jaro < BOOST_THRESHOLD {
        jaro
    } else {
        jaro + SCALING_FACTOR * prefix as f64 * (1.0 - jaro)
    }
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

    #[test]
    fn test_unicode_is_compared_by_character() {
        assert_close(compute_jaro_winkler("café", "café"), 1.0);
        assert!(compute_jaro_winkler("café", "cafe") > 0.8);
    }
}
