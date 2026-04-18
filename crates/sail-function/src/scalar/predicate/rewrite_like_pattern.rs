use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringBuilder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignatureClass,
    Volatility,
};

use crate::functions_utils::make_scalar_function;

/// Rewrites a SQL LIKE pattern so that `\` becomes the effective escape
/// character, translating any user-supplied escape character into
/// backslash-escape form.
///
/// Arrow's native LIKE kernel hard-codes `\` as the escape character. To
/// support arbitrary escape characters (the `ESCAPE` clause / third argument
/// of `like` / `ilike`), we pre-translate the pattern and hand Arrow a
/// pattern it can consume directly.
///
/// Arguments:
///   0. pattern: the LIKE pattern string
///   1. escape: a scalar single-character string
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RewriteLikePatternFunc {
    signature: Signature,
}

impl Default for RewriteLikePatternFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RewriteLikePatternFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RewriteLikePatternFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rewrite_like_pattern"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
            other => exec_err!("Unsupported data type {other:?} for function rewrite_like_pattern"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let escape = extract_escape_char(&args.args[1])?;
        let pattern_arg = args.args[0].clone();

        match pattern_arg.data_type() {
            DataType::Utf8 => {
                make_scalar_function(move |a: &[ArrayRef]| rewrite::<i32>(a, escape), vec![])(&[
                    pattern_arg,
                ])
            }
            DataType::LargeUtf8 => {
                make_scalar_function(move |a: &[ArrayRef]| rewrite::<i64>(a, escape), vec![])(&[
                    pattern_arg,
                ])
            }
            DataType::Utf8View => {
                make_scalar_function(move |a: &[ArrayRef]| rewrite_view(a, escape), vec![])(&[
                    pattern_arg,
                ])
            }
            other => exec_err!("Unsupported data type {other:?} for function rewrite_like_pattern"),
        }
    }
}

fn extract_escape_char(arg: &ColumnarValue) -> Result<char> {
    let ColumnarValue::Scalar(scalar) = arg else {
        return exec_err!("escape character for rewrite_like_pattern must be a scalar literal");
    };
    let s: &str = match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.as_str(),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            return exec_err!("escape character for rewrite_like_pattern must not be null");
        }
        other => {
            return exec_err!(
                "escape character for rewrite_like_pattern must be a string, got {other:?}"
            );
        }
    };
    let mut chars = s.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => exec_err!("escape character for rewrite_like_pattern must be a single character"),
    }
}

fn rewrite<T: OffsetSizeTrait>(args: &[ArrayRef], escape: char) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    let mut builder = GenericStringBuilder::<T>::new();
    let mut buffer = String::new();

    for string in string_array.iter() {
        match string {
            Some(string) => {
                buffer.clear();
                rewrite_like_into_string(&mut buffer, string, escape);
                builder.append_value(&buffer);
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn rewrite_view(args: &[ArrayRef], escape: char) -> Result<ArrayRef> {
    let string_array = as_string_view_array(&args[0])?;

    let mut builder = GenericStringBuilder::<i32>::new();
    let mut buffer = String::new();

    for string in string_array.iter() {
        match string {
            Some(string) => {
                buffer.clear();
                rewrite_like_into_string(&mut buffer, string, escape);
                builder.append_value(&buffer);
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Walks the pattern once, translating user escape sequences so the output
/// uses `\` as the escape character (what Arrow's LIKE kernel expects).
///
/// Rules (escape char `e`, `e != '\\'`):
///   - `\`          → `\\`        (existing backslash kept as a literal)
///   - `e%`         → `\%`
///   - `e_`         → `\_`
///   - `ee`         → `\e`
///   - `e<other>`   → `e<other>`  (pass-through; Spark would error here)
///   - trailing `e` → `e`         (pass-through)
#[inline]
fn rewrite_like_into_string(buffer: &mut String, pattern: &str, escape: char) {
    if escape == '\\' {
        buffer.push_str(pattern);
        return;
    }
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            buffer.push_str("\\\\");
        } else if c == escape {
            match chars.next() {
                Some(next) if next == '%' || next == '_' || next == escape => {
                    buffer.push('\\');
                    buffer.push(next);
                }
                Some(other) => {
                    // Invalid escape sequence: pass both chars through as
                    // literals. If `other` is `\`, double it so Arrow's
                    // LIKE kernel doesn't interpret it as the start of
                    // another escape sequence.
                    buffer.push(c);
                    if other == '\\' {
                        buffer.push_str("\\\\");
                    } else {
                        buffer.push(other);
                    }
                }
                None => {
                    buffer.push(c);
                }
            }
        } else {
            buffer.push(c);
        }
    }
}

// Can't really easily test with Python or BDD tests because this UDF isn't surfaced externally.
#[cfg(test)]
mod tests {
    use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion_common::cast::{as_large_string_array, as_string_array};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{exec_datafusion_err, internal_err};

    use super::*;

    fn rewrite_to_string(pattern: &str, escape: char) -> String {
        let mut buffer = String::new();
        rewrite_like_into_string(&mut buffer, pattern, escape);
        buffer
    }

    #[test]
    fn backslash_escape_is_identity() {
        assert_eq!(rewrite_to_string("a_b%c", '\\'), "a_b%c");
        assert_eq!(rewrite_to_string(r"\_", '\\'), r"\_");
    }

    #[test]
    fn translates_escape_before_wildcards() {
        assert_eq!(rewrite_to_string("!%", '!'), r"\%");
        assert_eq!(rewrite_to_string("!_", '!'), r"\_");
    }

    #[test]
    fn translates_escaped_escape_char() {
        // `!!` represents a literal `!`; Arrow-LIKE form is `\!`.
        assert_eq!(rewrite_to_string("!!", '!'), r"\!");
    }

    #[test]
    fn doubles_existing_backslash() {
        // A literal `\` in the input must reach Arrow as `\\`.
        assert_eq!(rewrite_to_string(r"a\b", '!'), r"a\\b");
    }

    #[test]
    fn handles_mixed_pattern() {
        // `a!_\b` with escape `!` → `a\_\\b`.
        assert_eq!(rewrite_to_string(r"a!_\b", '!'), r"a\_\\b");
    }

    #[test]
    fn does_not_reconsume_prior_match() {
        // `!!%` should produce `\!%`: after consuming `!!` the scanner must
        // advance past both chars so the lone `%` remains a wildcard.
        assert_eq!(rewrite_to_string("!!%", '!'), r"\!%");
        // `!!!%` → `\!\%`.
        assert_eq!(rewrite_to_string("!!!%", '!'), r"\!\%");
    }

    #[test]
    fn passes_through_invalid_escape_sequence() {
        // `!b` (escape before a non-special char): pass through.
        assert_eq!(rewrite_to_string("a!bc", '!'), "a!bc");
    }

    #[test]
    fn invalid_escape_pass_through_doubles_backslash() {
        // `!\%` with escape `!`: `!\` is invalid escape (pass-through);
        // the raw `\` must be doubled so Arrow doesn't treat the following
        // `%` as escaped. Expected: `!\\%` → Arrow reads `!` literal,
        // `\\` literal backslash, `%` wildcard. Matches `!\<anything>`.
        assert_eq!(rewrite_to_string(r"!\%", '!'), r"!\\%");
        // Same principle with `_`.
        assert_eq!(rewrite_to_string(r"!\_", '!'), r"!\\_");
        // Trailing `!\` pair: must not leave a raw trailing `\`.
        assert_eq!(rewrite_to_string(r"a!\", '!'), r"a!\\");
    }

    #[test]
    fn trailing_lone_escape_is_literal() {
        assert_eq!(rewrite_to_string("abc!", '!'), "abc!");
    }

    #[test]
    fn preserves_plain_wildcards() {
        assert_eq!(rewrite_to_string("a%b_c", '!'), "a%b_c");
    }

    #[test]
    fn handles_non_ascii_escape() {
        // Multibyte escape characters work too. Input `ä%ä_äää` =
        // `ä%` → `\%`, `ä_` → `\_`, `ää` → `\ä`, trailing lone `ä` → `ä`.
        assert_eq!(rewrite_to_string("ä%ä_äää", 'ä'), r"\%\_\ää");
    }

    fn invoke(args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
        let number_rows = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(a) => Some(a.len()),
                _ => None,
            })
            .unwrap_or(1);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| Arc::new(Field::new(format!("arg_{idx}"), arg.data_type(), true)))
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("f", DataType::Utf8, true));
        RewriteLikePatternFunc::new().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn array_from(result: ColumnarValue) -> Result<ArrayRef> {
        match result {
            ColumnarValue::Array(array) => Ok(array),
            ColumnarValue::Scalar(s) => internal_err!("expected array, got scalar {s:?}"),
        }
    }

    fn scalar_utf8(result: ColumnarValue) -> Result<String> {
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s),
            other => internal_err!("expected Utf8 scalar, got {other:?}"),
        }
    }

    #[test]
    fn invoke_scalar_utf8() -> Result<()> {
        let result = invoke(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a!_b".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("!".to_string()))),
        ])?;
        assert_eq!(scalar_utf8(result)?, r"a\_b");
        Ok(())
    }

    #[test]
    fn invoke_array_utf8() -> Result<()> {
        let patterns = Arc::new(StringArray::from(vec![
            Some("!%foo"),
            Some("bar!_baz"),
            None,
            Some(r"keep\as\is"),
        ]));
        let result = invoke(vec![
            ColumnarValue::Array(patterns),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("!".to_string()))),
        ])?;
        let array = array_from(result)?;
        let array = as_string_array(&array)?;
        assert_eq!(array.len(), 4);
        assert_eq!(array.value(0), r"\%foo");
        assert_eq!(array.value(1), r"bar\_baz");
        assert!(array.is_null(2));
        assert_eq!(array.value(3), r"keep\\as\\is");
        Ok(())
    }

    #[test]
    fn invoke_array_large_utf8() -> Result<()> {
        let patterns = Arc::new(LargeStringArray::from(vec![Some("!!%"), Some("x")]));
        let result = invoke(vec![
            ColumnarValue::Array(patterns),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("!".to_string()))),
        ])?;
        let array = array_from(result)?;
        let array = as_large_string_array(&array)?;
        assert_eq!(array.value(0), r"\!%");
        assert_eq!(array.value(1), "x");
        Ok(())
    }

    #[test]
    fn invoke_array_utf8_view() -> Result<()> {
        let patterns = Arc::new(StringViewArray::from(vec![Some(r"a!_\b"), Some("!!%")]));
        let result = invoke(vec![
            ColumnarValue::Array(patterns),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("!".to_string()))),
        ])?;
        let array = array_from(result)?;
        let array = as_string_array(&array)?;
        assert_eq!(array.value(0), r"a\_\\b");
        assert_eq!(array.value(1), r"\!%");
        Ok(())
    }

    #[test]
    fn invoke_rejects_multi_char_escape() -> Result<()> {
        let err = invoke(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("ab".to_string()))),
        ])
        .err()
        .ok_or_else(|| exec_datafusion_err!("expected error for multi-char escape"))?;
        assert!(
            err.to_string().contains("single character"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn invoke_rejects_null_escape() -> Result<()> {
        let err = invoke(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ])
        .err()
        .ok_or_else(|| exec_datafusion_err!("expected error for null escape"))?;
        assert!(
            err.to_string().contains("not be null"),
            "unexpected error: {err}"
        );
        Ok(())
    }
}
