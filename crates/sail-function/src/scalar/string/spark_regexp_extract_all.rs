use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, ListArray, ListBuilder, StringArrayType, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use regex::Regex;

use crate::error::{generic_exec_err, generic_internal_err, unsupported_data_types_exec_err};
use crate::functions_nested_utils::opt_downcast_arg;
use crate::functions_utils::{StrMemo, make_scalar_function};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpExtract {
    signature: Signature,
}

impl Default for SparkRegexpExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpExtract {
    pub const NAME: &'static str = "regexp_extract";

    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRegexpExtract {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_extract_types(Self::NAME, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
        }
        make_scalar_function(
            regexp_extract_inner,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::AcceptsSingular],
        )(&args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpExtractAll {
    signature: Signature,
}

impl Default for SparkRegexpExtractAll {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpExtractAll {
    pub const NAME: &'static str = "regexp_extract_all";

    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRegexpExtractAll {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_regexp_extract_types(Self::NAME, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
        }
        make_scalar_function(
            regexp_extract_all_inner,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::AcceptsSingular],
        )(&args)
    }
}

fn regexp_extract_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [values_arr, pattern_arr, idx_arr] = take_function_args(SparkRegexpExtract::NAME, args)?;
    let idx = opt_downcast_arg!(idx_arr, Int64Array);
    let Some(idx) = idx.as_ref() else {
        return Err(generic_internal_err(
            SparkRegexpExtract::NAME,
            "Could not downcast arguments to arrow arrays",
        ));
    };

    match values_arr.data_type() {
        DataType::Utf8 => {
            regexp_extract_with_pattern(values_arr.as_string::<i32>(), pattern_arr, idx)
        }
        DataType::LargeUtf8 => {
            regexp_extract_with_pattern(values_arr.as_string::<i64>(), pattern_arr, idx)
        }
        DataType::Utf8View => {
            regexp_extract_with_pattern(values_arr.as_string_view(), pattern_arr, idx)
        }
        _ => Err(generic_internal_err(
            SparkRegexpExtract::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn regexp_extract_with_pattern<'values, V>(
    values: V,
    pattern_arr: &ArrayRef,
    idx: &Int64Array,
) -> Result<ArrayRef>
where
    V: StringArrayType<'values>,
{
    match pattern_arr.data_type() {
        DataType::Utf8 => regexp_extract_arrays(values, pattern_arr.as_string::<i32>(), idx),
        DataType::LargeUtf8 => regexp_extract_arrays(values, pattern_arr.as_string::<i64>(), idx),
        DataType::Utf8View => regexp_extract_arrays(values, pattern_arr.as_string_view(), idx),
        _ => Err(generic_internal_err(
            SparkRegexpExtract::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn regexp_extract_arrays<'values, 'pattern, V, P>(
    values: V,
    pattern: P,
    idx: &Int64Array,
) -> Result<ArrayRef>
where
    V: StringArrayType<'values>,
    P: StringArrayType<'pattern>,
{
    let pattern_len = pattern.len();
    let idx_len = idx.len();

    let pattern_scalar_opt = (pattern_len == 1 && pattern.is_valid(0))
        .then(|| parse_regex(SparkRegexpExtract::NAME, pattern.value(0)))
        .transpose()?;
    let idx_scalar_opt = (idx_len == 1 && idx.is_valid(0)).then(|| idx.value(0));
    let is_pattern_null = pattern_len == 1 && pattern.is_null(0);
    let is_idx_null = idx_len == 1 && idx.is_null(0);

    let mut builder = StringBuilder::new();
    // Compile each distinct column pattern once per batch, not per row.
    let mut regex_memo = StrMemo::new();
    for i in 0..values.len() {
        let pattern_is_null = if pattern_len == 1 {
            is_pattern_null
        } else {
            pattern.is_null(i)
        };
        let idx_is_null = if idx_len == 1 {
            is_idx_null
        } else {
            idx.is_null(i)
        };

        if pattern_is_null || idx_is_null || values.is_null(i) {
            builder.append_null();
        } else {
            let re = regex_memo.resolve(
                pattern_scalar_opt.as_ref(),
                || pattern.value(i),
                |p| parse_regex(SparkRegexpExtract::NAME, p),
            )?;
            let group_idx = idx_scalar_opt.unwrap_or_else(|| idx.value(i));
            builder.append_value(extract_first_match(
                SparkRegexpExtract::NAME,
                values.value(i),
                re,
                group_idx,
            )?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn regexp_extract_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [values_arr, pattern_arr, idx_arr] = take_function_args(SparkRegexpExtractAll::NAME, args)?;
    let idx = opt_downcast_arg!(idx_arr, Int64Array);
    let Some(idx) = idx.as_ref() else {
        return Err(generic_internal_err(
            SparkRegexpExtractAll::NAME,
            "Could not downcast arguments to arrow arrays",
        ));
    };

    match values_arr.data_type() {
        DataType::Utf8 => {
            regexp_extract_all_with_pattern(values_arr.as_string::<i32>(), pattern_arr, idx)
        }
        DataType::LargeUtf8 => {
            regexp_extract_all_with_pattern(values_arr.as_string::<i64>(), pattern_arr, idx)
        }
        DataType::Utf8View => {
            regexp_extract_all_with_pattern(values_arr.as_string_view(), pattern_arr, idx)
        }
        _ => Err(generic_internal_err(
            SparkRegexpExtractAll::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn regexp_extract_all_with_pattern<'values, V>(
    values: V,
    pattern_arr: &ArrayRef,
    idx: &Int64Array,
) -> Result<ArrayRef>
where
    V: StringArrayType<'values>,
{
    match pattern_arr.data_type() {
        DataType::Utf8 => regexp_extract_all_arrays(values, pattern_arr.as_string::<i32>(), idx),
        DataType::LargeUtf8 => {
            regexp_extract_all_arrays(values, pattern_arr.as_string::<i64>(), idx)
        }
        DataType::Utf8View => regexp_extract_all_arrays(values, pattern_arr.as_string_view(), idx),
        _ => Err(generic_internal_err(
            SparkRegexpExtractAll::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn regexp_extract_all_arrays<'values, 'pattern, V, P>(
    values: V,
    pattern: P,
    idx: &Int64Array,
) -> Result<ArrayRef>
where
    V: StringArrayType<'values>,
    P: StringArrayType<'pattern>,
{
    let pattern_len = pattern.len();
    let idx_len = idx.len();

    let pattern_scalar_opt = (pattern_len == 1 && pattern.is_valid(0))
        .then(|| parse_regex(SparkRegexpExtractAll::NAME, pattern.value(0)))
        .transpose()?;
    let idx_scalar_opt = (idx_len == 1 && idx.is_valid(0)).then(|| idx.value(0));
    let is_pattern_null = pattern_len == 1 && pattern.is_null(0);
    let is_idx_null = idx_len == 1 && idx.is_null(0);

    let mut builder = ListBuilder::new(StringBuilder::new());
    // Compile each distinct column pattern once per batch, not per row.
    let mut regex_memo = StrMemo::new();
    for i in 0..values.len() {
        let pattern_is_null = if pattern_len == 1 {
            is_pattern_null
        } else {
            pattern.is_null(i)
        };
        let idx_is_null = if idx_len == 1 {
            is_idx_null
        } else {
            idx.is_null(i)
        };

        if pattern_is_null || idx_is_null || values.is_null(i) {
            builder.append_null();
        } else {
            let re = regex_memo.resolve(
                pattern_scalar_opt.as_ref(),
                || pattern.value(i),
                |p| parse_regex(SparkRegexpExtractAll::NAME, p),
            )?;
            let group_idx = idx_scalar_opt.unwrap_or_else(|| idx.value(i));
            let matches =
                extract_all_matches(SparkRegexpExtractAll::NAME, values.value(i), re, group_idx)?;
            builder.append_value(matches);
        }
    }
    let array: ListArray = builder.finish();
    Ok(Arc::new(array))
}

fn coerce_regexp_extract_types(
    function_name: &str,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    if !(2..=3).contains(&arg_types.len()) {
        return Err(unsupported_data_types_exec_err(
            function_name,
            "Expected (STRING, STRING) or (STRING, STRING, INT)",
            arg_types,
        ));
    }

    let mut res_types = vec![];
    for i in 0..=1 {
        res_types.push(match arg_types.get(i) {
            Some(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) => {
                Ok(arg_types[i].clone())
            }
            Some(DataType::Null) => Ok(DataType::Utf8),
            _ => Err(unsupported_data_types_exec_err(
                function_name,
                "Expected (STRING, STRING) or (STRING, STRING, INT)",
                arg_types,
            )),
        });
    }
    if arg_types.len() == 3 {
        res_types.push(if arg_types[2].is_null() || arg_types[2].is_integer() {
            Ok(DataType::Int64)
        } else {
            Err(unsupported_data_types_exec_err(
                function_name,
                "Expected (STRING, STRING) or (STRING, STRING, INT)",
                arg_types,
            ))
        });
    }
    res_types.into_iter().collect::<Result<Vec<_>>>()
}

fn parse_regex(function_name: &str, pattern: &str) -> Result<Regex> {
    Regex::new(pattern).map_err(|_| generic_exec_err(function_name, "Invalid regex pattern"))
}

fn validate_group_idx(function_name: &str, group_idx: i64) -> Result<usize> {
    if group_idx < 0 {
        return Err(generic_exec_err(
            function_name,
            &format!("The group index must be non-negative, but got {group_idx}"),
        ));
    }
    Ok(group_idx as usize)
}

fn extract_capture(
    function_name: &str,
    caps: &regex::Captures<'_>,
    group_idx: i64,
) -> Result<String> {
    let group_idx = validate_group_idx(function_name, group_idx)?;
    let max_group_idx = caps.len().saturating_sub(1);
    if group_idx > max_group_idx {
        return Err(generic_exec_err(
            function_name,
            &format!("Expects group index between 0 and {max_group_idx}, but got {group_idx}"),
        ));
    }
    Ok(caps
        .get(group_idx)
        .map(|m| m.as_str())
        .unwrap_or("")
        .to_string())
}

fn extract_first_match(
    function_name: &str,
    value: &str,
    re: &Regex,
    group_idx: i64,
) -> Result<String> {
    validate_group_idx(function_name, group_idx)?;
    match re.captures(value) {
        Some(caps) => extract_capture(function_name, &caps, group_idx),
        None => Ok(String::new()),
    }
}

fn extract_all_matches(
    function_name: &str,
    value: &str,
    re: &Regex,
    group_idx: i64,
) -> Result<Vec<Option<String>>> {
    validate_group_idx(function_name, group_idx)?;
    let mut results = Vec::new();
    for caps in re.captures_iter(value) {
        results.push(Some(extract_capture(function_name, &caps, group_idx)?));
    }
    Ok(results)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Int64Array, ListArray, StringArray};

    use super::*;

    fn strings(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    /// A pattern supplied as a column selects the regex per row; null patterns
    /// yield null outputs and an invalid pattern in the column errors.
    #[test]
    fn extract_with_column_pattern() -> Result<()> {
        let values = strings(vec![Some("a11b"), Some("a22b"), Some("xyz"), Some("a33b")]);
        let patterns = strings(vec![
            Some("([0-9]+)"),
            Some("([0-9]+)"),
            None,
            Some("([a-z]+)"),
        ]);
        let idx: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1, 1]));
        let output = regexp_extract_inner(&[values.clone(), patterns, idx.clone()])?;
        let output = output
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output");
        assert_eq!(output.value(0), "11");
        assert_eq!(output.value(1), "22");
        assert!(output.is_null(2));
        assert_eq!(output.value(3), "a");

        let invalid = strings(vec![Some("("), Some("("), Some("("), Some("(")]);
        assert!(regexp_extract_inner(&[values, invalid, idx]).is_err());
        Ok(())
    }

    #[test]
    fn extract_all_with_column_pattern() -> Result<()> {
        let values = strings(vec![Some("1a2"), Some("3b4"), Some("5c6")]);
        let patterns = strings(vec![Some("[0-9]"), Some("[a-z]"), Some("[0-9]")]);
        let idx: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 0, 0]));
        let output = regexp_extract_all_inner(&[values, patterns, idx])?;
        let output = output
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list output");
        let digits = output.value(0);
        let digits = digits
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string matches");
        assert_eq!(
            (digits.len(), digits.value(0), digits.value(1)),
            (2, "1", "2")
        );
        let letters = output.value(1);
        let letters = letters
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string matches");
        assert_eq!((letters.len(), letters.value(0)), (1, "b"));
        // Row 2 reuses "[0-9]" after row 0 compiled it: the memoized regex
        // must extract exactly as a fresh compile would.
        let reused = output.value(2);
        let reused = reused
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string matches");
        assert_eq!(
            (reused.len(), reused.value(0), reused.value(1)),
            (2, "5", "6")
        );
        Ok(())
    }
}
