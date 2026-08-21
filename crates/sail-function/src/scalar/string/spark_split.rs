use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, ListArray, ListBuilder, StringArrayType, StringBuilder,
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
pub struct SparkSplit {
    signature: Signature,
}

impl Default for SparkSplit {
    fn default() -> Self {
        Self::new()
    }
}
impl SparkSplit {
    pub const NAME: &'static str = "split";
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSplit {
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
        let err = || {
            Err(unsupported_data_types_exec_err(
                Self::NAME,
                "Expected (STRING, STRING) or (STRING, STRING, INT). Adjust the value to match the syntax, or change its target type. Use try_cast to handle malformed input and return NULL instead",
                arg_types,
            ))
        };

        let mut res_types = vec![];
        for i in 0..=1 {
            res_types.push(match arg_types.get(i) {
                Some(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) => {
                    Ok(arg_types[i].clone())
                }
                Some(DataType::Null) => Ok(DataType::Utf8),
                _ => err(),
            });
        }
        if arg_types.len() == 3 {
            res_types.push(if arg_types[2].is_null() || arg_types[2].is_integer() {
                Ok(DataType::Int32)
            } else {
                err()
            });
        }
        res_types.into_iter().collect::<Result<Vec<_>>>()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        if args.len() == 2 {
            args.push(ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))));
        }
        make_scalar_function(
            spark_split_inner,
            vec![Hint::Pad, Hint::AcceptsSingular, Hint::AcceptsSingular],
        )(&args)
    }
}

fn spark_split_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [values_arr, format_arr, limit_arr] = take_function_args(SparkSplit::NAME, args)?;
    let limit = opt_downcast_arg!(limit_arr, Int32Array);
    let Some(limit) = limit.as_ref() else {
        return Err(generic_internal_err(
            SparkSplit::NAME,
            "Could not downcast arguments to arrow arrays",
        ));
    };

    match values_arr.data_type() {
        DataType::Utf8 => split_with_format(values_arr.as_string::<i32>(), format_arr, limit),
        DataType::LargeUtf8 => split_with_format(values_arr.as_string::<i64>(), format_arr, limit),
        DataType::Utf8View => split_with_format(values_arr.as_string_view(), format_arr, limit),
        _ => Err(generic_internal_err(
            SparkSplit::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn split_with_format<'values, V>(
    values: V,
    format_arr: &ArrayRef,
    limit: &Int32Array,
) -> Result<ArrayRef>
where
    V: StringArrayType<'values>,
{
    match format_arr.data_type() {
        DataType::Utf8 => split_arrays(values, format_arr.as_string::<i32>(), limit),
        DataType::LargeUtf8 => split_arrays(values, format_arr.as_string::<i64>(), limit),
        DataType::Utf8View => split_arrays(values, format_arr.as_string_view(), limit),
        _ => Err(generic_internal_err(
            SparkSplit::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

fn split_arrays<'values, 'format, V, F>(
    values: V,
    format: F,
    limit: &Int32Array,
) -> Result<ArrayRef>
where
    V: StringArrayType<'values>,
    F: StringArrayType<'format>,
{
    let format_len = format.len();
    let limit_len = limit.len();
    let format_scalar = (format_len == 1 && format.is_valid(0))
        .then(|| parse_regex(format.value(0)))
        .transpose()?;
    let limit_scalar = (limit_len == 1 && limit.is_valid(0)).then(|| limit.value(0));
    let format_scalar_is_null = format_len == 1 && format.is_null(0);
    let limit_scalar_is_null = limit_len == 1 && limit.is_null(0);

    let mut builder = ListBuilder::new(StringBuilder::new());
    let mut regex_memo = StrMemo::new();
    for row in 0..values.len() {
        let format_index = if format_len == 1 { 0 } else { row };
        let limit_index = if limit_len == 1 { 0 } else { row };
        if format_scalar_is_null
            || limit_scalar_is_null
            || values.is_null(row)
            || format.is_null(format_index)
            || limit.is_null(limit_index)
        {
            builder.append_null();
        } else {
            let format_regex = regex_memo.resolve(
                format_scalar.as_ref(),
                || format.value(format_index),
                parse_regex,
            )?;
            let limit = limit_scalar.unwrap_or_else(|| limit.value(limit_index));
            let parts = split_to_array(values.value(row), format_regex, limit)?;
            builder.append_value(parts);
        }
    }
    let array: ListArray = builder.finish();
    Ok(Arc::new(array))
}

pub fn parse_regex(format: &str) -> Result<Regex> {
    Regex::new(format).map_err(|_| generic_exec_err(SparkSplit::NAME, "Invalid regex"))
}

pub fn split_to_array(value: &str, format: &Regex, limit: i32) -> Result<Vec<Option<String>>> {
    let values: Vec<&str> = if limit > 0 {
        format.splitn(value, limit as usize).collect::<Vec<&str>>()
    } else {
        format.split(value).collect::<Vec<&str>>()
    };
    Ok(values
        .iter()
        .map(|value| Some(value.to_string()))
        .collect::<Vec<Option<String>>>())
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray, StringArray, StringViewArray};

    use super::*;

    /// A delimiter supplied as a column selects the split pattern per row;
    /// null inputs yield null outputs.
    #[test]
    fn split_with_column_pattern() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a-b-c"),
            Some("a:b"),
            None,
            Some("x-y"),
        ]));
        let formats: ArrayRef = Arc::new(StringArray::from(vec![
            Some("-"),
            Some(":"),
            Some("-"),
            Some("-"),
        ]));
        let limits: ArrayRef = Arc::new(Int32Array::from(vec![-1, -1, -1, -1]));
        let output = spark_split_inner(&[values, formats, limits])?;
        let output = output
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list output");
        assert_eq!(output.len(), 4);
        let first = output.value(0);
        let first = first
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string parts");
        assert_eq!(
            (first.len(), first.value(0), first.value(1), first.value(2)),
            (3, "a", "b", "c")
        );
        let second = output.value(1);
        let second = second
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string parts");
        assert_eq!(
            (second.len(), second.value(0), second.value(1)),
            (2, "a", "b")
        );
        assert!(output.is_null(2));
        // Row 3 reuses "-" after row 0 compiled it: the memoized regex must
        // split exactly as a fresh compile would.
        let fourth = output.value(3);
        let fourth = fourth
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string parts");
        assert_eq!(
            (fourth.len(), fourth.value(0), fourth.value(1)),
            (2, "x", "y")
        );
        Ok(())
    }

    #[test]
    fn split_accepts_string_views() -> Result<()> {
        let values: ArrayRef = Arc::new(StringViewArray::from(vec!["a-b", "c-d"]));
        let format: ArrayRef = Arc::new(StringViewArray::from(vec!["-"]));
        let limit: ArrayRef = Arc::new(Int32Array::from(vec![-1]));

        let output = spark_split_inner(&[values, format, limit])?;
        let output = output
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list output");
        for (row, expected) in [["a", "b"], ["c", "d"]].into_iter().enumerate() {
            let parts = output.value(row);
            let parts = parts
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string parts");
            assert_eq!(parts.iter().collect::<Vec<_>>(), expected.map(Some));
        }
        Ok(())
    }
}
