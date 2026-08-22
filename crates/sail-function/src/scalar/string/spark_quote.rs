use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkQuote {
    signature: Signature,
}

impl Default for SparkQuote {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkQuote {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkQuote {
    fn name(&self) -> &str {
        "quote"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `Quote.nullable = true`, unconditional (class body, beats `RuntimeReplaceable`).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/stringExpressions.scala#L3787>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = match args.arg_fields.first().map(|f| f.data_type()) {
            Some(DataType::LargeUtf8) => DataType::LargeUtf8,
            _ => DataType::Utf8,
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 1 {
            return exec_err!("`quote` function requires 1 argument, got {}", args.len());
        }
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(quote::<i32>, vec![])(&args),
            DataType::Utf8View => make_scalar_function(quote_view, vec![])(&args),
            DataType::LargeUtf8 => make_scalar_function(quote::<i64>, vec![])(&args),
            other => {
                exec_err!("unsupported data type {other:?} for function `quote`")
            }
        }
    }
}

/// Wraps the input string in single quotes, escaping `\` and `'` with a backslash.
///
/// This matches Spark's `quote` function behavior.
fn compute_quote(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 2);
    result.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            result.push('\\');
        }
        result.push(c);
    }
    result.push('\'');
    result
}

fn quote<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<T>(&args[0])?;
    let result = str_array
        .iter()
        .map(|opt_str| opt_str.map(compute_quote))
        .collect::<GenericStringArray<T>>();
    Ok(Arc::new(result) as ArrayRef)
}

fn quote_view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let str_array = as_string_view_array(&args[0])?;
    let result = str_array
        .iter()
        .map(|opt_str| opt_str.map(compute_quote))
        .collect::<StringArray>();
    Ok(Arc::new(result) as ArrayRef)
}
