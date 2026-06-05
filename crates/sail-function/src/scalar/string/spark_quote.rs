use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "quote"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
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
