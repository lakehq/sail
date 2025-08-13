use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, ListBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field};
use datafusion_common::Result;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use regex::Regex;

use crate::extension::function::error_utils::{generic_exec_err, unsupported_data_types_exec_err};
use crate::extension::function::functions_nested_utils::opt_downcast_arg;
use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    /// AnalysisException: Failed to coerce arguments to satisfy a call to 'split' function:
    /// coercion from [Utf8, Utf8] to the signature UserDefined failed No function matches the
    /// given name and argument types 'split(Utf8, Utf8)'.
    /// You might need to add explicit type casts. Candidate functions: split(UserDefined)
    ///
    ///
    ///
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {

            [
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null ,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null
            ] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    DataType::Int64,
                ])
            }
            [
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null ,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null ,
                DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64 | DataType::Null | DataType::Utf8
            ] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    arg_types[2].clone(),
                ])
            }
            _ => Err(unsupported_data_types_exec_err(
                Self::NAME,
                "Expected (STRING, STRING) or (STRING, STRING, INT). Adjust the value to match the syntax, or change its target type. Use try_cast to handle malformed input and return NULL instead",
                arg_types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_split_inner, vec![])(&args)
    }
}

pub fn spark_split_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let len: usize = args[0].len();
    // Initialize default values in case of nulls
    let default_nulls: Vec<Option<&str>> = vec![None; len];
    let default_nulls: StringArray = StringArray::from(default_nulls);
    let default_limit: Int32Array = Int32Array::from(vec![0; len]);

    // Getting the arrays
    let values: &StringArray = opt_downcast_arg!(&args[0], StringArray).unwrap_or(&default_nulls);
    let format: &StringArray = opt_downcast_arg!(&args[1], StringArray).unwrap_or(&default_nulls);
    let limit: &Int32Array = opt_downcast_arg!(&args[2], Int32Array).unwrap_or(&default_limit);

    let mut builder = ListBuilder::new(StringBuilder::new());

    for i in 0..len {
        if values.is_null(i) || format.is_null(i) || limit.is_null(i) {
            builder.append_null();
        } else {
            let (value, format, limit): (&str, &str, i32) =
                (values.value(i), format.value(i), limit.value(i));
            let values_format: Vec<Option<String>> = split_to_array(value, format, limit)?;
            builder.append_value(values_format);
        }
    }

    let array: ListArray = builder.finish();
    Ok(Arc::new(array))
}

pub fn split_to_array(value: &str, format: &str, limit: i32) -> Result<Vec<Option<String>>> {
    let format: Regex =
        Regex::new(format).map_err(|_| generic_exec_err(SparkSplit::NAME, "Invalid regex"))?;
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
