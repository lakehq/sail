use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericStringArray, Int32Array, ListArray, ListBuilder, OffsetSizeTrait,
    StringBuilder, StringViewArray,
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
use crate::functions_utils::make_scalar_function;

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
    let values = string_array_like(values_arr);
    let format = string_array_like(format_arr);
    let limit = opt_downcast_arg!(limit_arr, Int32Array);

    match (values.as_deref(), format.as_deref(), limit.as_ref()) {
        (Some(values), Some(format), Some(limit)) => {
            let format_scalar_opt = (format.len_() == 1 && format.is_valid_(0))
                .then(|| parse_regex(format.value_(0)))
                .transpose()?;
            let limit_scalar_opt = (limit.len() == 1 && limit.is_valid(0)).then(|| limit.value(0));
            let is_format_null = format.len_() == 1 && format.is_null_(0);
            let is_limit_null = limit.len() == 1 && limit.is_null(0);

            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..args[0].len() {
                let format_index = if format.len_() == 1 { 0 } else { i };
                let limit_index = if limit.len() == 1 { 0 } else { i };
                if is_format_null
                    || is_limit_null
                    || values.is_null_(i)
                    || format.is_null_(format_index)
                    || limit.is_null(limit_index)
                {
                    builder.append_null();
                } else {
                    let format_regex = format_scalar_opt.as_ref().map_or_else(
                        || parse_regex(format.value_(format_index)),
                        |format_regex| Ok(format_regex.clone()),
                    )?;
                    let limit = limit_scalar_opt.unwrap_or_else(|| limit.value(limit_index));

                    let values_format: Vec<Option<String>> =
                        split_to_array(values.value_(i), &format_regex, limit)?;
                    builder.append_value(values_format);
                }
            }
            let array: ListArray = builder.finish();
            Ok(Arc::new(array))
        }
        _ => Err(generic_internal_err(
            SparkSplit::NAME,
            "Could not downcast arguments to arrow arrays",
        )),
    }
}

// Parquet strings remain `Utf8View` inside the plan. This also makes `str_to_map` view-aware
// because its first parsing stage delegates to `SparkSplit`.
trait StringArrayLike {
    fn len_(&self) -> usize;
    fn is_valid_(&self, index: usize) -> bool;
    fn is_null_(&self, index: usize) -> bool;
    fn value_(&self, index: usize) -> &str;
}

impl<O: OffsetSizeTrait> StringArrayLike for GenericStringArray<O> {
    fn len_(&self) -> usize {
        self.len()
    }
    fn is_valid_(&self, index: usize) -> bool {
        self.is_valid(index)
    }
    fn is_null_(&self, index: usize) -> bool {
        self.is_null(index)
    }
    fn value_(&self, index: usize) -> &str {
        self.value(index)
    }
}

impl StringArrayLike for StringViewArray {
    fn len_(&self) -> usize {
        self.len()
    }
    fn is_valid_(&self, index: usize) -> bool {
        self.is_valid(index)
    }
    fn is_null_(&self, index: usize) -> bool {
        self.is_null(index)
    }
    fn value_(&self, index: usize) -> &str {
        self.value(index)
    }
}

impl<T: StringArrayLike + ?Sized> StringArrayLike for &T {
    fn len_(&self) -> usize {
        (**self).len_()
    }
    fn is_valid_(&self, index: usize) -> bool {
        (**self).is_valid_(index)
    }
    fn is_null_(&self, index: usize) -> bool {
        (**self).is_null_(index)
    }
    fn value_(&self, index: usize) -> &str {
        (**self).value_(index)
    }
}

fn string_array_like(array: &ArrayRef) -> Option<Box<dyn StringArrayLike + '_>> {
    if let Some(array) = array.as_any().downcast_ref::<GenericStringArray<i32>>() {
        Some(Box::new(array))
    } else if let Some(array) = array.as_any().downcast_ref::<GenericStringArray<i64>>() {
        Some(Box::new(array))
    } else {
        array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|array| Box::new(array) as Box<dyn StringArrayLike>)
    }
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
