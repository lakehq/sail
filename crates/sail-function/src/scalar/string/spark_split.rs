use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericStringArray, Int32Array, ListArray, ListBuilder, OffsetSizeTrait,
    StringBuilder,
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
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::LargeUtf8, DataType::LargeUtf8) => spark_split_inner_downcast::<i64, i64>(args),
        (_, DataType::LargeUtf8) => spark_split_inner_downcast::<i32, i64>(args),
        (DataType::LargeUtf8, _) => spark_split_inner_downcast::<i64, i32>(args),
        _ => spark_split_inner_downcast::<i32, i32>(args),
    }
}

fn spark_split_inner_downcast<FirstOffset, SecondOffset>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    FirstOffset: OffsetSizeTrait,
    SecondOffset: OffsetSizeTrait,
{
    let [values_arr, format_arr, limit_arr] = take_function_args(SparkSplit::NAME, args)?;
    let values: Arc<Option<&GenericStringArray<FirstOffset>>> = Arc::new(
        values_arr
            .as_any()
            .downcast_ref::<GenericStringArray<FirstOffset>>(),
    );
    let format: Arc<Option<&GenericStringArray<SecondOffset>>> = Arc::new(
        format_arr
            .as_any()
            .downcast_ref::<GenericStringArray<SecondOffset>>(),
    );
    let limit = opt_downcast_arg!(limit_arr, Int32Array);

    match (values.as_ref(), format.as_ref(), limit.as_ref()) {
        (Some(values), Some(format), Some(limit)) => {
            let format_scalar_opt = (format.len() == 1 && format.is_valid(0))
                .then(|| parse_regex(format.value(0)))
                .transpose()?;
            let limit_scalar_opt = (limit.len() == 1 && limit.is_valid(0)).then(|| limit.value(0));
            let is_format_null = format.len() == 1 && format.is_null(0);
            let is_limit_null = limit.len() == 1 && limit.is_null(0);

            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..args[0].len() {
                if is_format_null
                    || is_limit_null
                    || values.is_null(i)
                    || format.is_null(i)
                    || limit.is_null(i)
                {
                    builder.append_null();
                } else {
                    let format_regex = format_scalar_opt.as_ref().map_or_else(
                        || parse_regex(format.value(i)),
                        |format_regex| Ok(format_regex.clone()),
                    )?;
                    let limit = limit_scalar_opt.unwrap_or_else(|| limit.value(i));

                    let values_format: Vec<Option<String>> =
                        split_to_array(values.value(i), &format_regex, limit)?;
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
