use std::any::Any;
use std::sync::Arc;

use crate::extension::function::error_utils::{generic_exec_err, unsupported_data_types_exec_err};
use crate::extension::function::functions_nested_utils::downcast_arg;
use crate::extension::function::functions_utils::make_scalar_function;
use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::DataFusionError;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use regex::Regex;
use std::any::type_name;

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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    DataType::Int32,
                ])
            }
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Int32] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    arg_types[2].clone(),
                ])
            }
            _ => Err(unsupported_data_types_exec_err(
                Self::NAME,
                "Must be of type (String, String) or (String, String, Int)",
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
    let limit: i32 = match args.get(2) {
        Some(limit) => limit
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| array.value(0))
            .unwrap_or(0),
        None => 0,
    };

    let values: &StringArray = downcast_arg!(&args[0], StringArray);
    let format: &StringArray = downcast_arg!(&args[1], StringArray);
    let values_format = values
        .iter()
        .zip(format.iter())
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => Ok(todo!()),
            _ => Ok(ScalarValue::Null),
        })
        .collect::<Result<Vec<ScalarValue>>>();

    todo!()
}

pub fn split_to_array(value: &str, format: &str) -> Result<ArrayRef> {
    let format: Regex =
        Regex::new(format).map_err(|e| generic_exec_err(SparkSplit::NAME, "Invalid regex"))?;
    let values = format.split(value).collect::<Vec<&str>>();
    let a = values
        .iter()
        .map(|value| ScalarValue::try_from_string(value.to_string(), &DataType::Utf8))
        .collect::<Result<Vec<ScalarValue>>>()?;
    todo!()
}
