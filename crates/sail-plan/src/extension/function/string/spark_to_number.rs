use crate::extension::function::functions_nested_utils::downcast_arg;
use crate::extension::function::functions_utils::make_scalar_function;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use core::any::type_name;
use datafusion_common::{
    exec_err, internal_err, plan_datafusion_err, DataFusionError, Result, ScalarValue,
};
use std::any::Any;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use regex::Regex;

use crate::extension::function::functions_nested_utils::*;

#[derive(Debug)]
pub struct SparkToNumber {
    signature: Signature,
}

impl SparkToNumber {
    pub const NAME: &'static str = "to_number";

    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl Default for SparkToNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkToNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Decimal128(38, 9)) // TODO: use precision and scale from arg_types
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_to_number_inner, vec![])(&args)
    }
}

pub fn spark_to_number_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return internal_err!(
            "`{}` function requires 2 arguments, got {}",
            SparkToNumber::NAME,
            args.len()
        );
    }
    let values = downcast_arg!(&args[0], StringArray);
    let format: &str = downcast_arg!(&args[1], StringArray).value(0);

    let result = values
        .iter()
        .map(|value| match value {
            Some(value) => todo!(),
            None => ScalarValue::Decimal128(None, 38, 9),
        })
        .collect::<ArrayRef>();

    todo!()
}

pub fn parse_number(value: &str, format: &str) -> Result<ScalarValue> {
    let format = format.trim().split('.').collect::<Vec<_>>();
    if let [left, right] = format[..] {
        todo!()
    }
    todo!()
}

pub fn match_format_regex(format: &str) -> Result<Regex> {
    // Define the regex pattern
    let pattern = r"^(?:MI|S)\$[09G,]*[.D][09]*\$(?:PR|MI|S)$";

    // Create a Regex instance
    let regex =
        Regex::new(pattern).map_err(|e| plan_datafusion_err!("Failed to compile regex: {}", e))?;

    // Check if the format matches the regex pattern
    if regex.is_match(format) {
        Ok(regex)
    } else {
        exec_err!(
            "Format string '{}' does not match the expected pattern",
            format
        )
    }
}
