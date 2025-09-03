use std::any::{type_name, Any};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion_common::*;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::extension::function::error_utils::generic_internal_err;
use crate::extension::function::functions_nested_utils::downcast_arg;
use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkLuhnCheck {
    signature: Signature,
}

impl SparkLuhnCheck {
    pub const NAME: &'static str = "luhn_check";

    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl Default for SparkLuhnCheck {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkLuhnCheck {
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
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_luhn_check_inner, vec![])(&args)
    }
}

pub fn spark_luhn_check_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        let message: String = format!("luhn_check requires 1 argument, got {}", args.len());
        return Err(generic_internal_err(SparkLuhnCheck::NAME, &message));
    };

    let values: &StringArray = downcast_arg!(&args[0], StringArray);
    let result: Result<Vec<ScalarValue>> = values
        .iter()
        .map(|value| {
            if let Some(value) = value {
                luhn_check(value)
            } else {
                Ok(ScalarValue::Boolean(None))
            }
        })
        .collect::<Result<Vec<ScalarValue>>>();
    let result: ArrayRef = ScalarValue::iter_to_array(result?)?;
    Ok(result)
}

pub fn luhn_check(_value: &str) -> Result<ScalarValue> {
    todo!()
}
