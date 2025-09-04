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
                Ok(ScalarValue::Boolean(Some(luhn_check(value))))
            } else {
                Ok(ScalarValue::Boolean(None))
            }
        })
        .collect::<Result<Vec<ScalarValue>>>();
    let result: ArrayRef = ScalarValue::iter_to_array(result?)?;
    Ok(result)
}

pub fn luhn_check(value: &str) -> bool {
    let digits: Option<Vec<u32>> = value.chars().map(|c| c.to_digit(10)).collect();
    let digits: Vec<u32> = match digits {
        Some(ds) if !ds.is_empty() => ds,
        _ => return false,
    };

    let len = digits.len();
    if len < 2 {
        return false;
    }

    let sum: u32 = digits.iter().rev().enumerate().fold(0u32, |acc, (i, &d)| {
        let digit = if i % 2 == 0 {
            d
        } else if d > 4 {
            2 * d - 9
        } else {
            2 * d
        };
        acc + digit
    });

    let check_digit: u32 = (10 - (sum % 10)) % 10;
    check_digit == 0
}
