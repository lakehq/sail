use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::unicode::reverse::ReverseFunc;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions_nested::reverse::array_reverse_inner;

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkReverse {
    signature: Signature,
}

impl Default for SparkReverse {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkReverse {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkReverse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("array_reverse needs one argument");
        }
        match args.args[0].data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                ReverseFunc::new().invoke_with_args(args)
            }
            _ => make_scalar_function(array_reverse_inner)(&args.args),
        }
    }
}
