use std::any::Any;

use datafusion::arrow::compute::kernels::cmp::{gt, lt};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::exec_err;
use datafusion_expr::ScalarFunctionArgs;

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct Greatest {
    signature: Signature,
}

impl Default for Greatest {
    fn default() -> Self {
        Self::new()
    }
}

impl Greatest {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Greatest {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let args = ColumnarValue::values_to_arrays(&args)?;
        let Ok((first, others)) = args.at_least_one() else {
            return exec_err!("function `greatest` called with no arguments");
        };
        let greatest = others.iter().try_fold(first, |greatest_so_far, current| {
            let comparison = gt(&greatest_so_far, current)?;
            zip(&comparison, &greatest_so_far, current)
        })?;
        Ok(ColumnarValue::Array(greatest))
    }
}

#[derive(Debug)]
pub struct Least {
    signature: Signature,
}

impl Least {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for Least {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for Least {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "least"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let args = ColumnarValue::values_to_arrays(&args)?;
        let Ok((first, others)) = args.at_least_one() else {
            return exec_err!("function `least` called with no arguments");
        };
        let least = others.iter().try_fold(first, |least_so_far, current| {
            let comparison = lt(&least_so_far, current)?;
            zip(&comparison, &least_so_far, current)
        })?;
        Ok(ColumnarValue::Array(least))
    }
}
