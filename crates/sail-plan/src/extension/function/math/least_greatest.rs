use std::any::Any;

use datafusion::arrow::compute::kernels::cmp::{gt, lt};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return Err(DataFusionError::Execution(
                "Greatest function called with no arguments".to_string(),
            ));
        }
        let args = ColumnarValue::values_to_arrays(args)?;
        let greatest =
            args.iter()
                .skip(1)
                .try_fold(args[0].clone(), |greatest_so_far, current| {
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return Err(DataFusionError::Execution(
                "Least function called with no arguments".to_string(),
            ));
        }
        let args = ColumnarValue::values_to_arrays(args)?;
        let least = args
            .iter()
            .skip(1)
            .try_fold(args[0].clone(), |least_so_far, current| {
                let comparison = lt(&least_so_far, current)?;
                zip(&comparison, &least_so_far, current)
            })?;
        Ok(ColumnarValue::Array(least))
    }
}
