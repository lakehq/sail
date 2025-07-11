use std::any::Any;

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkTryAdd {
    signature: Signature,
}

impl Default for SparkTryAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryAdd {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryAdd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.contains(&DataType::Int64) {
            Ok(DataType::Int64)
        } else {
            Ok(DataType::Int32)
        }
    }


    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_try_add",
                (2, 2),
                args.len(),
            ));
        };
        match (left.data_type(), right.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                todo!()
            }
        }

    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "spark_try_add",
                (2, 2),
                types.len(),
            ));
        }
        let left: &DataType = &types[0];
        let valid_left = matches!(left, DataType::Int32 | DataType::Int64);
        let right: &DataType = &types[1];
        let valid_right = matches!(right, DataType::Int32 | DataType::Int64);

        if valid_left && valid_right {
            Ok(vec![left.clone(), right.clone()])
        } else {
            Err(unsupported_data_types_exec_err(
                "spark_try_add",
                "Int32, Int32",
                types,
            ))
        }
    }
}
