use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, BinaryViewArray, GenericByteArray, Int64Array, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::{DataType, GenericBinaryType};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr::ScalarFunctionArgs;

use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkCrc32 {
    signature: Signature,
}

impl Default for SparkCrc32 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCrc32 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCrc32 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "crc32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_crc32_inner, vec![])(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!(
                "`crc32` function requires 1 argument, got {}",
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Ok(vec![arg_types[0].clone()])
            }
            DataType::Utf8 | DataType::Utf8View => Ok(vec![DataType::Binary]),
            DataType::LargeUtf8 => Ok(vec![DataType::LargeBinary]),
            DataType::Null => Ok(vec![DataType::Binary]),
            _ => exec_err!("`crc32` function does not support type {}", arg_types[0]),
        }
    }
}

fn crc32_i64(value: &[u8]) -> i64 {
    crc32fast::hash(value) as i64
}

// Helper function to process GenericByteArray
fn process_binary_array<T: OffsetSizeTrait>(
    array: &GenericByteArray<GenericBinaryType<T>>,
) -> Int64Array {
    array
        .iter()
        .map(|value| value.map(crc32_i64))
        .collect::<Int64Array>()
}

// Helper function to process BinaryViewArray
fn process_binary_view_array(array: &BinaryViewArray) -> Int64Array {
    array
        .iter()
        .map(|value| value.map(crc32_i64))
        .collect::<Int64Array>()
}

pub fn spark_crc32_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`crc32` function requires 1 argument, got {}", args.len());
    }

    let array = &args[0];

    let result: Int64Array = match array.data_type() {
        DataType::Binary => process_binary_array(array.as_binary::<i32>()),
        DataType::LargeBinary => process_binary_array(array.as_binary::<i64>()),
        DataType::BinaryView => process_binary_view_array(array.as_binary_view()),
        dt => {
            return exec_err!(
                "crc32 function was called with unexpected type {dt:?}. This should have been coerced."
            );
        }
    };
    Ok(Arc::new(result) as ArrayRef)
}
