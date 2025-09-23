use std::any::Any;
use std::fmt::Write;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, BinaryViewArray, GenericByteArray, OffsetSizeTrait, StringArray,
};
use datafusion::arrow::datatypes::{DataType, GenericBinaryType};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr::ScalarFunctionArgs;
use sha1::{Digest, Sha1};

use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkSha1 {
    signature: Signature,
}

impl Default for SparkSha1 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSha1 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSha1 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_sha1"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_sha1_inner, vec![])(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!(
                "`sha1` function requires 1 argument, got {}",
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
            _ => exec_err!("`sha1` function does not support type {}", arg_types[0]),
        }
    }
}

fn spark_sha1_digest(value: &[u8]) -> String {
    let result = Sha1::digest(value);
    let mut s = String::with_capacity(result.len() * 2);
    for b in result.as_slice() {
        #[allow(clippy::unwrap_used)]
        write!(&mut s, "{b:02x}").unwrap();
    }
    s
}

// Helper function to process GenericByteArray
fn process_binary_array<T: OffsetSizeTrait>(
    array: &GenericByteArray<GenericBinaryType<T>>,
) -> StringArray {
    array
        .iter()
        .map(|value| value.map(spark_sha1_digest))
        .collect::<StringArray>()
}

// Helper function to process BinaryViewArray
fn process_binary_view_array(array: &BinaryViewArray) -> StringArray {
    array
        .iter()
        .map(|value| value.map(spark_sha1_digest))
        .collect::<StringArray>()
}

pub fn spark_sha1_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`sha1` function requires 1 argument, got {}", args.len());
    }

    let array = &args[0];

    let result: StringArray = match array.data_type() {
        DataType::Binary => process_binary_array(array.as_binary::<i32>()),
        DataType::LargeBinary => process_binary_array(array.as_binary::<i64>()),
        DataType::BinaryView => process_binary_view_array(array.as_binary_view()),
        dt => {
            return exec_err!(
                "sha1 function was called with unexpected type {dt:?}. This should have been coerced."
            );
        }
    };
    Ok(Arc::new(result) as ArrayRef)
}
