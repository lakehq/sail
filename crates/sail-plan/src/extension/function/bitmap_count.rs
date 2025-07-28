use core::any::type_name;
use std::any::Any;
use std::sync::Arc;

use arrow::array::Int64Array;
use datafusion::arrow::array::{ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, DataFusionError};
use datafusion_expr::ScalarFunctionArgs;

use crate::extension::function::functions_nested_utils::*;
use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct BitmapCount {
    signature: Signature,
}

impl Default for BitmapCount {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapCount {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitmapCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bitmap_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (arg_types.len() == 1, arg_types.first()) {
            (true, Some(DataType::Binary | DataType::BinaryView | DataType::LargeBinary)) => {
                Ok(DataType::Int64)
            }
            _ => Err(DataFusionError::Internal(format!(
                "{} should only be called with a binary",
                self.name(),
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(bitmap_count_inner, vec![])(&args.args)
    }
}

macro_rules! downcast_and_count_ones {
    ($input_array:expr, $array_type:ident) => {{
        let arr = downcast_arg!($input_array, $array_type);
        Ok(arr
            .iter()
            .map(|opt| opt.map(|value| value.iter().map(|b| b.count_ones() as i64).sum()))
            .collect::<Int64Array>())
    }};
}

pub fn bitmap_count_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("bitmap_count", arg)?;

    let res: Result<arrow::array::Int64Array> = match &input_array.data_type() {
        DataType::Binary => downcast_and_count_ones!(input_array, BinaryArray),
        DataType::BinaryView => downcast_and_count_ones!(input_array, BinaryViewArray),
        DataType::LargeBinary => downcast_and_count_ones!(input_array, LargeBinaryArray),
        array_type => exec_err!("`bitmap_count` does not support type '{array_type:?}'."),
    };

    Ok(Arc::new(res?))
}
