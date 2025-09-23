use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_fixed_size_binary_array, as_large_binary_array,
};
use datafusion_common::{exec_err, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MakeValidUtf8 {
    signature: Signature,
}

impl Default for MakeValidUtf8 {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeValidUtf8 {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeValidUtf8 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_valid_utf8"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(data_type) => match data_type {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(data_type.clone()),
                DataType::Binary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
                    Ok(DataType::Utf8)
                }
                DataType::LargeBinary => Ok(DataType::LargeUtf8),
                _ => exec_err!("expected string array for `make_valid_utf8`"),
            },
            None => exec_err!("expected single argument for `make_valid_utf8`"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(make_valid_utf8_inner, vec![Hint::AcceptsSingular])(
            args.args.as_slice(),
        )
    }
}

fn make_valid_utf8_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.first() {
        Some(array) => match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(array.clone()),
            DataType::Binary => Ok(Arc::new(
                as_binary_array(&array)?
                    .iter()
                    .map(|x| x.map(String::from_utf8_lossy))
                    .collect::<StringArray>(),
            )),
            DataType::BinaryView => Ok(Arc::new(
                as_binary_view_array(&array)?
                    .iter()
                    .map(|x| x.map(String::from_utf8_lossy))
                    .collect::<StringArray>(),
            )),
            DataType::FixedSizeBinary(_) => Ok(Arc::new(
                as_fixed_size_binary_array(&array)?
                    .iter()
                    .map(|x| x.map(String::from_utf8_lossy))
                    .collect::<StringArray>(),
            )),
            DataType::LargeBinary => Ok(Arc::new(
                as_large_binary_array(&array)?
                    .iter()
                    .map(|x| x.map(String::from_utf8_lossy))
                    .collect::<LargeStringArray>(),
            )),
            _ => exec_err!("expected string array for `make_valid_utf8`"),
        },
        None => exec_err!("expected single argument for `make_valid_utf8`"),
    }
}
