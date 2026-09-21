use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_fixed_size_binary_array, as_large_binary_array,
};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
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
    fn name(&self) -> &str {
        "make_valid_utf8"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `MakeValidUTF8.nullable = true`, unconditional (class body, beats `RuntimeReplaceable`).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/stringExpressions.scala#L813>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = match args.arg_fields.first().map(|f| f.data_type()) {
            Some(data_type) => match data_type {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => data_type.clone(),
                DataType::Binary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
                    DataType::Utf8
                }
                DataType::LargeBinary => DataType::LargeUtf8,
                _ => return exec_err!("expected string array for `make_valid_utf8`"),
            },
            None => return exec_err!("expected single argument for `make_valid_utf8`"),
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
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
