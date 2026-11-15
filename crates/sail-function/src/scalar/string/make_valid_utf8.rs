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
    preserve_nullability: bool,
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
            preserve_nullability: false,
        }
    }

    /// The same conversion, but the result is nullable only when the input is. Spark's `upper`
    /// and `lower` convert binary input like this.
    pub fn preserving_nullability() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            preserve_nullability: true,
        }
    }
}

impl ScalarUDFImpl for MakeValidUtf8 {
    fn name(&self) -> &str {
        if self.preserve_nullability {
            "make_valid_utf8_preserving_nullability"
        } else {
            "make_valid_utf8"
        }
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
    /// The variant for `upper` and `lower` follows the input instead.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/stringExpressions.scala#L813>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let Some(input) = args.arg_fields.first() else {
            return exec_err!("expected single argument for `make_valid_utf8`");
        };
        let data_type = match input.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => input.data_type().clone(),
            DataType::Binary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
                DataType::Utf8
            }
            DataType::LargeBinary => DataType::LargeUtf8,
            _ => return exec_err!("expected string array for `make_valid_utf8`"),
        };
        let nullable = !self.preserve_nullability || input.is_nullable();
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn return_nullability(func: &MakeValidUtf8, nullable: bool) -> Result<bool> {
        let input = Field::new("input", DataType::Binary, nullable);
        let result = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[input.into()],
            scalar_arguments: &[None],
        })?;
        assert_eq!(result.data_type(), &DataType::Utf8);
        Ok(result.is_nullable())
    }

    #[test]
    fn make_valid_utf8_is_always_nullable() -> Result<()> {
        let func = MakeValidUtf8::new();
        assert!(return_nullability(&func, false)?);
        assert!(return_nullability(&func, true)?);
        Ok(())
    }

    #[test]
    fn preserving_nullability_follows_input() -> Result<()> {
        let func = MakeValidUtf8::preserving_nullability();
        assert!(!return_nullability(&func, false)?);
        assert!(return_nullability(&func, true)?);
        Ok(())
    }
}
