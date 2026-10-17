use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RaiseError {
    signature: Signature,
    divide_by_zero: bool,
}

impl Default for RaiseError {
    fn default() -> Self {
        Self::new()
    }
}

impl RaiseError {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            divide_by_zero: false,
        }
    }

    pub fn divide_by_zero() -> Self {
        Self {
            divide_by_zero: true,
            ..Self::new()
        }
    }

    pub fn is_divide_by_zero(&self) -> bool {
        self.divide_by_zero
    }

    /// Spark's explicit raise_error is non-foldable; internal value checks remain foldable.
    pub fn non_foldable() -> Self {
        let mut function = Self::new();
        function.signature.volatility = Volatility::Volatile;
        function
    }
}

fn raise_from_strings<'a>(
    mut iter: impl Iterator<Item = Option<&'a str>>,
) -> Result<ColumnarValue> {
    if let Some(message) = iter.find_map(|v| v) {
        Err(DataFusionError::Execution(message.to_string()))
    } else {
        internal_err!("raise_error expects a single UTF-8 string argument")
    }
}

impl ScalarUDFImpl for RaiseError {
    fn name(&self) -> &str {
        "raise_error"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `RaiseError.nullable = true`, unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/misc.scala#L86>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Null, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.divide_by_zero {
            // A higher-order function can invoke a lambda on an empty batch.
            // No row reaches the zero-divisor error in that case.
            if args.number_rows == 0 {
                return Ok(ColumnarValue::Array(ScalarValue::Null.to_array_of_size(0)?));
            }
            return Err(DataFusionError::External(Box::new(
                ArrowError::DivideByZero,
            )));
        }
        let ScalarFunctionArgs { args, .. } = args;
        let Ok(arg) = args.one() else {
            return internal_err!("raise_error should only be called with one argument");
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(message)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(message)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(message))) => {
                Err(DataFusionError::Execution(message))
            }
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8 => raise_from_strings(as_string_array(array.as_ref())?.iter()),
                DataType::LargeUtf8 => {
                    raise_from_strings(as_large_string_array(array.as_ref())?.iter())
                }
                DataType::Utf8View => {
                    raise_from_strings(as_string_view_array(array.as_ref())?.iter())
                }
                _ => internal_err!("raise_error expects a single UTF-8 string argument"),
            },
            _ => internal_err!("raise_error expects a single UTF-8 string argument"),
        }
    }
}
