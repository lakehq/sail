use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RaiseError {
    signature: Signature,
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
        }
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

    /// Spark: `RaiseError` declares `override def nullable: Boolean = true`
    /// (`misc.scala:86`) — unconditionally, not derived from its children. The expression never
    /// returns at all, but its declared type is `NullType` and its declared flag is `true`.
    ///
    /// Declared here rather than left to DataFusion's default: the default happens to agree
    /// today, but nothing pins it, and a change upstream would break parity in silence.
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Null, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
