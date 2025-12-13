/// [Credit]: <https://github.com/apache/datafusion/blob/e6e1eb229440591263c82bb2b913a4d5a16f9b70/datafusion/functions/src/utils.rs>
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

/// Creates a function to identify the optimal return type of a string function given
/// the type of its first argument.
///
/// If the input type is `LargeUtf8` or `LargeBinary` the return type is
/// `$largeUtf8Type`,
///
/// If the input type is `Utf8` or `Binary` the return type is `$utf8Type`,
///
/// If the input type is `Utf8View` the return type is $utf8Type,
macro_rules! get_optimal_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        pub(crate) fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                // LargeBinary inputs are automatically coerced to Utf8
                DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                // Binary inputs are automatically coerced to Utf8
                DataType::Utf8 | DataType::Binary => $utf8Type,
                // Utf8View max offset size is u32::MAX, the same as UTF8
                DataType::Utf8View | DataType::BinaryView => $utf8Type,
                DataType::Null => DataType::Null,
                DataType::Dictionary(_, value_type) => match **value_type {
                    DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                    DataType::Utf8 | DataType::Binary => $utf8Type,
                    DataType::Null => DataType::Null,
                    _ => {
                        return datafusion_common::exec_err!(
                            "The {} function can only accept strings, but got {:?}.",
                            name.to_uppercase(),
                            **value_type
                        );
                    }
                },
                data_type => {
                    return datafusion_common::exec_err!(
                        "The {} function can only accept strings, but got {:?}.",
                        name.to_uppercase(),
                        data_type
                    );
                }
            })
        }
    };
}

// `utf8_to_int_type`: returns either a Int32 or Int64 based on the input type size.
get_optimal_return_type!(utf8_to_int_type, DataType::Int64, DataType::Int32);

/// Creates a scalar function implementation for the given function.
/// * `inner` - the function to be executed
/// * `hints` - hints to be used when expanding scalars to arrays
pub(super) fn make_scalar_function<F>(inner: F, hints: Vec<Hint>) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.clone().into_array(expansion_len)
            })
            .collect::<Result<Vec<_>>>()?;

        let result = (inner)(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;

    use super::*;

    #[allow(clippy::unwrap_used)]
    #[test]
    fn string_to_int_type() {
        let v = utf8_to_int_type(&DataType::Utf8, "test").unwrap();
        assert_eq!(v, DataType::Int32);

        let v = utf8_to_int_type(&DataType::Utf8View, "test").unwrap();
        assert_eq!(v, DataType::Int32);

        let v = utf8_to_int_type(&DataType::LargeUtf8, "test").unwrap();
        assert_eq!(v, DataType::Int64);
    }
}
