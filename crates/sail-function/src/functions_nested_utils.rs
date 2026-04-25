/// [Credit]: <https://github.com/apache/datafusion/blob/94d178ebe9674669b32ecd7896b5597f49e90791/datafusion/functions-nested/src/utils.rs>
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;
macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!("could not cast to {}", type_name::<$ARRAY_TYPE>()))
        })?
    }};
}

macro_rules! opt_downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>()
    }};
}

pub(crate) use {downcast_arg, opt_downcast_arg};

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(
    inner: F,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}
