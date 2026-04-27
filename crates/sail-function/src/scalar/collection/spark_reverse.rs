/// Spark-compatible `reverse` function.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#reverse>
use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::functions::unicode::reverse::ReverseFunc;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions_nested::reverse::array_reverse_inner;

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkReverse {
    signature: Signature,
}

impl Default for SparkReverse {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkReverse {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn is_array_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

fn is_string_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn is_binary_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Binary | DataType::LargeBinary | DataType::BinaryView)
}

fn reverse_binary(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arg = &args[0];
    match arg {
        ColumnarValue::Scalar(ScalarValue::Binary(None))
        | ColumnarValue::Scalar(ScalarValue::LargeBinary(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => {
            let reversed: Vec<u8> = bytes.iter().copied().rev().collect();
            let s = String::from_utf8_lossy(&reversed).into_owned();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))))
        }
        ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))) => {
            let reversed: Vec<u8> = bytes.iter().copied().rev().collect();
            let s = String::from_utf8_lossy(&reversed).into_owned();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))))
        }
        ColumnarValue::Array(array) => {
            let mut builder = StringBuilder::new();
            match array.data_type() {
                DataType::Binary => {
                    let arr = array.as_binary::<i32>();
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let reversed: Vec<u8> =
                                arr.value(i).iter().copied().rev().collect();
                            builder.append_value(String::from_utf8_lossy(&reversed).as_ref());
                        }
                    }
                }
                DataType::LargeBinary => {
                    let arr = array.as_binary::<i64>();
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let reversed: Vec<u8> =
                                arr.value(i).iter().copied().rev().collect();
                            builder.append_value(String::from_utf8_lossy(&reversed).as_ref());
                        }
                    }
                }
                DataType::BinaryView => {
                    let arr = array.as_binary_view();
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let reversed: Vec<u8> =
                                arr.value(i).iter().copied().rev().collect();
                            builder.append_value(String::from_utf8_lossy(&reversed).as_ref());
                        }
                    }
                }
                other => {
                    return exec_err!(
                        "reverse: unexpected binary type in invoke: {other}"
                    )
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
        other => exec_err!("reverse: unexpected binary scalar value: {other:?}"),
    }
}

impl ScalarUDFImpl for SparkReverse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            dt if is_array_type(dt) => Ok(dt.clone()),
            _ => Ok(DataType::Utf8),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return plan_err!(
                "reverse requires exactly 1 argument, got {}",
                arg_types.len()
            );
        }
        let dt = &arg_types[0];
        if is_array_type(dt) || is_string_type(dt) || is_binary_type(dt) {
            // Keep as-is: arrays are reversed as arrays, strings/binary handled directly.
            Ok(vec![dt.clone()])
        } else if matches!(dt, DataType::Null) {
            // Untyped NULL: treat as string so DataFusion casts it to Utf8 NULL.
            Ok(vec![DataType::Utf8])
        } else if matches!(dt, DataType::Map(_, _) | DataType::Struct(_)) {
            plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] \
                 The argument to the `reverse` function must be a string, binary, or array type, \
                 not `{dt}`."
            )
        } else {
            // Numeric, temporal, boolean, etc. — Spark casts to STRING first.
            Ok(vec![DataType::Utf8])
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("reverse requires exactly 1 argument");
        }
        let dt = args.args[0].data_type();
        if is_string_type(&dt) {
            ReverseFunc::new().invoke_with_args(args)
        } else if is_binary_type(&dt) {
            reverse_binary(&args.args)
        } else if is_array_type(&dt) {
            make_scalar_function(array_reverse_inner)(&args.args)
        } else {
            exec_err!("reverse: unsupported type {dt}")
        }
    }
}
