use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkConv {
    signature: Signature,
}

impl Default for SparkConv {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConv {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int32, DataType::Int32],
                Volatility::Immutable,
            ),
            // signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_conv"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [num, from_base, to_base] = args.as_slice() else {
            return exec_err!("conv() requires 3 arguments, got {}", args.len());
        };

        match (num, from_base, to_base) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(num_str))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(from))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(to))),
            ) => {
                if *from < 2 || *from > 36 || *to < 2 || *to > 36 {
                    // Fixme to review
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                match i64::from_str_radix(num_str, *from as u32) {
                    Ok(n) => {
                        let result = if *to == 10 {
                            format!("{n}")
                        } else {
                            format!("{n:x}")
                        };
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                    }
                    Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))), // conversión inválida
                }
            }

            _ => exec_err!(
                "conv() expects (Utf8, Int32, Int32), got {num:?}, {from_base:?}, {to_base:?}"
            ),
        }
    }
}
