use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_comet_spark_expr::scalar_funcs::{spark_hex, spark_unhex};
use datafusion_common::{DataFusionError, ScalarValue};

#[derive(Debug)]
pub(crate) struct SparkHex {
    signature: Signature,
}

impl SparkHex {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkHex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_hex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(
                "spark_hex expects exactly one argument".to_string(),
            ));
        }

        match &args[0] {
            ColumnarValue::Array(_) => spark_hex(args),
            ColumnarValue::Scalar(scalar) => {
                let scalar = if let ScalarValue::Int32(value) = scalar {
                    &ScalarValue::Int64(value.map(|v| v as i64))
                } else {
                    scalar
                };
                let array = scalar.to_array()?;
                let array_args = [ColumnarValue::Array(array)];
                spark_hex(&array_args)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct SparkUnHex {
    signature: Signature,
}

impl SparkUnHex {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![DataType::Utf8]),
                    Exact(vec![DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkUnHex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_unhex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        spark_unhex(args)
    }
}
