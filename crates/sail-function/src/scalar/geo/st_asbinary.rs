use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// ST_AsBinary - Convert Geometry/Geography to WKB (Binary)
///
/// Input: Binary containing WKB (with geoarrow metadata)
/// Output: Binary (WKB without metadata)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StAsBinary {
    signature: Signature,
}

impl Default for StAsBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl StAsBinary {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StAsBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_asbinary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("st_asbinary requires exactly 1 argument");
        }
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;

        if args.len() != 1 {
            return exec_err!(
                "st_asbinary requires exactly 1 argument, got {}",
                args.len()
            );
        }

        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(b.to_vec()))))
            }
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            other => exec_err!("Unsupported argument type for st_asbinary: {:?}", other),
        }
    }
}
