use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        // st_asbinary strips metadata, so return a plain Binary field
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
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
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(array.clone())),
            other => exec_err!("Unsupported argument type for st_asbinary: {:?}", other),
        }
    }
}
