use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// ST_GeoGFromWKB - Convert WKB to Geography(4326)
///
/// Input: Binary containing WKB
/// Output: Binary (same bytes) - the SRID 4326 is tracked at the type level
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StGeoGFromWKB {
    signature: Signature,
}

impl Default for StGeoGFromWKB {
    fn default() -> Self {
        Self::new()
    }
}

impl StGeoGFromWKB {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StGeoGFromWKB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geogfromwkb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("st_geogfromwkb requires at least 1 argument");
        }
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;

        if args.len() != 1 {
            return exec_err!(
                "st_geogfromwkb requires exactly 1 argument, got {}",
                args.len()
            );
        }

        match &args[0] {
            ColumnarValue::Array(arr) => {
                // Pass through the binary as-is
                Ok(ColumnarValue::Array(arr.clone()))
            }
            ColumnarValue::Scalar(ScalarValue::Binary(Some(wkb))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Binary(Some(wkb.to_vec())),
            )),
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            other => exec_err!("Unsupported argument type for st_geogfromwkb: {:?}", other),
        }
    }
}
