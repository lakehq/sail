use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use super::wkb_reader::validate_geometry;

/// ST_GeomFromWKB - Convert WKB to Geometry(0)
///
/// Input: Binary containing WKB
/// Output: Binary with type Geometry(0)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StGeomFromWKB {
    signature: Signature,
}

impl Default for StGeomFromWKB {
    fn default() -> Self {
        Self::new()
    }
}

impl StGeomFromWKB {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StGeomFromWKB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_geomfromwkb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("st_geomfromwkb requires exactly 1 argument");
        }
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;

        if args.len() != 1 {
            return exec_err!(
                "st_geomfromwkb requires exactly 1 argument, got {}",
                args.len()
            );
        }

        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => {
                if let Err(e) = validate_geometry(b) {
                    return exec_err!("Invalid WKB: {}", e);
                }
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(b.to_vec()))))
            }
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            other => exec_err!("Unsupported argument type for st_geomfromwkb: {:?}", other),
        }
    }
}
