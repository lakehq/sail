use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// ST_GeomFromWKB - Convert WKB to Geometry
///
/// Input: Binary containing WKB, optionally an SRID integer
/// Output: Binary (same bytes)
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
            signature: Signature::variadic(
                vec![DataType::Binary, DataType::Int32],
                Volatility::Immutable,
            ),
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
            return exec_err!("st_geomfromwkb requires at least 1 argument");
        }
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;

        if args.is_empty() {
            return exec_err!("st_geomfromwkb requires at least 1 argument");
        }

        // Handle 1-arg form: st_geomfromwkb(wkb)
        if args.len() == 1 {
            let wkb_arg = &args[0];
            return match wkb_arg {
                ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(arr.clone())),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(wkb))) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Binary(Some(wkb.to_vec())),
                )),
                ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
                }
                other => exec_err!("Unsupported argument type for st_geomfromwkb: {:?}", other),
            };
        }

        // Handle 2-arg form: st_geomfromwkb(wkb, srid)
        // Just pass through the WKB
        if args.len() == 2 {
            let wkb_arg = &args[0];
            return match wkb_arg {
                ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(arr.clone())),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(wkb))) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Binary(Some(wkb.to_vec())),
                )),
                ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
                }
                other => exec_err!("Unsupported argument type for st_geomfromwkb: {:?}", other),
            };
        }

        exec_err!(
            "st_geomfromwkb requires 1 or 2 arguments, got {}",
            args.len()
        )
    }
}
