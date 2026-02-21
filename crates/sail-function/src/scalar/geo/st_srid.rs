use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_binary_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::geo::ewkb;

/// ST_SRID - Get the SRID from a Geometry/Geography value
///
/// Input: Binary (either plain WKB or EWKB)
/// Output: Int32 (the SRID)
///
/// For columns with a specific SRID (Geometry(N)), the SRID is known from the
/// type and this function should be constant-folded at the plan level.
///
/// For columns with mixed SRID (Geometry(ANY)), the SRID is stored in the
/// EWKB header and we extract it at runtime.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StSrid {
    signature: Signature,
}

impl Default for StSrid {
    fn default() -> Self {
        Self::new()
    }
}

impl StSrid {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StSrid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_srid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("st_srid requires at least 1 argument");
        }
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;

        if args.len() != 1 {
            return exec_err!("st_srid requires exactly 1 argument, got {}", args.len());
        }

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let binary_arr = as_binary_array(arr)?;

                // Process each value - extract SRID from EWKB if present
                let mut results: Vec<Option<i32>> = Vec::with_capacity(binary_arr.len());

                for opt_bytes in binary_arr.iter() {
                    match opt_bytes {
                        Some(bytes) => {
                            if ewkb::is_ewkb(bytes) {
                                // Extract SRID from EWKB
                                match ewkb::ewkb_read_srid(bytes) {
                                    Ok(Some(srid)) => results.push(Some(srid)),
                                    Ok(None) => results.push(Some(0)),
                                    Err(e) => {
                                        return exec_err!("Failed to read SRID from EWKB: {:?}", e)
                                    }
                                }
                            } else {
                                // Plain WKB - no embedded SRID
                                // In practice, this case should be handled at plan time
                                // via constant folding. We return 0 as default.
                                results.push(Some(0));
                            }
                        }
                        None => results.push(None),
                    }
                }

                // Build the result array
                let result_array = Int32Array::from(results);
                Ok(ColumnarValue::Array(Arc::new(result_array)))
            }
            ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => {
                if ewkb::is_ewkb(bytes) {
                    // Extract SRID from EWKB
                    match ewkb::ewkb_read_srid(bytes) {
                        Ok(Some(srid)) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(srid)))),
                        Ok(None) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(0)))),
                        Err(e) => exec_err!("Failed to read SRID from EWKB: {:?}", e),
                    }
                } else {
                    // Plain WKB - should be constant-folded at plan time
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(0))))
                }
            }
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
                // NULL input - return NULL
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)))
            }
            other => exec_err!("Unsupported argument type for st_srid: {:?}", other),
        }
    }
}
