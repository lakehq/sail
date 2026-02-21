use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::BinaryBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_binary_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::geo::ewkb;

/// ST_AsBinary - Convert Geometry/Geography to WKB binary
///
/// Input: Binary (either plain WKB or EWKB depending on column type)
/// Output: Binary (plain WKB, always)
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
            return exec_err!("st_asbinary requires at least 1 argument");
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
            ColumnarValue::Array(arr) => {
                let binary_arr = as_binary_array(arr)?;

                // Process each value - convert EWKB to plain WKB if needed
                let mut builder = BinaryBuilder::new();

                for opt_bytes in binary_arr.iter() {
                    match opt_bytes {
                        Some(bytes) => {
                            if ewkb::is_ewkb(bytes) {
                                // Convert EWKB to WKB
                                match ewkb::ewkb_to_wkb(bytes) {
                                    Ok(wkb) => builder.append_value(&wkb),
                                    Err(_) => {
                                        // If conversion fails, just use as-is
                                        builder.append_value(bytes);
                                    }
                                };
                            } else {
                                // Already plain WKB, use as-is
                                builder.append_value(bytes);
                            }
                        }
                        None => builder.append_null(),
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => {
                if ewkb::is_ewkb(bytes) {
                    // Convert EWKB to WKB
                    match ewkb::ewkb_to_wkb(bytes) {
                        Ok(wkb) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(wkb)))),
                        Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
                            bytes.to_vec(),
                        )))),
                    }
                } else {
                    // Already plain WKB
                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
                        bytes.to_vec(),
                    ))))
                }
            }
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            other => exec_err!("Unsupported argument type for st_asbinary: {:?}", other),
        }
    }
}
