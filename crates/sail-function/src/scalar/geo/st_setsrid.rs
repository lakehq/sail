use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::BinaryBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_binary_array, as_int32_array};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::geo::ewkb;

/// ST_SetSRID - Set the SRID on a Geometry/Geography value
///
/// Input: Binary (either plain WKB or EWKB), Int32 (the new SRID)
/// Output: Binary (with updated SRID)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StSetSrid {
    signature: Signature,
}

impl Default for StSetSrid {
    fn default() -> Self {
        Self::new()
    }
}

impl StSetSrid {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Int32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StSetSrid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_setsrid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return exec_err!("st_setsrid requires exactly 2 arguments");
        }
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;

        if args.len() != 2 {
            return exec_err!(
                "st_setsrid requires exactly 2 arguments, got {}",
                args.len()
            );
        }

        let geo_arg = &args[0];
        let srid_arg = &args[1];

        match (geo_arg, srid_arg) {
            (ColumnarValue::Array(geo_arr), ColumnarValue::Array(srid_arr)) => {
                let binary_arr = as_binary_array(geo_arr)?;
                let int32_arr = as_int32_array(srid_arr)?;

                let mut builder = BinaryBuilder::new();

                for (opt_wkb, opt_srid) in binary_arr.iter().zip(int32_arr.iter()) {
                    match (opt_wkb, opt_srid) {
                        (Some(bytes), Some(srid)) => {
                            if ewkb::is_ewkb(bytes) {
                                match ewkb::ewkb_set_srid(bytes, srid) {
                                    Ok(updated) => builder.append_value(&updated),
                                    Err(_) => builder.append_value(bytes),
                                }
                            } else {
                                match ewkb::wkb_to_ewkb(bytes, srid) {
                                    Ok(ewkb_bytes) => builder.append_value(&ewkb_bytes),
                                    Err(_) => builder.append_value(bytes),
                                }
                            }
                        }
                        (Some(bytes), None) => builder.append_value(bytes),
                        (None, _) => builder.append_null(),
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))),
                ColumnarValue::Scalar(ScalarValue::Int32(srid_val)),
            ) => match srid_val {
                Some(srid) => {
                    if ewkb::is_ewkb(bytes) {
                        match ewkb::ewkb_set_srid(bytes, *srid) {
                            Ok(updated) => {
                                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(updated))))
                            }
                            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
                                bytes.to_vec(),
                            )))),
                        }
                    } else {
                        match ewkb::wkb_to_ewkb(bytes, *srid) {
                            Ok(ewkb_bytes) => {
                                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(ewkb_bytes))))
                            }
                            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
                                bytes.to_vec(),
                            )))),
                        }
                    }
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
            },
            (ColumnarValue::Scalar(ScalarValue::Binary(None)), _) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            (_, ColumnarValue::Scalar(ScalarValue::Int32(None))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            other => exec_err!("Unsupported argument types for st_setsrid: {:?}", other),
        }
    }
}
