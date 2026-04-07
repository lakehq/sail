use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::cast::as_binary_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let mut metadata = HashMap::new();
        metadata.insert(
            sail_common::spec::ARROW_EXTENSION_NAME_KEY.to_string(),
            "geoarrow.wkb".to_string(),
        );
        metadata.insert(
            sail_common::spec::ARROW_EXTENSION_METADATA_KEY.to_string(),
            r#"{"crs":"SRID:0"}"#.to_string(),
        );
        Ok(Arc::new(
            Field::new(self.name(), DataType::Binary, true).with_metadata(metadata),
        ))
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
            ColumnarValue::Array(array) => {
                let binary_array = as_binary_array(array)?;
                for (i, opt) in binary_array.iter().enumerate() {
                    if let Some(b) = opt {
                        if let Err(e) = validate_geometry(b) {
                            return exec_err!("Invalid WKB at index {}: {}", i, e);
                        }
                    }
                }
                Ok(ColumnarValue::Array(array.clone()))
            }
            other => exec_err!("Unsupported argument type for st_geomfromwkb: {:?}", other),
        }
    }
}
