use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// The Spark `isnan` function, which is true for a NaN value and false otherwise.
/// A NULL value is not NaN, so the result is never NULL, unlike the DataFusion `isnan`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIsNaN {
    signature: Signature,
}

impl Default for SparkIsNaN {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIsNaN {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkIsNaN {
    fn name(&self) -> &str {
        "spark_isnan"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("isnan requires 1 argument");
        };
        match value {
            ColumnarValue::Scalar(value) => {
                let is_nan = match value {
                    ScalarValue::Float64(Some(v)) => v.is_nan(),
                    ScalarValue::Float32(Some(v)) => v.is_nan(),
                    _ => false,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(is_nan))))
            }
            ColumnarValue::Array(array) => {
                let result: BooleanArray = match array.data_type() {
                    DataType::Float64 => array
                        .as_primitive::<Float64Type>()
                        .iter()
                        .map(|v| Some(v.is_some_and(f64::is_nan)))
                        .collect(),
                    DataType::Float32 => array
                        .as_primitive::<Float32Type>()
                        .iter()
                        .map(|v| Some(v.is_some_and(f32::is_nan)))
                        .collect(),
                    _ => BooleanArray::from(vec![false; array.len()]),
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}
