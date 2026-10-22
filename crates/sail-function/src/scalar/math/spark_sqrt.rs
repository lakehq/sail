use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Float64Array};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSqrt {
    signature: Signature,
}

impl Default for SparkSqrt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSqrt {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSqrt {
    fn name(&self) -> &str {
        "spark_sqrt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("sqrt expects exactly one argument");
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(value.map(f64::sqrt)),
            )),
            ColumnarValue::Array(array) if array.data_type() == &DataType::Float64 => {
                let values: Float64Array = array.as_primitive::<Float64Type>().unary(f64::sqrt);
                Ok(ColumnarValue::Array(Arc::new(values) as ArrayRef))
            }
            other => exec_err!(
                "sqrt expects a double argument, got {:?}",
                other.data_type()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn negative_input_returns_nan() -> Result<()> {
        let result = SparkSqrt::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(-1.0)))],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Float64, false))],
            number_rows: 1,
            return_field: Arc::new(Field::new("sqrt", DataType::Float64, false)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) = result else {
            return exec_err!("sqrt should return a Float64 scalar");
        };
        assert!(value.is_nan());
        Ok(())
    }
}
