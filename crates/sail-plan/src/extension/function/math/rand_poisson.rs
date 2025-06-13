use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand_distr::{Distribution, Poisson};

#[derive(Debug)]
pub struct RandPoisson {
    signature: Signature,
}

impl Default for RandPoisson {
    fn default() -> Self {
        Self::new()
    }
}

impl RandPoisson {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for RandPoisson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rand_poisson"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(&args.args)?;
        let i64s = as_int64_array(&args[0])?;

        let mut rng = rand::rng();

        let new_array = i64s
            .iter()
            .map(|array_elem| {
                array_elem.map(|value| {
                    Poisson::new(value as f64) // 🧠 convierte a f64 si es i64
                        .unwrap()
                        .sample(&mut rng) as i64
                })
            })
            .collect::<Int64Array>();

        Ok(ColumnarValue::from(Arc::new(new_array) as ArrayRef))
    }
}
