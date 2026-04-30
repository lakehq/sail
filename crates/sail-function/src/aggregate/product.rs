use std::any::Any;
use std::fmt::{Debug, Formatter};

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::cast::as_float64_array;
use datafusion::common::downcast_value;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion_common::types::{
    logical_float16, logical_float32, logical_float64, logical_int16, logical_int32, logical_int64,
    logical_int8, logical_uint16, logical_uint32, logical_uint64, logical_uint8, NativeType,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};

#[derive(PartialEq, Eq, Hash)]
pub struct ProductFunction {
    signature: Signature,
}

impl Debug for ProductFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProductFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ProductFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ProductFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_float64()),
                    vec![
                        TypeSignatureClass::Native(logical_int8()),
                        TypeSignatureClass::Native(logical_int16()),
                        TypeSignatureClass::Native(logical_int32()),
                        TypeSignatureClass::Native(logical_int64()),
                        TypeSignatureClass::Native(logical_uint8()),
                        TypeSignatureClass::Native(logical_uint16()),
                        TypeSignatureClass::Native(logical_uint32()),
                        TypeSignatureClass::Native(logical_uint64()),
                        TypeSignatureClass::Native(logical_float16()),
                        TypeSignatureClass::Native(logical_float32()),
                        TypeSignatureClass::Native(logical_float64()),
                    ],
                    NativeType::Float64,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ProductFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "product"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ProductAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new("product", DataType::Float64, true).into()])
    }
}

#[derive(Debug, Default)]
pub struct ProductAccumulator {
    product: Option<f64>,
}

impl ProductAccumulator {
    pub fn new() -> Self {
        Self { product: None }
    }

    fn multiply(&mut self, value: f64) {
        self.product = Some(self.product.unwrap_or(1.0) * value);
    }
}

impl Accumulator for ProductAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = as_float64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.multiply(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(self.product))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64(self.product)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let products = downcast_value!(states[0], Float64Array);
        for value in products.iter().flatten() {
            self.multiply(value);
        }
        Ok(())
    }
}
