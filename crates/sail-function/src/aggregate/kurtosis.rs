use std::any::Any;
use std::fmt::Debug;

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

use crate::aggregate::skewness::SkewnessAccumulator;

#[derive(PartialEq, Eq, Hash)]
pub struct KurtosisFunction {
    signature: Signature,
}

impl Debug for KurtosisFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KurtosisFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for KurtosisFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl KurtosisFunction {
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

impl AggregateUDFImpl for KurtosisFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kurtosis"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(KurtosisAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new("n", DataType::Float64, true).into(),
            Field::new("avg", DataType::Float64, true).into(),
            Field::new("m2", DataType::Float64, true).into(),
            Field::new("m3", DataType::Float64, true).into(),
            Field::new("m4", DataType::Float64, true).into(),
        ])
    }
}

#[derive(Debug, Default)]
pub struct KurtosisAccumulator {
    s: SkewnessAccumulator,
    m4: f64,
}

impl KurtosisAccumulator {
    pub fn new() -> Self {
        Self {
            s: SkewnessAccumulator::new(),
            m4: 0.0,
        }
    }
}

impl Accumulator for KurtosisAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = as_float64_array(&values[0])?;
        for value in array.iter().flatten() {
            let (delta, delta_n, delta2, delta_n2) = self.s.update_one(value);
            self.m4 += -4.0 * delta_n * self.s.m3() - 6.0 * delta_n2 * self.s.m2()
                + delta * (delta * delta2 - delta_n * delta_n2);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.s
            .null_or_value(self.s.n() * self.m4 / self.s.m2().powi(2) - 3.0)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok([self.s.state()?.as_slice(), [self.m4.into()].as_slice()].concat())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let ns = downcast_value!(states[0], Float64Array);
        let avgs = downcast_value!(states[1], Float64Array);
        let m2s = downcast_value!(states[2], Float64Array);
        let m3s = downcast_value!(states[3], Float64Array);
        let m4s = downcast_value!(states[4], Float64Array);

        for i in 0..ns.len() {
            let n2 = ns.value(i);
            if n2 == 0.0 {
                continue;
            }

            let n1 = self.s.n();
            let m2_left = self.s.m2();
            let m3_left = self.s.m3();

            let avg_right = avgs.value(i);
            let m2_right = m2s.value(i);
            let m3_right = m3s.value(i);
            let m4_right = m4s.value(i);

            let (delta, delta_n) = self.s.merge_one(n2, avg_right, m2_right, m3_right);

            // higher order moments computed according to:
            // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
            self.m4 += m4_right
                + delta_n * delta_n * delta_n * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2)
                + 6.0 * delta_n * delta_n * (n1 * n1 * m2_right + n2 * n2 * m2_left)
                + 4.0 * delta_n * (n1 * m3_right - n2 * m3_left);
        }

        Ok(())
    }
}
