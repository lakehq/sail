use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow::array::{ArrayRef, AsArray, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{downcast_value, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_common::types::{
    logical_float16, logical_float32, logical_float64, logical_int16, logical_int32, logical_int64,
    logical_int8, logical_uint16, logical_uint32, logical_uint64, logical_uint8, NativeType,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};

#[derive(PartialEq, Eq, Hash)]
pub struct SkewnessFunc {
    name: String,
    signature: Signature,
}

impl Debug for SkewnessFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkewnessFunc")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for SkewnessFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SkewnessFunc {
    pub fn new() -> Self {
        Self {
            name: "skewness".to_string(),
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

impl AggregateUDFImpl for SkewnessFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(SkewnessAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new("n", DataType::Float64, true).into(),
            Field::new("avg", DataType::Float64, true).into(),
            Field::new("m2", DataType::Float64, true).into(),
            Field::new("m3", DataType::Float64, true).into(),
        ])
    }
}

#[derive(Debug, Default)]
pub struct SkewnessAccumulator {
    n: f64,
    avg: f64,
    m2: f64,
    m3: f64,
}

impl SkewnessAccumulator {
    pub fn new() -> Self {
        Self {
            n: 0.0,
            avg: 0.0,
            m2: 0.0,
            m3: 0.0,
        }
    }

    pub fn update_one(&mut self, value: f64) -> (f64, f64, f64, f64) {
        self.n += 1.0;
        let delta = value - self.avg;
        let delta_n = delta / self.n;
        self.avg += delta_n;
        self.m2 += delta * (delta - delta_n);

        let delta2 = delta * delta;
        let delta_n2 = delta_n * delta_n;
        self.m3 = self.m3 - 3.0 * delta_n * self.m2 + delta * (delta2 - delta_n2);
        (delta, delta_n, delta2, delta_n2)
    }

    pub fn merge_one(
        &mut self,
        n_right: f64,
        avg_right: f64,
        m2_right: f64,
        m3_right: f64,
    ) -> (f64, f64) {
        self.n += n_right;

        let delta = avg_right - self.avg;
        let delta_n = if self.n == 0.0 { 0.0 } else { delta / self.n };
        self.avg += delta_n * n_right;

        // higher order moments computed according to:
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
        self.m2 += m2_right + delta * delta_n * self.n * n_right;

        self.m3 += m3_right
            + delta_n * delta_n * delta * self.n * n_right * (self.n - n_right)
            + 3.0 * delta_n * (self.n * m2_right - n_right * self.m2);

        (delta, delta_n)
    }

    pub fn n(&self) -> f64 {
        self.n
    }
    pub fn avg(&self) -> f64 {
        self.avg
    }
    pub fn m2(&self) -> f64 {
        self.m2
    }
    pub fn m3(&self) -> f64 {
        self.m3
    }

    /// Returns NULL when fewer than 2 values have been accumulated,
    /// matching Spark's behavior for skewness and kurtosis.
    /// With n=1, m2 (variance) is zero, causing division by zero in the
    /// skewness formula `sqrt(n) * m3 / m2^(3/2)`, which produces NaN.
    /// Spark returns NULL for n < 2 and computes from n >= 2 onward.
    pub fn null_or_value(&self, value: f64) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(if self.n() < 2.0 {
            None
        } else {
            Some(value)
        }))
    }
}

impl Accumulator for SkewnessAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let array = values[0].as_primitive::<Float64Type>();
        for value in array.iter().flatten() {
            self.update_one(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        self.null_or_value(self.n.sqrt() * self.m3 / (self.m2.powi(3)).sqrt())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.n),
            ScalarValue::from(self.avg),
            ScalarValue::from(self.m2),
            ScalarValue::from(self.m3),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let ns = downcast_value!(states[0], Float64Array);
        let avgs = downcast_value!(states[1], Float64Array);
        let m2s = downcast_value!(states[2], Float64Array);
        let m3s = downcast_value!(states[3], Float64Array);

        for i in 0..ns.len() {
            let n_right = ns.value(i);
            if n_right == 0.0 {
                continue;
            }
            let avg_right = avgs.value(i);
            let m2_right = m2s.value(i);
            let m3_right = m3s.value(i);

            self.merge_one(n_right, avg_right, m2_right, m3_right);
        }
        Ok(())
    }
}
