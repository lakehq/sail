use std::any::Any;
use std::fmt::Debug;
use std::ops::{Div, Mul, Sub};

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/skewness.rs>
use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, UInt64Type};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_common::types::logical_float64;
use datafusion_expr_common::signature::TypeSignatureClass;

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
                vec![TypeSignatureClass::Native(logical_float64())],
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

    fn state_fields(&self, _args: StateFieldsArgs) -> datafusion::common::Result<Vec<Field>> {
        Ok(vec![
            Field::new("count", DataType::UInt64, true),
            Field::new("sum", DataType::Float64, true),
            Field::new("sum_sqr", DataType::Float64, true),
            Field::new("sum_cub", DataType::Float64, true),
        ])
    }
}

/// Accumulator for calculating the skewness
/// This implementation follows the DuckDB implementation:
/// <https://github.com/duckdb/duckdb/blob/main/src/core_functions/aggregate/distributive/skew.cpp>
#[derive(Debug)]
pub struct SkewnessAccumulator {
    count: u64,
    sum: f64,
    sum_sqr: f64,
    sum_cub: f64,
}

impl SkewnessAccumulator {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0f64,
            sum_sqr: 0f64,
            sum_cub: 0f64,
        }
    }
}

impl Accumulator for SkewnessAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let array = values[0].as_primitive::<Float64Type>();
        for val in array.iter().flatten() {
            self.count += 1;
            self.sum += val;
            self.sum_sqr += val.powi(2);
            self.sum_cub += val.powi(3);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        if self.count <= 2 {
            return Ok(ScalarValue::Float64(None));
        }
        let count = self.count as f64;
        let t1 = 1f64 / count;
        let p = (t1 * (self.sum_sqr - self.sum * self.sum * t1))
            .powi(3)
            .max(0f64);
        let div = p.sqrt();
        if div == 0f64 {
            return Ok(ScalarValue::Float64(None));
        }
        let t2 = count.mul(count.sub(1f64)).sqrt().div(count.sub(2f64));
        let res = t2
            * t1
            * (self.sum_cub - 3f64 * self.sum_sqr * self.sum * t1
                + 2f64 * self.sum.powi(3) * t1 * t1)
            / div;
        Ok(ScalarValue::Float64(Some(res)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.sum),
            ScalarValue::from(self.sum_sqr),
            ScalarValue::from(self.sum_cub),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let counts = states[0].as_primitive::<UInt64Type>();
        let sums = states[1].as_primitive::<Float64Type>();
        let sum_sqrs = states[2].as_primitive::<Float64Type>();
        let sum_cubs = states[3].as_primitive::<Float64Type>();

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0 {
                continue;
            }
            self.count += c;
            self.sum += sums.value(i);
            self.sum_sqr += sum_sqrs.value(i);
            self.sum_cub += sum_cubs.value(i);
        }
        Ok(())
    }
}
