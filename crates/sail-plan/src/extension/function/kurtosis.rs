use std::any::Any;
use std::fmt::Debug;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/kurtosis.rs>
use datafusion::arrow::array::{ArrayRef, Float64Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::cast::as_float64_array;
use datafusion::common::{downcast_value, DataFusionError};
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion_common::types::logical_float64;
use datafusion_expr_common::signature::TypeSignatureClass;

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
                vec![TypeSignatureClass::Native(logical_float64())],
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

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new("count", DataType::UInt64, true),
            Field::new("sum", DataType::Float64, true),
            Field::new("sum_sqr", DataType::Float64, true),
            Field::new("sum_cub", DataType::Float64, true),
            Field::new("sum_four", DataType::Float64, true),
        ])
    }
}

/// Accumulator for calculating the excess kurtosis (Fisher’s definition) with bias correction according to the sample size.
/// This implementation follows the [DuckDB implementation]:
/// <https://github.com/duckdb/duckdb/blob/main/src/core_functions/aggregate/distributive/kurtosis.cpp>
#[derive(Debug, Default)]
pub struct KurtosisAccumulator {
    count: u64,
    sum: f64,
    sum_sqr: f64,
    sum_cub: f64,
    sum_four: f64,
}

impl KurtosisAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sqr: 0.0,
            sum_cub: 0.0,
            sum_four: 0.0,
        }
    }
}

impl Accumulator for KurtosisAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = as_float64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.count += 1;
            self.sum += value;
            self.sum_sqr += value.powi(2);
            self.sum_cub += value.powi(3);
            self.sum_four += value.powi(4);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let sums = downcast_value!(states[1], Float64Array);
        let sum_sqrs = downcast_value!(states[2], Float64Array);
        let sum_cubs = downcast_value!(states[3], Float64Array);
        let sum_fours = downcast_value!(states[4], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0 {
                continue;
            }
            self.count += c;
            self.sum += sums.value(i);
            self.sum_sqr += sum_sqrs.value(i);
            self.sum_cub += sum_cubs.value(i);
            self.sum_four += sum_fours.value(i);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count <= 3 {
            return Ok(ScalarValue::Float64(None));
        }

        let count_64 = 1_f64 / self.count as f64;
        let m4 = count_64
            * (self.sum_four - 4.0 * self.sum_cub * self.sum * count_64
                + 6.0 * self.sum_sqr * self.sum.powi(2) * count_64.powi(2)
                - 3.0 * self.sum.powi(4) * count_64.powi(3));

        let m2 = (self.sum_sqr - self.sum.powi(2) * count_64) * count_64;
        if m2 <= 0.0 {
            return Ok(ScalarValue::Float64(None));
        }

        let count = self.count as f64;
        let numerator = (count - 1.0) * ((count + 1.0) * m4 / m2.powi(2) - 3.0 * (count - 1.0));
        let denominator = (count - 2.0) * (count - 3.0);

        let target = numerator / denominator;

        Ok(ScalarValue::Float64(Some(target)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.sum),
            ScalarValue::from(self.sum_sqr),
            ScalarValue::from(self.sum_cub),
            ScalarValue::from(self.sum_four),
        ])
    }
}
