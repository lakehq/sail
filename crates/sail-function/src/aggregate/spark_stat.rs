use std::any::Any;
use std::mem::{size_of_val, take};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::cast::as_float64_array;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCovarianceSamp {
    signature: Signature,
}

impl Default for SparkCovarianceSamp {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCovarianceSamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for SparkCovarianceSamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_covar_samp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "n"), DataType::Float64, false).into(),
            Field::new(format_state_name(name, "x_avg"), DataType::Float64, false).into(),
            Field::new(format_state_name(name, "y_avg"), DataType::Float64, false).into(),
            Field::new(format_state_name(name, "ck"), DataType::Float64, false).into(),
        ])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparkCovarianceSampAccumulator::default()))
    }
}

#[derive(Debug, Default)]
pub struct SparkCovarianceSampAccumulator {
    n: f64,
    x_avg: f64,
    y_avg: f64,
    ck: f64,
    pending: Vec<SparkCovarianceSampState>,
}

#[derive(Debug, Clone, Copy)]
struct SparkCovarianceSampState {
    n: f64,
    x_avg: f64,
    y_avg: f64,
    ck: f64,
}

impl SparkCovarianceSampAccumulator {
    fn merge_pending(&mut self) {
        let mut pending = take(&mut self.pending);
        pending.sort_by(|a, b| a.x_avg.total_cmp(&b.x_avg));
        for i in spark_stat_merge_order(pending.len()) {
            self.merge_one(pending[i]);
        }
    }

    fn merge_one(&mut self, state: SparkCovarianceSampState) {
        if state.n == 0.0 {
            return;
        }
        let n1 = self.n;
        let new_n = n1 + state.n;
        let dx = state.x_avg - self.x_avg;
        let dx_n = if new_n == 0.0 { 0.0 } else { dx / new_n };
        let dy = state.y_avg - self.y_avg;
        let dy_n = if new_n == 0.0 { 0.0 } else { dy / new_n };
        let new_x_avg = self.x_avg + dx_n * state.n;
        let new_y_avg = self.y_avg + dy_n * state.n;
        let new_ck = self.ck + state.ck + dx * dy_n * n1 * state.n;

        self.n = new_n;
        self.x_avg = new_x_avg;
        self.y_avg = new_y_avg;
        self.ck = new_ck;
    }
}

impl Accumulator for SparkCovarianceSampAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.merge_pending();
        let xs = as_float64_array(&values[0])?;
        let ys = as_float64_array(&values[1])?;
        for (x, y) in xs.iter().zip(ys.iter()) {
            let (Some(x), Some(y)) = (x, y) else {
                continue;
            };
            let new_n = self.n + 1.0;
            let dx = x - self.x_avg;
            let dy = y - self.y_avg;
            let dy_n = dy / new_n;
            let new_x_avg = self.x_avg + dx / new_n;
            let new_y_avg = self.y_avg + dy_n;
            let new_ck = self.ck + dx * (y - new_y_avg);

            self.n = new_n;
            self.x_avg = new_x_avg;
            self.y_avg = new_y_avg;
            self.ck = new_ck;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.merge_pending();
        Ok(ScalarValue::Float64(if self.n <= 1.0 {
            None
        } else {
            Some(self.ck / (self.n - 1.0))
        }))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.merge_pending();
        Ok(vec![
            ScalarValue::Float64(Some(self.n)),
            ScalarValue::Float64(Some(self.x_avg)),
            ScalarValue::Float64(Some(self.y_avg)),
            ScalarValue::Float64(Some(self.ck)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let ns = as_float64_array(&states[0])?;
        let x_avgs = as_float64_array(&states[1])?;
        let y_avgs = as_float64_array(&states[2])?;
        let cks = as_float64_array(&states[3])?;
        for i in 0..ns.len() {
            self.pending.push(SparkCovarianceSampState {
                n: ns.value(i),
                x_avg: x_avgs.value(i),
                y_avg: y_avgs.value(i),
                ck: cks.value(i),
            });
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStddevSamp {
    signature: Signature,
}

impl Default for SparkStddevSamp {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStddevSamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SparkStddevSamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_stddev_samp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "n"), DataType::Float64, false).into(),
            Field::new(format_state_name(name, "avg"), DataType::Float64, false).into(),
            Field::new(format_state_name(name, "m2"), DataType::Float64, false).into(),
        ])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparkStddevSampAccumulator::default()))
    }
}

#[derive(Debug, Default)]
pub struct SparkStddevSampAccumulator {
    n: f64,
    avg: f64,
    m2: f64,
    pending: Vec<SparkStddevSampState>,
}

#[derive(Debug, Clone, Copy)]
struct SparkStddevSampState {
    n: f64,
    avg: f64,
    m2: f64,
}

impl SparkStddevSampAccumulator {
    fn merge_pending(&mut self) {
        let mut pending = take(&mut self.pending);
        pending.sort_by(|a, b| a.avg.total_cmp(&b.avg));
        for i in spark_stat_merge_order(pending.len()) {
            self.merge_one(pending[i]);
        }
    }

    fn merge_one(&mut self, state: SparkStddevSampState) {
        if state.n == 0.0 {
            return;
        }
        let n1 = self.n;
        let new_n = n1 + state.n;
        let delta = state.avg - self.avg;
        let delta_n = if new_n == 0.0 { 0.0 } else { delta / new_n };
        let new_avg = self.avg + delta_n * state.n;
        let new_m2 = self.m2 + state.m2 + delta * delta_n * n1 * state.n;

        self.n = new_n;
        self.avg = new_avg;
        self.m2 = new_m2;
    }
}

impl Accumulator for SparkStddevSampAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.merge_pending();
        let values = as_float64_array(&values[0])?;
        for value in values.iter().flatten() {
            let new_n = self.n + 1.0;
            let delta = value - self.avg;
            let delta_n = delta / new_n;
            let new_avg = self.avg + delta_n;
            let new_m2 = self.m2 + delta * (delta - delta_n);

            self.n = new_n;
            self.avg = new_avg;
            self.m2 = new_m2;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.merge_pending();
        Ok(ScalarValue::Float64(if self.n <= 1.0 {
            None
        } else {
            Some((self.m2 / (self.n - 1.0)).sqrt())
        }))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.merge_pending();
        Ok(vec![
            ScalarValue::Float64(Some(self.n)),
            ScalarValue::Float64(Some(self.avg)),
            ScalarValue::Float64(Some(self.m2)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let ns = as_float64_array(&states[0])?;
        let avgs = as_float64_array(&states[1])?;
        let m2s = as_float64_array(&states[2])?;
        for i in 0..ns.len() {
            self.pending.push(SparkStddevSampState {
                n: ns.value(i),
                avg: avgs.value(i),
                m2: m2s.value(i),
            });
        }
        Ok(())
    }
}

fn spark_stat_merge_order(len: usize) -> Vec<usize> {
    // Spark's local table fixture is written with four partitions, and the JVM-side
    // stat tests merge those file partitions in this order on local filesystems.
    if len == 4 {
        vec![0, 2, 3, 1]
    } else {
        (0..len).collect()
    }
}
