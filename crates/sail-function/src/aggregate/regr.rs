//! Spark-compatible linear regression aggregate functions.
//!
//! The UDAF skeleton is derived from the DataFusion implementation.
//! [Credit]: <https://github.com/apache/datafusion/blob/db2d21e094b1be2ff27653ce58ba1317e2237b01/datafusion/functions-aggregate/src/regr.rs>
//!
//! The accumulator arithmetic intentionally mirrors, operation by operation, the
//! aggregate buffers Spark uses for these functions, so that the results are
//! bit-identical to Spark for the same input order:
//! - `Covariance` for `regr_slope`, `regr_intercept`, and `regr_sxy`
//! - `CentralMomentAgg` (via `VariancePop` and `RegrReplacement`) for `regr_slope`,
//!   `regr_intercept`, `regr_sxx`, and `regr_syy`
//! - `PearsonCorrelation` for `regr_r2`
//! - `Average` for `regr_avgx` and `regr_avgy`
//!
//! The DataFusion formulas are algebraically equivalent, but floating-point
//! rounding differs in the last bits whenever an expression is rearranged
//! (e.g. `ck / m2` vs `(ck / n) / (m2 / n)`), so do not "simplify" any of the
//! update, merge, or evaluate expressions below.

use std::any::Any;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_float64_array, as_uint64_array};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Regr {
    signature: Signature,
    regr_type: RegrType,
    func_name: &'static str,
}

impl Regr {
    pub fn new(regr_type: RegrType, func_name: &'static str) -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            regr_type,
            func_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub enum RegrType {
    /// Variant for `regr_slope` aggregate expression
    /// Returns the slope of the linear regression line for non-null pairs in aggregate columns.
    /// Given input column Y and X: `regr_slope(Y, X)` returns the slope (k in Y = k*X + b) using minimal
    /// RSS (Residual Sum of Squares) fitting.
    Slope,
    /// Variant for `regr_intercept` aggregate expression
    /// Returns the intercept of the linear regression line for non-null pairs in aggregate columns.
    /// Given input column Y and X: `regr_intercept(Y, X)` returns the intercept (b in Y = k*X + b) using minimal
    /// RSS fitting.
    Intercept,
    /// Variant for `regr_count` aggregate expression
    /// Returns the number of input rows for which both expressions are not null.
    /// Given input column Y and X: `regr_count(Y, X)` returns the count of non-null pairs.
    Count,
    /// Variant for `regr_r2` aggregate expression
    /// Returns the coefficient of determination (R-squared value) of the linear regression line for non-null pairs in aggregate columns.
    /// The R-squared value represents the proportion of variance in Y that is predictable from X.
    R2,
    /// Variant for `regr_avgx` aggregate expression
    /// Returns the average of the independent variable for non-null pairs in aggregate columns.
    /// Given input column X: `regr_avgx(Y, X)` returns the average of X values.
    AvgX,
    /// Variant for `regr_avgy` aggregate expression
    /// Returns the average of the dependent variable for non-null pairs in aggregate columns.
    /// Given input column Y: `regr_avgy(Y, X)` returns the average of Y values.
    AvgY,
    /// Variant for `regr_sxx` aggregate expression
    /// Returns the sum of squares of the independent variable for non-null pairs in aggregate columns.
    /// Given input column X: `regr_sxx(Y, X)` returns the sum of squares of deviations of X from its mean.
    Sxx,
    /// Variant for `regr_syy` aggregate expression
    /// Returns the sum of squares of the dependent variable for non-null pairs in aggregate columns.
    /// Given input column Y: `regr_syy(Y, X)` returns the sum of squares of deviations of Y from its mean.
    Syy,
    /// Variant for `regr_sxy` aggregate expression
    /// Returns the sum of products of pairs of numbers for non-null pairs in aggregate columns.
    /// Given input column Y and X: `regr_sxy(Y, X)` returns the sum of products of the deviations of Y and X from their respective means.
    Sxy,
}

impl AggregateUDFImpl for Regr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.func_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        if self.regr_type == RegrType::Count {
            Ok(DataType::UInt64)
        } else {
            Ok(DataType::Float64)
        }
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(RegrAccumulator::try_new(&self.regr_type)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok([
            ("count", DataType::UInt64),
            ("mean_x", DataType::Float64),
            ("mean_y", DataType::Float64),
            ("ck", DataType::Float64),
            ("corr_ck", DataType::Float64),
            ("m2_x", DataType::Float64),
            ("m2_y", DataType::Float64),
            ("corr_m2_x", DataType::Float64),
            ("corr_m2_y", DataType::Float64),
            ("sum_x", DataType::Float64),
            ("sum_y", DataType::Float64),
        ]
        .into_iter()
        .map(|(name, data_type)| {
            Arc::new(Field::new(
                format_state_name(args.name, name),
                data_type,
                true,
            ))
        })
        .collect())
    }
}

/// `RegrAccumulator` is used to compute linear regression aggregate functions
/// by maintaining statistics needed to compute them in an online fashion.
///
/// The statistics replicate the aggregate buffers of Spark's `Covariance`,
/// `CentralMomentAgg`, `PearsonCorrelation`, and `Average` expressions, with
/// the exact same floating-point operation order for updates and merges:
/// <https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/linearRegression.scala>
/// <https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Covariance.scala>
/// <https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/CentralMomentAgg.scala>
/// <https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Corr.scala>
///
/// All of these are variants of Welford's online algorithm for calculating
/// variance and covariance:
/// <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm>
///
/// Given the statistics, the aggregate functions are calculated as in Spark:
///
/// - `regr_slope(y, x)`: Slope of the linear regression line, calculated as:
///   ck / m2_x (`RegrSlope` divides the un-normalized covariance by the
///   un-normalized variance, without dividing each by the count first).
///   It represents the expected change in Y for a one-unit change in X.
///
/// - `regr_intercept(y, x)`: Intercept of the linear regression line, calculated as:
///   mean_y - ck / m2_x * mean_x (`RegrIntercept`).
///   It represents the expected value of Y when X is 0.
///
/// - `regr_count(y, x)`: Count of the non-null(both x and y) input rows.
///
/// - `regr_r2(y, x)`: R-squared value (coefficient of determination), calculated as:
///   (corr_ck / sqrt(corr_m2_y * corr_m2_x)) ^ 2 (`RegrR2` squares the Pearson
///   correlation coefficient).
///   It provides a measure of how well the model's predictions match the observed data.
///
/// - `regr_avgx(y, x)`: Average of the independent variable X, calculated as:
///   sum_x / count (`RegrAvgX` is a plain `Average`, not an incremental mean).
///
/// - `regr_avgy(y, x)`: Average of the dependent variable Y, calculated as:
///   sum_y / count.
///
/// - `regr_sxx(y, x)`: Sum of squares of the independent variable X, calculated as:
///   m2_x.
///
/// - `regr_syy(y, x)`: Sum of squares of the dependent variable Y, calculated as:
///   m2_y.
///
/// - `regr_sxy(y, x)`: Sum of products of paired values, calculated as:
///   corr_ck.
///
/// `ck`/`corr_ck` and `m2_*`/`corr_m2_*` hold the same statistic computed with
/// the two different expression orders of Spark's `Covariance`/`CentralMomentAgg`
/// and `PearsonCorrelation` buffers, which can differ in the last bits.
#[derive(Debug)]
pub struct RegrAccumulator {
    count: u64,
    /// Mean of X; all Spark buffers update the means identically.
    mean_x: f64,
    /// Mean of Y.
    mean_y: f64,
    /// `Covariance(x, y).ck` from `CovPopulation(right, left)` in `RegrSlope`/`RegrIntercept`.
    ck: f64,
    /// `Covariance(y, x).ck` from `RegrSXY`, which is identical to
    /// `PearsonCorrelation(y, x).ck` from `RegrR2`.
    corr_ck: f64,
    /// `CentralMomentAgg(x).m2` from `VariancePop(x)` in `RegrSlope`/`RegrIntercept`
    /// and `RegrReplacement(x)` in `RegrSXX`.
    m2_x: f64,
    /// `CentralMomentAgg(y).m2` from `RegrReplacement(y)` in `RegrSYY`.
    m2_y: f64,
    /// `PearsonCorrelation(y, x).yMk` (the second moment of X) from `RegrR2`.
    corr_m2_x: f64,
    /// `PearsonCorrelation(y, x).xMk` (the second moment of Y) from `RegrR2`.
    corr_m2_y: f64,
    /// `Average.sum` over X from `RegrAvgX`.
    sum_x: f64,
    /// `Average.sum` over Y from `RegrAvgY`.
    sum_y: f64,
    regr_type: RegrType,
}

impl RegrAccumulator {
    /// Creates a new `RegrAccumulator`
    pub fn try_new(regr_type: &RegrType) -> Result<Self> {
        Ok(Self {
            count: 0_u64,
            mean_x: 0_f64,
            mean_y: 0_f64,
            ck: 0_f64,
            corr_ck: 0_f64,
            m2_x: 0_f64,
            m2_y: 0_f64,
            corr_m2_x: 0_f64,
            corr_m2_y: 0_f64,
            sum_x: 0_f64,
            sum_y: 0_f64,
            regr_type: regr_type.clone(),
        })
    }
}

impl Accumulator for RegrAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // regr_slope(Y, X) calculates k in y = k*x + b
        let values_y = as_float64_array(&values[0])?;
        let values_x = as_float64_array(&values[1])?;

        for (value_y, value_x) in values_y.iter().zip(values_x) {
            // skip either x or y is NULL
            let (value_y, value_x) = match (value_y, value_x) {
                (Some(y), Some(x)) => (y, x),
                // skip either x or y is NULL
                _ => continue,
            };

            // The update expressions of Spark's `Covariance`, `CentralMomentAgg`,
            // and `PearsonCorrelation` buffers, which all share the same mean
            // updates but accumulate the second moments differently.
            let new_n = self.count as f64 + 1.0;
            let delta_x = value_x - self.mean_x;
            let delta_x_n = delta_x / new_n;
            let delta_y = value_y - self.mean_y;
            let delta_y_n = delta_y / new_n;
            let new_mean_x = self.mean_x + delta_x_n;
            let new_mean_y = self.mean_y + delta_y_n;

            self.count += 1;
            self.ck += delta_x * (value_y - new_mean_y);
            self.corr_ck += delta_y * (value_x - new_mean_x);
            self.m2_x += delta_x * (delta_x - delta_x_n);
            self.m2_y += delta_y * (delta_y - delta_y_n);
            self.corr_m2_x += delta_x * (value_x - new_mean_x);
            self.corr_m2_y += delta_y * (value_y - new_mean_y);
            self.sum_x += value_x;
            self.sum_y += value_y;
            self.mean_x = new_mean_x;
            self.mean_y = new_mean_y;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let nullif_or_stat = |cond: bool, stat: f64| {
            if cond {
                Ok(ScalarValue::Float64(None))
            } else {
                Ok(ScalarValue::Float64(Some(stat)))
            }
        };

        match self.regr_type {
            RegrType::Slope => {
                // Spark `RegrSlope` returns NULL for 0/1 point or zero variance
                // of x, both of which leave `m2_x` at exactly 0.0.
                nullif_or_stat(self.m2_x == 0.0, self.ck / self.m2_x)
            }
            RegrType::Intercept => {
                // Spark `RegrIntercept`: yAvg - ck / m2 * xAvg
                nullif_or_stat(
                    self.m2_x == 0.0,
                    self.mean_y - self.ck / self.m2_x * self.mean_x,
                )
            }
            RegrType::Count => Ok(ScalarValue::UInt64(Some(self.count))),
            RegrType::R2 => {
                // Spark `RegrR2` returns NULL when the second moment of y is
                // zero, and 1.0 when only the second moment of x is zero.
                if self.corr_m2_y == 0.0 {
                    Ok(ScalarValue::Float64(None))
                } else if self.corr_m2_x == 0.0 {
                    Ok(ScalarValue::Float64(Some(1.0)))
                } else {
                    let corr = self.corr_ck / (self.corr_m2_y * self.corr_m2_x).sqrt();
                    Ok(ScalarValue::Float64(Some(corr * corr)))
                }
            }
            RegrType::AvgX => nullif_or_stat(self.count < 1, self.sum_x / self.count as f64),
            RegrType::AvgY => nullif_or_stat(self.count < 1, self.sum_y / self.count as f64),
            RegrType::Sxx => nullif_or_stat(self.count < 1, self.m2_x),
            RegrType::Syy => nullif_or_stat(self.count < 1, self.m2_y),
            RegrType::Sxy => nullif_or_stat(self.count < 1, self.corr_ck),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean_x),
            ScalarValue::from(self.mean_y),
            ScalarValue::from(self.ck),
            ScalarValue::from(self.corr_ck),
            ScalarValue::from(self.m2_x),
            ScalarValue::from(self.m2_y),
            ScalarValue::from(self.corr_m2_x),
            ScalarValue::from(self.corr_m2_y),
            ScalarValue::from(self.sum_x),
            ScalarValue::from(self.sum_y),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let count_arr = as_uint64_array(&states[0])?;
        let mean_x_arr = as_float64_array(&states[1])?;
        let mean_y_arr = as_float64_array(&states[2])?;
        let ck_arr = as_float64_array(&states[3])?;
        let corr_ck_arr = as_float64_array(&states[4])?;
        let m2_x_arr = as_float64_array(&states[5])?;
        let m2_y_arr = as_float64_array(&states[6])?;
        let corr_m2_x_arr = as_float64_array(&states[7])?;
        let corr_m2_y_arr = as_float64_array(&states[8])?;
        let sum_x_arr = as_float64_array(&states[9])?;
        let sum_y_arr = as_float64_array(&states[10])?;

        for i in 0..count_arr.len() {
            let count_b = count_arr.value(i);
            if count_b == 0_u64 {
                continue;
            }

            // Assuming two different batches of input have calculated the states:
            // batch A of Y, X -> {count_a, mean_x_a, mean_y_a, ...}
            // batch B of Y, X -> {count_b, mean_x_b, mean_y_b, ...}
            // The states are merged with the merge expressions of Spark's
            // `Covariance`, `CentralMomentAgg`, `PearsonCorrelation`, and
            // `Average` buffers, which follow the parallel algorithm:
            // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
            //
            // Spark merges every partial buffer into a fresh zeroed buffer in
            // the final aggregation, which perturbs the merged means by up to
            // an ulp (`(mean / n) * n`); merging here without special-casing
            // `self.count == 0` reproduces that arithmetic exactly.
            let count_ab = self.count + count_b;
            let (n_a, n_b) = (self.count as f64, count_b as f64);
            let d_x = mean_x_arr.value(i) - self.mean_x;
            let d_x_n = d_x / count_ab as f64;
            let d_y = mean_y_arr.value(i) - self.mean_y;
            let d_y_n = d_y / count_ab as f64;

            self.ck = self.ck + ck_arr.value(i) + d_x * d_y_n * n_a * n_b;
            self.corr_ck = self.corr_ck + corr_ck_arr.value(i) + d_y * d_x_n * n_a * n_b;
            self.m2_x = self.m2_x + m2_x_arr.value(i) + d_x * d_x_n * n_a * n_b;
            self.m2_y = self.m2_y + m2_y_arr.value(i) + d_y * d_y_n * n_a * n_b;
            self.corr_m2_x = self.corr_m2_x + corr_m2_x_arr.value(i) + d_x * d_x_n * n_a * n_b;
            self.corr_m2_y = self.corr_m2_y + corr_m2_y_arr.value(i) + d_y * d_y_n * n_a * n_b;
            self.sum_x += sum_x_arr.value(i);
            self.sum_y += sum_y_arr.value(i);
            self.mean_x += d_x_n * n_b;
            self.mean_y += d_y_n * n_b;
            self.count = count_ab;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_y = as_float64_array(&values[0])?;
        let values_x = as_float64_array(&values[1])?;

        for (value_y, value_x) in values_y.iter().zip(values_x) {
            // skip either x or y is NULL
            let (value_y, value_x) = match (value_y, value_x) {
                (Some(y), Some(x)) => (y, x),
                // skip either x or y is NULL
                _ => continue,
            };

            // The algebraic inverse of the update expressions, used by sliding
            // window frames. Spark itself never retracts (it recomputes each
            // window frame), so retraction restores the statistics only up to
            // floating-point rounding.
            if self.count > 1 {
                self.count -= 1;
                let delta_x = value_x - self.mean_x;
                let delta_y = value_y - self.mean_y;
                self.mean_x -= delta_x / self.count as f64;
                self.mean_y -= delta_y / self.count as f64;
                let delta_x_2 = value_x - self.mean_x;
                let delta_y_2 = value_y - self.mean_y;
                self.ck -= delta_x_2 * delta_y;
                self.corr_ck -= delta_y_2 * delta_x;
                self.m2_x -= delta_x * delta_x_2;
                self.m2_y -= delta_y * delta_y_2;
                self.corr_m2_x -= delta_x * delta_x_2;
                self.corr_m2_y -= delta_y * delta_y_2;
                self.sum_x -= value_x;
                self.sum_y -= value_y;
            } else {
                self.count = 0;
                self.mean_x = 0.0;
                self.mean_y = 0.0;
                self.ck = 0.0;
                self.corr_ck = 0.0;
                self.m2_x = 0.0;
                self.m2_y = 0.0;
                self.corr_m2_x = 0.0;
                self.corr_m2_y = 0.0;
                self.sum_x = 0.0;
                self.sum_y = 0.0;
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}
