use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::ArrayRef;
/// [Credit]: <https://github.com/apache/datafusion/blob/db2d21e094b1be2ff27653ce58ba1317e2237b01/datafusion/functions-aggregate/src/regr.rs>
use arrow::datatypes::FieldRef;
use arrow::datatypes::{DataType, Field};
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
        Ok(vec![
            Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(args.name, "mean_x"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(args.name, "mean_y"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(args.name, "m2_x"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(args.name, "m2_y"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(args.name, "algo_const"),
                DataType::Float64,
                true,
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect())
    }
}

/// `RegrAccumulator` is used to compute linear regression aggregate functions
/// by maintaining statistics needed to compute them in an online fashion.
///
/// This struct uses Welford's online algorithm for calculating variance and covariance:
/// <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm>
///
/// Given the statistics, the following aggregate functions can be calculated:
///
/// - `regr_slope(y, x)`: Slope of the linear regression line, calculated as:
///   cov_pop(x, y) / var_pop(x).
///   It represents the expected change in Y for a one-unit change in X.
///
/// - `regr_intercept(y, x)`: Intercept of the linear regression line, calculated as:
///   mean_y - (regr_slope(y, x) * mean_x).
///   It represents the expected value of Y when X is 0.
///
/// - `regr_count(y, x)`: Count of the non-null(both x and y) input rows.
///
/// - `regr_r2(y, x)`: R-squared value (coefficient of determination), calculated as:
///   (cov_pop(x, y) ^ 2) / (var_pop(x) * var_pop(y)).
///   It provides a measure of how well the model's predictions match the observed data.
///
/// - `regr_avgx(y, x)`: Average of the independent variable X, calculated as: mean_x.
///
/// - `regr_avgy(y, x)`: Average of the dependent variable Y, calculated as: mean_y.
///
/// - `regr_sxx(y, x)`: Sum of squares of the independent variable X, calculated as:
///   m2_x.
///
/// - `regr_syy(y, x)`: Sum of squares of the dependent variable Y, calculated as:
///   m2_y.
///
/// - `regr_sxy(y, x)`: Sum of products of paired values, calculated as:
///   algo_const.
///
/// Here's how the statistics maintained in this struct are calculated:
/// - `cov_pop(x, y)`: algo_const / count.
/// - `var_pop(x)`: m2_x / count.
/// - `var_pop(y)`: m2_y / count.
#[derive(Debug)]
pub struct RegrAccumulator {
    count: u64,
    mean_x: f64,
    mean_y: f64,
    m2_x: f64,
    m2_y: f64,
    algo_const: f64,
    regr_type: RegrType,
}

impl RegrAccumulator {
    /// Creates a new `RegrAccumulator`
    pub fn try_new(regr_type: &RegrType) -> Result<Self> {
        Ok(Self {
            count: 0_u64,
            mean_x: 0_f64,
            mean_y: 0_f64,
            m2_x: 0_f64,
            m2_y: 0_f64,
            algo_const: 0_f64,
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

            // Update states for regr_slope(y,x) [using cov_pop(x,y)/var_pop(x)]
            self.count += 1;
            let delta_x = value_x - self.mean_x;
            let delta_y = value_y - self.mean_y;
            self.mean_x += delta_x / self.count as f64;
            self.mean_y += delta_y / self.count as f64;
            let delta_x_2 = value_x - self.mean_x;
            let delta_y_2 = value_y - self.mean_y;
            self.m2_x += delta_x * delta_x_2;
            self.m2_y += delta_y * delta_y_2;
            self.algo_const += delta_x * (value_y - self.mean_y);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let cov_pop_x_y = self.algo_const / self.count as f64;
        let var_pop_x = self.m2_x / self.count as f64;
        let var_pop_y = self.m2_y / self.count as f64;

        let nullif_or_stat = |cond: bool, stat: f64| {
            if cond {
                Ok(ScalarValue::Float64(None))
            } else {
                Ok(ScalarValue::Float64(Some(stat)))
            }
        };

        match self.regr_type {
            RegrType::Slope => {
                // Only 0/1 point or slope is infinite
                let nullif_cond = self.count <= 1 || var_pop_x == 0.0;
                nullif_or_stat(nullif_cond, cov_pop_x_y / var_pop_x)
            }
            RegrType::Intercept => {
                let slope = cov_pop_x_y / var_pop_x;
                // Only 0/1 point or slope is infinite
                let nullif_cond = self.count <= 1 || var_pop_x == 0.0;
                nullif_or_stat(nullif_cond, self.mean_y - slope * self.mean_x)
            }
            RegrType::Count => Ok(ScalarValue::UInt64(Some(self.count))),
            RegrType::R2 => {
                // Only 0/1 point or all x(or y) is the same
                let nullif_cond = self.count <= 1 || var_pop_x == 0.0 || var_pop_y == 0.0;
                nullif_or_stat(
                    nullif_cond,
                    (cov_pop_x_y * cov_pop_x_y) / (var_pop_x * var_pop_y),
                )
            }
            RegrType::AvgX => nullif_or_stat(self.count < 1, self.mean_x),
            RegrType::AvgY => nullif_or_stat(self.count < 1, self.mean_y),
            RegrType::Sxx => nullif_or_stat(self.count < 1, self.m2_x),
            RegrType::Syy => nullif_or_stat(self.count < 1, self.m2_y),
            RegrType::Sxy => nullif_or_stat(self.count < 1, self.algo_const),
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
            ScalarValue::from(self.m2_x),
            ScalarValue::from(self.m2_y),
            ScalarValue::from(self.algo_const),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let count_arr = as_uint64_array(&states[0])?;
        let mean_x_arr = as_float64_array(&states[1])?;
        let mean_y_arr = as_float64_array(&states[2])?;
        let m2_x_arr = as_float64_array(&states[3])?;
        let m2_y_arr = as_float64_array(&states[4])?;
        let algo_const_arr = as_float64_array(&states[5])?;

        for i in 0..count_arr.len() {
            let count_b = count_arr.value(i);
            if count_b == 0_u64 {
                continue;
            }
            let (count_a, mean_x_a, mean_y_a, m2_x_a, m2_y_a, algo_const_a) = (
                self.count,
                self.mean_x,
                self.mean_y,
                self.m2_x,
                self.m2_y,
                self.algo_const,
            );
            let (count_b, mean_x_b, mean_y_b, m2_x_b, m2_y_b, algo_const_b) = (
                count_b,
                mean_x_arr.value(i),
                mean_y_arr.value(i),
                m2_x_arr.value(i),
                m2_y_arr.value(i),
                algo_const_arr.value(i),
            );

            // Assuming two different batches of input have calculated the states:
            // batch A of Y, X -> {count_a, mean_x_a, mean_y_a, m2_x_a, algo_const_a}
            // batch B of Y, X -> {count_b, mean_x_b, mean_y_b, m2_x_b, algo_const_b}
            // The merged states from A and B are {count_ab, mean_x_ab, mean_y_ab, m2_x_ab,
            // algo_const_ab}
            //
            // Reference for the algorithm to merge states:
            // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
            let count_ab = count_a + count_b;
            let (count_a, count_b) = (count_a as f64, count_b as f64);
            let d_x = mean_x_b - mean_x_a;
            let d_y = mean_y_b - mean_y_a;
            let mean_x_ab = mean_x_a + d_x * count_b / count_ab as f64;
            let mean_y_ab = mean_y_a + d_y * count_b / count_ab as f64;
            let m2_x_ab = m2_x_a + m2_x_b + d_x * d_x * count_a * count_b / count_ab as f64;
            let m2_y_ab = m2_y_a + m2_y_b + d_y * d_y * count_a * count_b / count_ab as f64;
            let algo_const_ab =
                algo_const_a + algo_const_b + d_x * d_y * count_a * count_b / count_ab as f64;

            self.count = count_ab;
            self.mean_x = mean_x_ab;
            self.mean_y = mean_y_ab;
            self.m2_x = m2_x_ab;
            self.m2_y = m2_y_ab;
            self.algo_const = algo_const_ab;
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

            // Update states for regr_slope(y,x) [using cov_pop(x,y)/var_pop(x)]
            if self.count > 1 {
                self.count -= 1;
                let delta_x = value_x - self.mean_x;
                let delta_y = value_y - self.mean_y;
                self.mean_x -= delta_x / self.count as f64;
                self.mean_y -= delta_y / self.count as f64;
                let delta_x_2 = value_x - self.mean_x;
                let delta_y_2 = value_y - self.mean_y;
                self.m2_x -= delta_x * delta_x_2;
                self.m2_y -= delta_y * delta_y_2;
                self.algo_const -= delta_x * (value_y - self.mean_y);
            } else {
                self.count = 0;
                self.mean_x = 0.0;
                self.m2_x = 0.0;
                self.m2_y = 0.0;
                self.mean_y = 0.0;
                self.algo_const = 0.0;
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}
