use std::any::Any;
use std::fmt::Debug;

use datafusion::arrow;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_float64_array;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

/// The `PercentileFunction` calculates the exact percentile (quantile) from a set of values.
///
/// Unlike `approx_percentile_cont`, this function calculates the exact percentile by:
/// 1. Collecting all values
/// 2. Sorting them
/// 3. Computing the exact percentile using linear interpolation
///
/// This is computationally more expensive than the approximate version but provides exact results.
///
/// For the special case of percentile 0.5 (median), this is equivalent to the `median` function.
#[derive(PartialEq, Eq, Hash)]
pub struct PercentileFunction {
    signature: Signature,
}

impl Debug for PercentileFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PercentileFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for PercentileFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for PercentileFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<arrow::datatypes::FieldRef>> {
        Ok(vec![
            arrow::datatypes::Field::new("values", DataType::Float64, true).into(),
            arrow::datatypes::Field::new("percentile", DataType::Float64, false).into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Extract the percentile value from the second argument
        let percentile = if acc_args.exprs.len() >= 2 {
            // Try to evaluate the second argument as a constant
            // For now, we'll handle this in the accumulator
            0.5 // Default to median if not specified
        } else {
            0.5 // Default to median
        };

        Ok(Box::new(PercentileAccumulator::new(percentile)))
    }
}

#[derive(Debug)]
pub struct PercentileAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }

    fn calculate_percentile(&self, sorted_values: &[f64], percentile: f64) -> Option<f64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n = sorted_values.len();

        // Spark uses (n - 1) * percentile for the position
        let pos = (n - 1) as f64 * percentile;
        let lower_idx = pos.floor() as usize;
        let upper_idx = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Some(sorted_values[lower_idx])
        } else {
            let lower_val = sorted_values[lower_idx];
            let upper_val = sorted_values[upper_idx];
            let fraction = pos - lower_idx as f64;
            Some(lower_val + fraction * (upper_val - lower_val))
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        if values.len() >= 2 {
            if let Some(percentile_array) =
                values[1].as_primitive_opt::<arrow::datatypes::Float64Type>()
            {
                if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                    self.percentile = percentile_array.value(0);
                }
            }
        }

        let float_array = arrow::compute::cast(array, &DataType::Float64)?;
        let float_array = as_float64_array(&float_array)?;

        for value in float_array.iter().flatten() {
            self.values.push(value);
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values_scalar = ScalarValue::new_list_nullable(
            &self
                .values
                .iter()
                .map(|&v| ScalarValue::Float64(Some(v)))
                .collect::<Vec<_>>(),
            &DataType::Float64,
        );

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::Float64(Some(self.percentile)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = &states[0];

        if let Some(list_array) = values_list.as_list_opt::<i32>() {
            for i in 0..list_array.len() {
                if !list_array.is_null(i) {
                    let value_array = list_array.value(i);
                    let float_array = as_float64_array(&value_array)?;

                    for value in float_array.iter().flatten() {
                        self.values.push(value);
                    }
                }
            }
        }

        if states.len() >= 2 {
            let percentile_array = as_float64_array(&states[1])?;
            if !percentile_array.is_empty() && !percentile_array.is_null(0) {
                self.percentile = percentile_array.value(0);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }

        let mut sorted_values = self.values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        match self.calculate_percentile(&sorted_values, self.percentile) {
            Some(result) => Ok(ScalarValue::Float64(Some(result))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.values)
            + self.values.capacity() * std::mem::size_of::<f64>()
            + std::mem::size_of::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Float64Array;

    use super::*;

    #[test]
    fn test_percentile_median() -> Result<()> {
        let mut acc = PercentileAccumulator::new(0.5);

        // Test data from the Ibis test
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            Some(2.5),
            Some(3.5),
            Some(5.0),
            Some(6.0),
            Some(7.5),
            Some(8.5),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Expected median: (3.5 + 5.0) / 2 = 4.25
        assert_eq!(result, ScalarValue::Float64(Some(4.25)));
        Ok(())
    }

    #[test]
    fn test_percentile_25() -> Result<()> {
        let mut acc = PercentileAccumulator::new(0.25);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            Some(2.5),
            Some(3.5),
            Some(5.0),
            Some(6.0),
            Some(7.5),
            Some(8.5),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // For 8 values, 25th percentile position = (8-1) * 0.25 = 1.75
        // Interpolate between index 1 (1.0) and index 2 (2.5)
        // Result = 1.0 + 0.75 * (2.5 - 1.0) = 1.0 + 0.75 * 1.5 = 2.125
        assert_eq!(result, ScalarValue::Float64(Some(2.125)));
        Ok(())
    }

    #[test]
    fn test_percentile_empty() -> Result<()> {
        let mut acc = PercentileAccumulator::new(0.5);
        let result = acc.evaluate()?;
        assert_eq!(result, ScalarValue::Float64(None));
        Ok(())
    }

    #[test]
    fn test_percentile_single_value() -> Result<()> {
        let mut acc = PercentileAccumulator::new(0.5);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(42.0)]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Float64(Some(42.0)));
        Ok(())
    }

    #[test]
    fn test_percentile_with_nulls() -> Result<()> {
        let mut acc = PercentileAccumulator::new(0.5);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(2.0),
            None,
            Some(3.0),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        // Median of [1.0, 2.0, 3.0] is 2.0
        assert_eq!(result, ScalarValue::Float64(Some(2.0)));
        Ok(())
    }
}
