use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_common::scalar::ScalarStructBuilder;

/// SGD Gradient Sum aggregate function for distributed linear regression training.
///
/// This function computes the sum of gradients across all samples in a partition,
/// which can then be merged with gradients from other partitions for distributed SGD.
///
/// # Arguments
/// - `features`: Array of Float64 (feature vector for each row)
/// - `label`: Float64 (target value)
/// - `coefficients`: Array of Float64 (current model coefficients β)
///
/// # Returns
/// A struct containing:
/// - `gradient_sum`: The accumulated gradient vector
/// - `count`: Number of samples processed
/// - `loss_sum`: Sum of squared errors for monitoring
///
/// # Algorithm
/// For each sample (x, y) with current coefficients β:
/// ```text
/// prediction = x · β (dot product)
/// error = prediction - y
/// gradient = x * error
/// gradient_sum += gradient
/// loss_sum += error²
/// count += 1
/// ```
#[derive(PartialEq, Eq, Hash)]
pub struct SGDGradientSum {
    name: String,
    signature: Signature,
}

impl Debug for SGDGradientSum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SGDGradientSum")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for SGDGradientSum {
    fn default() -> Self {
        Self::new()
    }
}

impl SGDGradientSum {
    pub fn new() -> Self {
        Self {
            name: "sgd_gradient_sum".to_string(),
            // Arguments: features (list), label (float), coefficients (list)
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SGDGradientSum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Return a struct with gradient_sum (list), count (i64), loss_sum (f64)
        Ok(DataType::Struct(
            vec![
                Field::new(
                    "gradient_sum",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    true,
                ),
                Field::new("count", DataType::Int64, false),
                Field::new("loss_sum", DataType::Float64, false),
            ]
            .into(),
        ))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SGDGradientSumAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                "gradient_sum",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )
            .into(),
            Field::new("count", DataType::Int64, false).into(),
            Field::new("loss_sum", DataType::Float64, false).into(),
        ])
    }
}

/// Accumulator for SGD gradient computation.
#[derive(Debug)]
pub struct SGDGradientSumAccumulator {
    gradient_sum: Vec<f64>,
    count: i64,
    loss_sum: f64,
}

impl Default for SGDGradientSumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SGDGradientSumAccumulator {
    pub fn new() -> Self {
        Self {
            gradient_sum: Vec::new(),
            count: 0,
            loss_sum: 0.0,
        }
    }

    /// Process a single sample and update the gradient sum.
    fn update_one(&mut self, features: &[f64], label: f64, coefficients: &[f64]) {
        // Initialize gradient_sum if empty
        if self.gradient_sum.is_empty() {
            self.gradient_sum = vec![0.0; features.len()];
        }

        // Compute prediction: x · β
        let prediction: f64 = features
            .iter()
            .zip(coefficients.iter())
            .map(|(x, b)| x * b)
            .sum();

        // Compute error
        let error = prediction - label;

        // Update gradient sum: gradient_sum += x * error
        for (i, &x) in features.iter().enumerate() {
            self.gradient_sum[i] += x * error;
        }

        // Update loss sum for monitoring
        self.loss_sum += error * error;
        self.count += 1;
    }

    /// Merge another accumulator's state into this one.
    fn merge_one(&mut self, other_gradient: &[f64], other_count: i64, other_loss: f64) {
        if other_count == 0 {
            return;
        }

        // Initialize if empty
        if self.gradient_sum.is_empty() {
            self.gradient_sum = vec![0.0; other_gradient.len()];
        }

        // Add gradients element-wise
        for (i, &g) in other_gradient.iter().enumerate() {
            if i < self.gradient_sum.len() {
                self.gradient_sum[i] += g;
            }
        }

        self.count += other_count;
        self.loss_sum += other_loss;
    }
}

impl Accumulator for SGDGradientSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 3 {
            return exec_err!(
                "sgd_gradient_sum requires 3 arguments (features, label, coefficients), got {}",
                values.len()
            );
        }

        let features_array = &values[0];
        let labels_array = &values[1];
        let coefficients_array = &values[2];

        // Get the labels as Float64Array
        let labels = labels_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("label must be Float64".to_string())
            })?;

        // Features and coefficients are List<Float64>
        let features_list = features_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "features must be List<Float64>".to_string(),
                )
            })?;

        let coefficients_list = coefficients_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "coefficients must be List<Float64>".to_string(),
                )
            })?;

        // Process each row
        for i in 0..labels.len() {
            if labels.is_null(i) || features_list.is_null(i) || coefficients_list.is_null(i) {
                continue;
            }

            let label = labels.value(i);

            // Extract features for this row
            let features_values = features_list.value(i);
            let features_float = features_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "features inner must be Float64".to_string(),
                    )
                })?;
            let features: Vec<f64> = features_float.values().to_vec();

            // Extract coefficients for this row (should be same for all rows)
            let coef_values = coefficients_list.value(i);
            let coef_float = coef_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "coefficients inner must be Float64".to_string(),
                    )
                })?;
            let coefficients: Vec<f64> = coef_float.values().to_vec();

            self.update_one(&features, label, &coefficients);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Return struct with gradient_sum, count, loss_sum
        let gradient_scalars: Vec<ScalarValue> = self
            .gradient_sum
            .iter()
            .map(|&g| ScalarValue::Float64(Some(g)))
            .collect();

        let gradient_list = ScalarValue::new_list_nullable(&gradient_scalars, &DataType::Float64);

        ScalarStructBuilder::new()
            .with_scalar(
                Field::new(
                    "gradient_sum",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    true,
                ),
                ScalarValue::List(gradient_list),
            )
            .with_scalar(
                Field::new("count", DataType::Int64, false),
                ScalarValue::Int64(Some(self.count)),
            )
            .with_scalar(
                Field::new("loss_sum", DataType::Float64, false),
                ScalarValue::Float64(Some(self.loss_sum)),
            )
            .build()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.gradient_sum.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize state for distributed execution
        let gradient_scalars: Vec<ScalarValue> = self
            .gradient_sum
            .iter()
            .map(|&g| ScalarValue::Float64(Some(g)))
            .collect();

        let gradient_list = ScalarValue::new_list_nullable(&gradient_scalars, &DataType::Float64);

        Ok(vec![
            ScalarValue::List(gradient_list),
            ScalarValue::Int64(Some(self.count)),
            ScalarValue::Float64(Some(self.loss_sum)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let gradient_lists = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "gradient_sum state must be List".to_string(),
                )
            })?;

        let counts = states[1]
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "count state must be Int64".to_string(),
                )
            })?;

        let losses = states[2]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "loss_sum state must be Float64".to_string(),
                )
            })?;

        for i in 0..gradient_lists.len() {
            if gradient_lists.is_null(i) {
                continue;
            }

            let gradient_values = gradient_lists.value(i);
            let gradient_float = gradient_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "gradient inner must be Float64".to_string(),
                    )
                })?;
            let gradient: Vec<f64> = gradient_float.values().to_vec();

            let count = counts.value(i);
            let loss = losses.value(i);

            self.merge_one(&gradient, count, loss);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Builder, ListBuilder};

    use super::*;

    fn create_test_data() -> (ArrayRef, ArrayRef, ArrayRef) {
        // Create features: [[1.0], [2.0], [3.0]]
        let mut features_builder = ListBuilder::new(Float64Builder::new());
        features_builder.values().append_value(1.0);
        features_builder.append(true);
        features_builder.values().append_value(2.0);
        features_builder.append(true);
        features_builder.values().append_value(3.0);
        features_builder.append(true);
        let features = Arc::new(features_builder.finish()) as ArrayRef;

        // Create labels: [2.0, 4.0, 6.0] (y = 2x)
        let labels = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef;

        // Create coefficients: [[0.0], [0.0], [0.0]] (initial)
        let mut coef_builder = ListBuilder::new(Float64Builder::new());
        coef_builder.values().append_value(0.0);
        coef_builder.append(true);
        coef_builder.values().append_value(0.0);
        coef_builder.append(true);
        coef_builder.values().append_value(0.0);
        coef_builder.append(true);
        let coefficients = Arc::new(coef_builder.finish()) as ArrayRef;

        (features, labels, coefficients)
    }

    #[test]
    fn test_sgd_gradient_basic() -> Result<()> {
        let mut acc = SGDGradientSumAccumulator::new();
        let (features, labels, coefficients) = create_test_data();

        acc.update_batch(&[features, labels, coefficients])?;

        assert_eq!(acc.count, 3);
        // With β = 0, predictions are all 0
        // errors = [0-2, 0-4, 0-6] = [-2, -4, -6]
        // gradients = [1*(-2), 2*(-4), 3*(-6)] = [-2, -8, -18]
        // gradient_sum = -2 + -8 + -18 = -28
        assert_eq!(acc.gradient_sum.len(), 1);
        assert!((acc.gradient_sum[0] - (-28.0)).abs() < 1e-10);

        // loss_sum = 4 + 16 + 36 = 56
        assert!((acc.loss_sum - 56.0).abs() < 1e-10);

        Ok(())
    }

    #[test]
    fn test_sgd_gradient_merge() -> Result<()> {
        let mut acc1 = SGDGradientSumAccumulator::new();
        acc1.gradient_sum = vec![-10.0];
        acc1.count = 2;
        acc1.loss_sum = 20.0;

        let mut acc2 = SGDGradientSumAccumulator::new();
        acc2.gradient_sum = vec![-18.0];
        acc2.count = 1;
        acc2.loss_sum = 36.0;

        acc1.merge_one(&acc2.gradient_sum, acc2.count, acc2.loss_sum);

        assert_eq!(acc1.count, 3);
        assert!((acc1.gradient_sum[0] - (-28.0)).abs() < 1e-10);
        assert!((acc1.loss_sum - 56.0).abs() < 1e-10);

        Ok(())
    }
}
