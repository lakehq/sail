// SIMD-optimized OLS sufficient statistics.
// Uses iterator patterns that LLVM auto-vectorizes effectively.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_common::scalar::ScalarStructBuilder;

/// OLS Sufficient Statistics aggregate function for distributed linear regression.
///
/// This function computes the sufficient statistics X^T X and X^T y across all samples
/// in a partition, which can then be merged for distributed OLS training.
///
/// # Arguments
/// - `features`: Array of Float64 (feature vector for each row)
/// - `label`: Float64 (target value)
///
/// # Returns
/// A struct containing:
/// - `xtx`: The accumulated X^T X matrix (flattened as p² elements)
/// - `xty`: The accumulated X^T y vector (p elements)
/// - `count`: Number of samples processed
///
/// # Algorithm
/// For each sample (x, y):
/// ```text
/// xtx += x * x^T (outer product)
/// xty += x * y
/// count += 1
/// ```
///
/// The final OLS solution is: β = (X^T X)^-1 X^T y
///
/// # Performance
///
/// This implementation uses faer for SIMD-accelerated linear algebra operations:
/// - Outer product (rank-1 update) uses vectorized multiply-add
/// - Element-wise matrix/vector addition uses SIMD
/// - Arrow slices are used directly without allocation
#[derive(PartialEq, Eq, Hash)]
pub struct OLSSufficientStats {
    name: String,
    signature: Signature,
}

impl Debug for OLSSufficientStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OLSSufficientStats")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for OLSSufficientStats {
    fn default() -> Self {
        Self::new()
    }
}

impl OLSSufficientStats {
    pub fn new() -> Self {
        Self {
            name: "ols_sufficient_stats".to_string(),
            // Arguments: features (list), label (float)
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for OLSSufficientStats {
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
        // Return a struct with xtx (list), xty (list), count (i64)
        Ok(DataType::Struct(
            vec![
                Field::new(
                    "xtx",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    true,
                ),
                Field::new(
                    "xty",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    true,
                ),
                Field::new("count", DataType::Int64, false),
            ]
            .into(),
        ))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(OLSSufficientStatsAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                "xtx",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )
            .into(),
            Field::new(
                "xty",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )
            .into(),
            Field::new("count", DataType::Int64, false).into(),
        ])
    }
}

/// Accumulator for OLS sufficient statistics computation.
///
/// Stores X^T X (flattened p×p matrix) and X^T y (p vector).
#[derive(Debug)]
pub struct OLSSufficientStatsAccumulator {
    /// X^T X matrix, flattened row-major (p² elements)
    xtx: Vec<f64>,
    /// X^T y vector (p elements)
    xty: Vec<f64>,
    /// Number of features (p)
    num_features: usize,
    /// Number of samples processed
    count: i64,
}

impl Default for OLSSufficientStatsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl OLSSufficientStatsAccumulator {
    pub fn new() -> Self {
        Self {
            xtx: Vec::new(),
            xty: Vec::new(),
            num_features: 0,
            count: 0,
        }
    }

    /// Initialize the matrices for p features.
    fn initialize(&mut self, num_features: usize) {
        if self.num_features == 0 && num_features > 0 {
            self.num_features = num_features;
            self.xtx = vec![0.0; num_features * num_features];
            self.xty = vec![0.0; num_features];
        }
    }

    /// Process a single sample and update sufficient statistics.
    ///
    /// Optimized for SIMD auto-vectorization by LLVM.
    /// Complexity: O(p²) per sample, but with vectorized inner loops.
    #[inline]
    fn update_one(&mut self, features: &[f64], label: f64) {
        self.initialize(features.len());

        let p = self.num_features;

        // Outer product: xtx += x * x^T
        // Row-by-row with vectorized inner loop (LLVM auto-vectorizes zip+map)
        for i in 0..p {
            let xi = features[i];
            let row = &mut self.xtx[i * p..(i + 1) * p];
            // This pattern is auto-vectorized by LLVM
            row.iter_mut()
                .zip(features.iter())
                .for_each(|(xtx_ij, &xj)| *xtx_ij += xi * xj);
        }

        // axpy: xty += label * x (vectorized)
        self.xty
            .iter_mut()
            .zip(features.iter())
            .for_each(|(xty_i, &xi)| *xty_i += label * xi);

        self.count += 1;
    }

    /// Merge another accumulator's state into this one.
    ///
    /// Optimized for SIMD auto-vectorization.
    #[inline]
    fn merge_one(&mut self, other_xtx: &[f64], other_xty: &[f64], other_count: i64) {
        if other_count == 0 {
            return;
        }

        // Initialize if empty
        if self.num_features == 0 && !other_xty.is_empty() {
            self.num_features = other_xty.len();
            self.xtx = vec![0.0; self.num_features * self.num_features];
            self.xty = vec![0.0; self.num_features];
        }

        // Vectorized matrix addition: self.xtx += other_xtx
        self.xtx
            .iter_mut()
            .zip(other_xtx.iter())
            .for_each(|(a, &b)| *a += b);

        // Vectorized vector addition: self.xty += other_xty
        self.xty
            .iter_mut()
            .zip(other_xty.iter())
            .for_each(|(a, &b)| *a += b);

        self.count += other_count;
    }
}

impl Accumulator for OLSSufficientStatsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return exec_err!(
                "ols_sufficient_stats requires 2 arguments (features, label), got {}",
                values.len()
            );
        }

        let features_array = &values[0];
        let labels_array = &values[1];

        // Get the labels as Float64Array
        let labels = labels_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("label must be Float64".to_string())
            })?;

        // Features is List<Float64>
        let features_list = features_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "features must be List<Float64>".to_string(),
                )
            })?;

        // Process each row
        for i in 0..labels.len() {
            if labels.is_null(i) || features_list.is_null(i) {
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
            // Use Arrow slice directly - no allocation needed
            let features: &[f64] = features_float.values();

            self.update_one(features, label);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Return struct with xtx, xty, count
        let xtx_scalars: Vec<ScalarValue> = self
            .xtx
            .iter()
            .map(|&v| ScalarValue::Float64(Some(v)))
            .collect();

        let xty_scalars: Vec<ScalarValue> = self
            .xty
            .iter()
            .map(|&v| ScalarValue::Float64(Some(v)))
            .collect();

        let xtx_list = ScalarValue::new_list_nullable(&xtx_scalars, &DataType::Float64);
        let xty_list = ScalarValue::new_list_nullable(&xty_scalars, &DataType::Float64);

        ScalarStructBuilder::new()
            .with_scalar(
                Field::new(
                    "xtx",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    true,
                ),
                ScalarValue::List(xtx_list),
            )
            .with_scalar(
                Field::new(
                    "xty",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    true,
                ),
                ScalarValue::List(xty_list),
            )
            .with_scalar(
                Field::new("count", DataType::Int64, false),
                ScalarValue::Int64(Some(self.count)),
            )
            .build()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.xtx.capacity() * std::mem::size_of::<f64>()
            + self.xty.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize state for distributed execution
        let xtx_scalars: Vec<ScalarValue> = self
            .xtx
            .iter()
            .map(|&v| ScalarValue::Float64(Some(v)))
            .collect();

        let xty_scalars: Vec<ScalarValue> = self
            .xty
            .iter()
            .map(|&v| ScalarValue::Float64(Some(v)))
            .collect();

        let xtx_list = ScalarValue::new_list_nullable(&xtx_scalars, &DataType::Float64);
        let xty_list = ScalarValue::new_list_nullable(&xty_scalars, &DataType::Float64);

        Ok(vec![
            ScalarValue::List(xtx_list),
            ScalarValue::List(xty_list),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let xtx_lists = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("xtx state must be List".to_string())
            })?;

        let xty_lists = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("xty state must be List".to_string())
            })?;

        let counts = states[2]
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "count state must be Int64".to_string(),
                )
            })?;

        for i in 0..xtx_lists.len() {
            if xtx_lists.is_null(i) || xty_lists.is_null(i) {
                continue;
            }

            // Extract xtx
            let xtx_values = xtx_lists.value(i);
            let xtx_float = xtx_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "xtx inner must be Float64".to_string(),
                    )
                })?;
            let xtx: &[f64] = xtx_float.values();

            // Extract xty
            let xty_values = xty_lists.value(i);
            let xty_float = xty_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "xty inner must be Float64".to_string(),
                    )
                })?;
            let xty: &[f64] = xty_float.values();

            let count = counts.value(i);

            self.merge_one(xtx, xty, count);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Builder, ListBuilder};

    use super::*;

    fn create_test_data() -> (ArrayRef, ArrayRef) {
        // Create features: [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]
        let mut features_builder = ListBuilder::new(Float64Builder::new());

        // Row 1: [1.0, 0.0]
        features_builder.values().append_value(1.0);
        features_builder.values().append_value(0.0);
        features_builder.append(true);

        // Row 2: [0.0, 1.0]
        features_builder.values().append_value(0.0);
        features_builder.values().append_value(1.0);
        features_builder.append(true);

        // Row 3: [1.0, 1.0]
        features_builder.values().append_value(1.0);
        features_builder.values().append_value(1.0);
        features_builder.append(true);

        let features = Arc::new(features_builder.finish()) as ArrayRef;

        // Create labels: [1.0, 2.0, 3.0] (y = x1 + 2*x2)
        let labels = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;

        (features, labels)
    }

    #[test]
    fn test_ols_stats_basic() -> Result<()> {
        let mut acc = OLSSufficientStatsAccumulator::new();
        let (features, labels) = create_test_data();

        acc.update_batch(&[features, labels])?;

        assert_eq!(acc.count, 3);
        assert_eq!(acc.num_features, 2);

        // X = [[1, 0], [0, 1], [1, 1]]
        // X^T X = [[2, 1], [1, 2]] (flattened: [2, 1, 1, 2])
        assert_eq!(acc.xtx.len(), 4);
        assert!((acc.xtx[0] - 2.0).abs() < 1e-10); // [0,0]
        assert!((acc.xtx[1] - 1.0).abs() < 1e-10); // [0,1]
        assert!((acc.xtx[2] - 1.0).abs() < 1e-10); // [1,0]
        assert!((acc.xtx[3] - 2.0).abs() < 1e-10); // [1,1]

        // y = [1, 2, 3]
        // X^T y = [1*1 + 0*2 + 1*3, 0*1 + 1*2 + 1*3] = [4, 5]
        assert_eq!(acc.xty.len(), 2);
        assert!((acc.xty[0] - 4.0).abs() < 1e-10);
        assert!((acc.xty[1] - 5.0).abs() < 1e-10);

        Ok(())
    }

    #[test]
    fn test_ols_stats_merge() -> Result<()> {
        let mut acc1 = OLSSufficientStatsAccumulator::new();
        acc1.num_features = 2;
        acc1.xtx = vec![1.0, 0.0, 0.0, 0.0]; // From first row [1,0]
        acc1.xty = vec![1.0, 0.0];
        acc1.count = 1;

        let mut acc2 = OLSSufficientStatsAccumulator::new();
        acc2.num_features = 2;
        acc2.xtx = vec![1.0, 1.0, 1.0, 2.0]; // From rows [0,1] and [1,1]
        acc2.xty = vec![3.0, 5.0];
        acc2.count = 2;

        acc1.merge_one(&acc2.xtx, &acc2.xty, acc2.count);

        assert_eq!(acc1.count, 3);
        // X^T X should be [[2, 1], [1, 2]]
        assert!((acc1.xtx[0] - 2.0).abs() < 1e-10);
        assert!((acc1.xtx[1] - 1.0).abs() < 1e-10);
        assert!((acc1.xtx[2] - 1.0).abs() < 1e-10);
        assert!((acc1.xtx[3] - 2.0).abs() < 1e-10);

        // X^T y should be [4, 5]
        assert!((acc1.xty[0] - 4.0).abs() < 1e-10);
        assert!((acc1.xty[1] - 5.0).abs() < 1e-10);

        Ok(())
    }
}
