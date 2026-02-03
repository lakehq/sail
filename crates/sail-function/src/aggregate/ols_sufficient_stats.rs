// TODO(dependency): Consider adding `faer` crate for optimized linear algebra.
// faer is a pure-Rust, zero-dependency BLAS/LAPACK alternative with excellent performance.
// It would enable:
// - Fast matrix multiplication for batch processing
// - Optimized symmetric rank-1 updates for X^T X accumulation
// - SIMD-accelerated vector operations
// - Efficient linear system solving (Cholesky/LU) for the final β = (X^T X)^-1 X^T y
//
// Alternative: Use Arrow's compute kernels where applicable (add, multiply, sum).
// Arrow has built-in SIMD for Float64Array operations.

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
/// # Performance Optimizations (TODO)
///
/// The current implementation uses naive scalar loops. Several optimizations could
/// significantly improve performance:
///
/// 1. **SIMD via Arrow compute kernels**: Instead of converting Arrow arrays to Vec<f64>,
///    use arrow::compute::kernels::numeric for vectorized operations. Arrow has built-in
///    SIMD support for arithmetic operations on Float64Array.
///
/// 2. **BLAS/LAPACK library**: For matrix operations, consider using:
///    - `faer` crate: Pure Rust, no dependencies, very fast matrix operations
///    - `ndarray` with `ndarray-linalg`: Uses system BLAS (OpenBLAS/MKL)
///    - The outer product (x * x^T) could be computed as a BLAS syrk/ger operation
///
/// 3. **Batch processing**: Instead of processing row by row, accumulate multiple rows
///    in a matrix and compute X_batch^T * X_batch in one BLAS call. This converts
///    O(n*p²) scalar ops into O(n/batch) matrix multiplications.
///
/// 4. **Symmetric matrix optimization**: Since X^T X is symmetric, only compute and
///    store the upper/lower triangle (p*(p+1)/2 elements instead of p²).
///
/// 5. **Avoid Vec allocations**: Use Arrow's PrimitiveArray::values() slice directly
///    instead of .to_vec() to eliminate memory copies.
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
    /// TODO(perf): This function is the main performance bottleneck.
    /// Current complexity: O(p²) per sample with scalar operations.
    ///
    /// Potential optimizations:
    /// - Use `faer` crate for BLAS-level outer product: faer::linalg::matmul::matmul()
    /// - Process samples in batches: X_batch^T * X_batch is faster than n individual updates
    /// - Use SIMD intrinsics directly for the inner loop (requires unsafe)
    fn update_one(&mut self, features: &[f64], label: f64) {
        self.initialize(features.len());

        let p = self.num_features;

        // Update X^T X: xtx[i][j] += x[i] * x[j]
        // TODO(perf): Replace with BLAS syr (symmetric rank-1 update) or ger (outer product)
        // Example with faer: outer_product(features, features) + xtx
        for (i, &xi) in features.iter().enumerate().take(p) {
            for (j, &xj) in features.iter().enumerate().take(p) {
                self.xtx[i * p + j] += xi * xj;
            }
        }

        // Update X^T y: xty[i] += x[i] * y
        // TODO(perf): Replace with BLAS axpy: xty += label * features
        for (i, &xi) in features.iter().enumerate().take(p) {
            self.xty[i] += xi * label;
        }

        self.count += 1;
    }

    /// Merge another accumulator's state into this one.
    ///
    /// TODO(perf): Use SIMD for vector addition. Arrow's compute::add() or
    /// faer::zipped!() could vectorize this element-wise addition.
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

        // Add matrices element-wise
        for (i, &val) in other_xtx.iter().enumerate() {
            if i < self.xtx.len() {
                self.xtx[i] += val;
            }
        }

        // Add vectors element-wise
        for (i, &val) in other_xty.iter().enumerate() {
            if i < self.xty.len() {
                self.xty[i] += val;
            }
        }

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
                datafusion::error::DataFusionError::Execution(
                    "xtx state must be List".to_string(),
                )
            })?;

        let xty_lists = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "xty state must be List".to_string(),
                )
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
