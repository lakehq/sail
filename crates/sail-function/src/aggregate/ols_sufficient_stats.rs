// Hybrid OLS sufficient statistics with automatic algorithm selection.
//
// Strategy selection based on benchmarks:
// - SIMD row-by-row: Best for p < 200 (most common cases)
// - Batch GEMM: Best for p >= 200 (high-dimensional data, cache blocking helps)
//
// Benchmark results (1M rows):
//   p=100: SIMD 20.5s vs Batch GEMM 25.0s → SIMD wins
//   p=500: SIMD 426.6s vs Batch GEMM 334.9s → Batch GEMM wins

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use faer::Mat;
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
/// This implementation uses a hybrid approach with automatic algorithm selection:
/// - **SIMD row-by-row**: Best for p < 200, uses LLVM auto-vectorization
/// - **Batch GEMM**: Best for p >= 200, uses faer's cache-blocked matrix multiplication
///
/// The strategy can be overridden with `SAIL_OLS_STRATEGY` environment variable:
/// - `auto` (default): Choose based on p (threshold = 200)
/// - `simd`: Force SIMD row-by-row processing
/// - `gemm`: Force batch GEMM processing
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

    /// Process a single sample and update sufficient statistics (SIMD path).
    ///
    /// Optimized for SIMD auto-vectorization by LLVM.
    /// Best for p < 200 where matrix copy overhead dominates GEMM benefits.
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

    /// SIMD-optimized batch processing using row-by-row updates.
    ///
    /// Best for p < 200 where LLVM auto-vectorization is efficient
    /// and matrix copy overhead would dominate GEMM benefits.
    fn update_batch_simd(&mut self, labels: &Float64Array, features_list: &ListArray) -> Result<()> {
        for i in 0..labels.len() {
            if labels.is_null(i) || features_list.is_null(i) {
                continue;
            }

            let label = labels.value(i);
            let features_values = features_list.value(i);
            let features_float = features_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "features inner must be Float64".to_string(),
                    )
                })?;
            let features: &[f64] = features_float.values();

            self.update_one(features, label);
        }
        Ok(())
    }

    /// Batch GEMM processing using faer's cache-blocked matrix multiplication.
    ///
    /// Best for p >= 200 where cache blocking in GEMM provides significant speedup
    /// over row-by-row rank-1 updates that cause cache misses.
    fn update_batch_gemm(
        &mut self,
        labels: &Float64Array,
        features_list: &ListArray,
        n_valid: usize,
        p: usize,
    ) -> Result<()> {
        // Build batch matrices using faer
        // X_batch: n_valid x p (each row is one sample)
        // y_batch: n_valid x 1
        let mut x_batch = Mat::<f64>::zeros(n_valid, p);
        let mut y_batch = Mat::<f64>::zeros(n_valid, 1);

        let mut row_idx = 0usize;
        for i in 0..labels.len() {
            if labels.is_null(i) || features_list.is_null(i) {
                continue;
            }

            let label = labels.value(i);
            let features_values = features_list.value(i);
            let features_float = features_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "features inner must be Float64".to_string(),
                    )
                })?;
            let features: &[f64] = features_float.values();

            // Fill row in X_batch
            for (j, &f) in features.iter().enumerate() {
                x_batch[(row_idx, j)] = f;
            }
            y_batch[(row_idx, 0)] = label;
            row_idx += 1;
        }

        // Compute X^T * X using GEMM (cache-blocked matrix multiplication)
        let xtx_batch = x_batch.transpose() * &x_batch;

        // Compute X^T * y using GEMM
        let xty_batch = x_batch.transpose() * &y_batch;

        // Add batch results to accumulator (vectorized)
        for i in 0..p {
            for j in 0..p {
                self.xtx[i * p + j] += xtx_batch[(i, j)];
            }
            self.xty[i] += xty_batch[(i, 0)];
        }
        self.count += n_valid as i64;

        Ok(())
    }
}

/// OLS computation strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OlsStrategy {
    /// Automatic selection based on p (default)
    Auto,
    /// Force SIMD row-by-row processing
    Simd,
    /// Force batch GEMM processing
    Gemm,
}

impl OlsStrategy {
    /// Read strategy from SAIL_OLS_STRATEGY environment variable.
    fn from_env() -> Self {
        match std::env::var("SAIL_OLS_STRATEGY")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "simd" => OlsStrategy::Simd,
            "gemm" => OlsStrategy::Gemm,
            _ => OlsStrategy::Auto, // "auto" or unset
        }
    }

    /// Determine whether to use GEMM based on strategy and feature count.
    fn use_gemm(self, p: usize) -> bool {
        match self {
            OlsStrategy::Auto => p >= 200, // Threshold based on benchmarks
            OlsStrategy::Simd => false,
            OlsStrategy::Gemm => true,
        }
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

        // First pass: count valid rows and get feature dimension
        let mut n_valid = 0usize;
        let mut p = 0usize;
        for i in 0..labels.len() {
            if labels.is_null(i) || features_list.is_null(i) {
                continue;
            }
            if p == 0 {
                let features_values = features_list.value(i);
                p = features_values.len();
            }
            n_valid += 1;
        }

        if n_valid == 0 || p == 0 {
            return Ok(());
        }

        // Initialize accumulator if needed
        self.initialize(p);

        // Choose strategy based on environment variable and feature count
        let strategy = OlsStrategy::from_env();

        if strategy.use_gemm(p) {
            // Batch GEMM path: best for high-dimensional data (p >= 200)
            self.update_batch_gemm(labels, features_list, n_valid, p)?;
        } else {
            // SIMD row-by-row path: best for most cases (p < 200)
            self.update_batch_simd(labels, features_list)?;
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
