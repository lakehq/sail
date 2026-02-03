//! OLS (Ordinary Least Squares) solver using the Normal Equation.
//!
//! Computes: β = (X^T X)^-1 X^T y
//!
//! This gives the exact least squares solution in a single pass.
//! Complexity: O(n * p²) for matrix computation + O(p³) for solving.

use sail_common::error::{CommonError, CommonResult};

use crate::model::LinearRegressionModel;

/// Train using OLS (Normal Equation) - exact closed-form solution.
pub fn solve_ols(features: &[Vec<f64>], labels: &[f64]) -> CommonResult<LinearRegressionModel> {
    if features.is_empty() {
        return Err(CommonError::invalid("No training samples provided"));
    }

    let n = features.len();
    let p = features[0].len();

    // Step 1: Compute X^T X (p x p matrix)
    let mut xtx = vec![vec![0.0; p]; p];
    for row in features {
        for i in 0..p {
            for j in 0..p {
                xtx[i][j] += row[i] * row[j];
            }
        }
    }

    // Step 2: Compute X^T y (p vector)
    let mut xty = vec![0.0; p];
    for (row, &label) in features.iter().zip(labels.iter()) {
        for (i, &xi) in row.iter().enumerate() {
            xty[i] += xi * label;
        }
    }

    log::debug!("OLS: n={}, p={}", n, p);
    log::debug!("OLS: X^T X = {:?}", xtx);
    log::debug!("OLS: X^T y = {:?}", xty);

    // Step 3: Solve (X^T X) β = X^T y using Gaussian elimination with partial pivoting
    let coefficients = solve_linear_system(&mut xtx, &mut xty)?;

    log::info!("OLS solution: coefficients={:?}", coefficients);

    Ok(LinearRegressionModel::new(coefficients, 0.0))
}

/// Solve a linear system Ax = b using Gaussian elimination with partial pivoting.
///
/// Modifies A and b in place. Returns the solution vector x.
fn solve_linear_system(a: &mut [Vec<f64>], b: &mut [f64]) -> CommonResult<Vec<f64>> {
    let n = b.len();

    if n == 0 {
        return Ok(vec![]);
    }

    // Forward elimination with partial pivoting
    for col in 0..n {
        // Find pivot (largest absolute value in column)
        let (max_row, max_val) = a
            .iter()
            .enumerate()
            .skip(col)
            .map(|(i, row)| (i, row[col].abs()))
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or((col, 0.0));

        // Check for singular matrix
        if max_val < 1e-10 {
            return Err(CommonError::invalid(
                "Matrix is singular or nearly singular - cannot solve OLS",
            ));
        }

        // Swap rows if needed
        if max_row != col {
            a.swap(col, max_row);
            b.swap(col, max_row);
        }

        // Eliminate column entries below pivot
        let pivot = a[col][col];
        let pivot_row = a[col].clone();
        let pivot_b = b[col];

        for (row_idx, (a_row, b_val)) in a.iter_mut().zip(b.iter_mut()).enumerate() {
            if row_idx > col {
                let factor = a_row[col] / pivot;
                for (a_elem, pivot_elem) in a_row.iter_mut().zip(pivot_row.iter()) {
                    *a_elem -= factor * pivot_elem;
                }
                *b_val -= factor * pivot_b;
            }
        }
    }

    // Back substitution
    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        let mut sum = b[i];
        for (j, &xj) in x.iter().enumerate().skip(i + 1) {
            sum -= a[i][j] * xj;
        }
        x[i] = sum / a[i][i];
    }

    Ok(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_ols_simple() {
        // y = 1*x1 + 2*x2
        let features = vec![
            vec![1.0, 0.0], // y = 1
            vec![0.0, 1.0], // y = 2
            vec![1.0, 1.0], // y = 3
        ];
        let labels = vec![1.0, 2.0, 3.0];

        let model = solve_ols(&features, &labels).unwrap();

        assert!((model.coefficients()[0] - 1.0).abs() < 1e-10);
        assert!((model.coefficients()[1] - 2.0).abs() < 1e-10);
    }
}
