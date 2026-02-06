//! SGD (Stochastic Gradient Descent) solver.
//!
//! Uses batch gradient descent to minimize MSE:
//! For y = Xβ, the gradient is: ∂MSE/∂β = (2/n) * X^T * (Xβ - y)

use sail_common::error::CommonResult;

use crate::model::LinearRegressionModel;

/// SGD training parameters.
#[derive(Debug, Clone)]
pub struct SGDParams {
    pub max_iter: usize,
    pub learning_rate: f64,
    pub tolerance: f64,
}

impl Default for SGDParams {
    fn default() -> Self {
        Self {
            max_iter: 1000,
            learning_rate: 0.1,
            tolerance: 1e-6,
        }
    }
}

/// Train using SGD (Stochastic Gradient Descent).
pub fn solve_sgd(
    features: &[Vec<f64>],
    labels: &[f64],
    params: &SGDParams,
) -> CommonResult<LinearRegressionModel> {
    let num_features = if features.is_empty() {
        0
    } else {
        features[0].len()
    };

    let mut coefficients = vec![0.0; num_features];
    let n = labels.len() as f64;
    let mut prev_loss = f64::MAX;

    for epoch in 0..params.max_iter {
        // Compute predictions and gradients
        let mut gradient = vec![0.0; coefficients.len()];
        let mut loss_sum = 0.0;

        for (x, &y) in features.iter().zip(labels.iter()) {
            // Compute prediction: y_pred = x · β
            let y_pred: f64 = x
                .iter()
                .zip(coefficients.iter())
                .map(|(xi, bi)| xi * bi)
                .sum();

            // Compute error
            let error = y_pred - y;
            loss_sum += error * error;

            // Accumulate gradient: ∂MSE/∂β_j = 2 * error * x_j
            for (j, xj) in x.iter().enumerate() {
                gradient[j] += error * xj;
            }
        }

        // Update coefficients: β = β - learning_rate * (2/n) * gradient
        let scale = 2.0 / n;
        for (coef, grad) in coefficients.iter_mut().zip(gradient.iter()) {
            *coef -= params.learning_rate * scale * grad;
        }

        // Compute average loss (MSE)
        let avg_loss = loss_sum / n;

        log::debug!(
            "Epoch {}: loss={:.6}, coefficients={:?}",
            epoch,
            avg_loss,
            coefficients
        );

        // Check convergence
        if (prev_loss - avg_loss).abs() < params.tolerance {
            log::info!("Converged at epoch {} with loss {:.6}", epoch, avg_loss);
            break;
        }
        prev_loss = avg_loss;
    }

    Ok(LinearRegressionModel::new(coefficients, 0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_sgd_simple() {
        // y = 1*x1 + 2*x2
        let features = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]];
        let labels = vec![1.0, 2.0, 3.0];

        let params = SGDParams {
            max_iter: 10000,
            learning_rate: 0.1,
            tolerance: 1e-10,
        };

        let model = solve_sgd(&features, &labels, &params).unwrap();

        // SGD is approximate, allow some tolerance
        assert!((model.coefficients()[0] - 1.0).abs() < 0.01);
        assert!((model.coefficients()[1] - 2.0).abs() < 0.01);
    }
}
