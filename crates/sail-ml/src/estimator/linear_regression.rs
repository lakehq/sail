//! Linear Regression estimator.

use sail_common::error::CommonResult;

use crate::model::LinearRegressionModel;
use crate::solver::{ols, sgd, Solver};

/// Linear Regression estimator.
///
/// Supports multiple solvers:
/// - OLS (Normal Equation) - exact closed-form solution
/// - SGD - iterative gradient descent
///
/// # Example
///
/// ```ignore
/// let lr = LinearRegression::new()
///     .with_solver(Solver::OLS);
///
/// let model = lr.fit(&features, &labels)?;
/// ```
#[derive(Debug, Clone)]
pub struct LinearRegression {
    solver: Solver,
    max_iter: usize,
    learning_rate: f64,
    tolerance: f64,
    fit_intercept: bool,
    reg_param: f64,
}

impl Default for LinearRegression {
    fn default() -> Self {
        Self::new()
    }
}

impl LinearRegression {
    /// Create a new LinearRegression estimator with default parameters.
    pub fn new() -> Self {
        Self {
            solver: Solver::Auto,
            max_iter: 1000,
            learning_rate: 0.1,
            tolerance: 1e-6,
            fit_intercept: false, // TODO: implement intercept
            reg_param: 0.0,       // TODO: implement regularization
        }
    }

    /// Set the solver type.
    pub fn with_solver(mut self, solver: Solver) -> Self {
        self.solver = solver;
        self
    }

    /// Set maximum iterations (for SGD).
    pub fn with_max_iter(mut self, max_iter: usize) -> Self {
        self.max_iter = max_iter;
        self
    }

    /// Set learning rate (for SGD).
    pub fn with_learning_rate(mut self, learning_rate: f64) -> Self {
        self.learning_rate = learning_rate;
        self
    }

    /// Set convergence tolerance.
    pub fn with_tolerance(mut self, tolerance: f64) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Set whether to fit an intercept term.
    pub fn with_fit_intercept(mut self, fit_intercept: bool) -> Self {
        self.fit_intercept = fit_intercept;
        self
    }

    /// Set regularization parameter (L2).
    pub fn with_reg_param(mut self, reg_param: f64) -> Self {
        self.reg_param = reg_param;
        self
    }

    /// Train the model on the given data.
    pub fn fit(
        &self,
        features: &[Vec<f64>],
        labels: &[f64],
    ) -> CommonResult<LinearRegressionModel> {
        match self.solver.effective() {
            Solver::OLS | Solver::Auto => {
                log::info!("Using OLS solver (exact solution)");
                ols::solve_ols(features, labels)
            }
            Solver::SGD => {
                log::info!("Using SGD solver (iterative)");
                let params = sgd::SGDParams {
                    max_iter: self.max_iter,
                    learning_rate: self.learning_rate,
                    tolerance: self.tolerance,
                };
                sgd::solve_sgd(features, labels, &params)
            }
        }
    }
}
