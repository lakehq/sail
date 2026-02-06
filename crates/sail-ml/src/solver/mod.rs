//! Solvers for ML training algorithms.

pub mod ols;
pub mod sgd;

pub use ols::solve_ols;
pub use sgd::solve_sgd;

/// Available solvers for linear regression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(clippy::upper_case_acronyms)]
pub enum Solver {
    /// Ordinary Least Squares - exact closed-form solution.
    /// Uses Normal Equation: β = (X^T X)^-1 X^T y
    /// Best for: small to medium datasets, exact results needed.
    /// Complexity: O(n·p² + p³)
    #[default]
    OLS,

    /// Stochastic Gradient Descent - iterative approximation.
    /// Best for: large sparse datasets, approximate results acceptable.
    /// Complexity: O(n·p·iterations)
    SGD,

    /// Automatically choose the best solver based on data characteristics.
    /// Currently defaults to OLS.
    Auto,
}

impl Solver {
    /// Parse solver name from string (Spark ML compatible).
    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "normal" | "ols" => Solver::OLS,
            "sgd" | "gd" => Solver::SGD,
            _ => Solver::Auto,
        }
    }

    /// Get the effective solver (resolves Auto).
    pub fn effective(&self) -> Solver {
        match self {
            Solver::Auto => Solver::OLS, // Default to exact solution
            other => *other,
        }
    }
}
