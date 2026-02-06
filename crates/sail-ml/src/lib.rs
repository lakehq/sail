//! Sail ML - Machine Learning library for Sail.
//!
//! This crate provides ML estimators and transformers compatible with Spark ML.
//!
//! # Estimators
//!
//! - [`LinearRegression`](estimator::LinearRegression) - Linear regression with OLS and SGD solvers
//!
//! # Solvers
//!
//! - [`OLS`](solver::ols) - Ordinary Least Squares (exact closed-form solution)
//! - [`SGD`](solver::sgd) - Stochastic Gradient Descent (iterative)
//!
//! # Example
//!
//! ```ignore
//! use sail_ml::estimator::LinearRegression;
//! use sail_ml::solver::Solver;
//!
//! let lr = LinearRegression::new()
//!     .with_solver(Solver::OLS)
//!     .with_fit_intercept(false);
//!
//! let model = lr.fit(&features, &labels)?;
//! println!("Coefficients: {:?}", model.coefficients());
//! ```

pub mod estimator;
pub mod model;
pub mod solver;

pub use estimator::LinearRegression;
pub use model::LinearRegressionModel;
pub use solver::Solver;
