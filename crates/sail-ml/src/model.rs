//! Trained ML models.

/// A trained linear regression model.
#[derive(Debug, Clone)]
pub struct LinearRegressionModel {
    coefficients: Vec<f64>,
    intercept: f64,
    num_features: usize,
}

impl LinearRegressionModel {
    /// Create a new trained model.
    pub fn new(coefficients: Vec<f64>, intercept: f64) -> Self {
        let num_features = coefficients.len();
        Self {
            coefficients,
            intercept,
            num_features,
        }
    }

    /// Get the model coefficients.
    pub fn coefficients(&self) -> &[f64] {
        &self.coefficients
    }

    /// Get the model intercept.
    pub fn intercept(&self) -> f64 {
        self.intercept
    }

    /// Get the number of features.
    pub fn num_features(&self) -> usize {
        self.num_features
    }

    /// Predict for a single sample.
    pub fn predict(&self, features: &[f64]) -> f64 {
        let mut prediction = self.intercept;
        for (coef, feat) in self.coefficients.iter().zip(features.iter()) {
            prediction += coef * feat;
        }
        prediction
    }

    /// Predict for multiple samples.
    pub fn predict_batch(&self, features: &[Vec<f64>]) -> Vec<f64> {
        features.iter().map(|f| self.predict(f)).collect()
    }
}
