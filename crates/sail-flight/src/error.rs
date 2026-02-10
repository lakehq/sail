use std::fmt;

use datafusion::error::DataFusionError;

/// Flight SQL error types
#[derive(Debug)]
pub enum FlightError {
    /// Internal server error
    Internal(DataFusionError),
    /// Session error
    Session(String),
}

impl fmt::Display for FlightError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlightError::Internal(e) => write!(f, "Internal error: {}", e),
            FlightError::Session(msg) => write!(f, "Session error: {}", msg),
        }
    }
}

impl std::error::Error for FlightError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FlightError::Internal(e) => Some(e),
            FlightError::Session(_) => None,
        }
    }
}

impl From<DataFusionError> for FlightError {
    fn from(e: DataFusionError) -> Self {
        FlightError::Internal(e)
    }
}
