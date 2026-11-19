// SPDX-License-Identifier: Apache-2.0

use datafusion::error::DataFusionError;

/// Flight SQL error types.
#[derive(Debug, thiserror::Error)]
pub enum FlightError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("Session error: {0}")]
    Session(String),
}
