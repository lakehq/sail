use std::convert::Infallible;

use datafusion::common::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SystemStoreError {
    #[error("invalid system store key")]
    InvalidKey,
    #[error("invalid system store value: {0}")]
    InvalidValue(String),
    #[error("internal system store error: {0}")]
    Internal(String),
    #[error("datafusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),
}

impl From<Infallible> for SystemStoreError {
    fn from(error: Infallible) -> Self {
        match error {}
    }
}

impl From<SystemStoreError> for DataFusionError {
    fn from(error: SystemStoreError) -> Self {
        Self::External(Box::new(error))
    }
}

impl SystemStoreError {
    pub fn invalid_value(message: impl Into<String>) -> Self {
        Self::InvalidValue(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

/// Result returned by system store operations.
pub type Result<T> = std::result::Result<T, SystemStoreError>;
