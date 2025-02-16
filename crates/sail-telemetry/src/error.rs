use std::env;

use opentelemetry::trace::TraceError;
use sail_common::error::CommonError;
use thiserror::Error;
use tonic::codegen::http::header;
use tonic::metadata::errors::{
    InvalidMetadataKey, InvalidMetadataValue, InvalidMetadataValueBytes, ToStrError,
};

pub type TelemetryResult<T> = Result<T, TelemetryError>;

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("env error: {0}")]
    EnvError(#[from] env::VarError),
    #[error("http header error: {0}")]
    HttpHeaderToStrError(#[from] header::ToStrError),
    #[error("http header error: {0}")]
    HttpInvalidHeaderValue(#[from] header::InvalidHeaderValue),
    #[error("tonic metadata error: {0}")]
    TonicInvalidMetadataValue(#[from] InvalidMetadataValue),
    #[error("tonic metadata error: {0}")]
    TonicInvalidMetadataValueBytes(#[from] InvalidMetadataValueBytes),
    #[error("tonic metadata error: {0}")]
    TonicInvalidMetadataKey(#[from] InvalidMetadataKey),
    #[error("tonic metadata error: {0}")]
    TonicMetadataToStrError(#[from] ToStrError),
    #[error("trace error: {0}")]
    TraceError(#[from] TraceError),
}

impl TelemetryError {
    pub fn missing(message: impl Into<String>) -> Self {
        TelemetryError::MissingArgument(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        TelemetryError::InvalidArgument(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        TelemetryError::NotSupported(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        TelemetryError::InternalError(message.into())
    }
}

impl From<CommonError> for TelemetryError {
    fn from(error: CommonError) -> Self {
        match error {
            CommonError::MissingArgument(message) => TelemetryError::MissingArgument(message),
            CommonError::InvalidArgument(message) => TelemetryError::InvalidArgument(message),
            CommonError::NotSupported(message) => TelemetryError::NotSupported(message),
            CommonError::InternalError(message) => TelemetryError::InternalError(message),
        }
    }
}
