use opentelemetry_otlp::ExporterBuildError;
use thiserror::Error;

pub type TelemetryResult<T> = Result<T, TelemetryError>;

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("exporter build error: {0}")]
    ExporterBuildError(#[from] ExporterBuildError),
}

impl TelemetryError {
    pub fn invalid(message: impl Into<String>) -> Self {
        TelemetryError::InvalidArgument(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        TelemetryError::InternalError(message.into())
    }
}
