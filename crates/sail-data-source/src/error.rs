use datafusion_common::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataSourceError {
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("missing option: {key}")]
    MissingOption { key: &'static str },
    #[error("invalid option: {key}: {value}{}", .cause.as_deref().map(|c| format!(" ({})", c)).unwrap_or_default())]
    InvalidOption {
        key: String,
        value: String,
        cause: Option<String>,
    },
}

pub type DataSourceResult<T> = Result<T, DataSourceError>;

impl From<DataSourceError> for DataFusionError {
    fn from(e: DataSourceError) -> Self {
        match e {
            DataSourceError::DataFusion(e) => e,
            e => DataFusionError::External(Box::new(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_datafusion_errors() {
        let error = DataSourceError::from(DataFusionError::Plan("provider failed".to_string()));
        assert!(matches!(
            DataFusionError::from(error),
            DataFusionError::Plan(message) if message == "provider failed"
        ));
    }

    #[test]
    fn option_errors_remain_external() {
        for error in [
            DataSourceError::MissingOption { key: "path" },
            DataSourceError::InvalidOption {
                key: "mode".to_string(),
                value: "invalid".to_string(),
                cause: None,
            },
        ] {
            assert!(matches!(
                DataFusionError::from(error),
                DataFusionError::External(error) if error.is::<DataSourceError>()
            ));
        }
    }
}
