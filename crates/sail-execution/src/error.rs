use datafusion::common::DataFusionError;
use thiserror::Error;

pub type ExecutionResult<T> = Result<T, ExecutionError>;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
}
