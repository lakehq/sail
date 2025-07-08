pub mod writer;

use datafusion_common::DataFusionError;
use deltalake::errors::DeltaTableError;
pub use writer::WriteBuilder;

#[derive(thiserror::Error, Debug)]
pub(crate) enum WriteError {
    #[allow(dead_code)]
    #[error("No data source supplied to write command.")]
    MissingData,

    #[error("Failed to execute write task: {source}")]
    WriteTask { source: tokio::task::JoinError },

    #[error("A table already exists at: {0}")]
    AlreadyExists(String),

    #[error(
        "Specified table partitioning does not match table partitioning: expected: {expected:?}, got: {got:?}",
    )]
    PartitionColumnMismatch {
        expected: Vec<String>,
        got: Vec<String>,
    },

    #[error("Failed to create physical plan: {source}")]
    PhysicalPlan { source: DataFusionError },

    #[error("Delta writer error: {source}")]
    DeltaWriter { source: DeltaTableError },

    #[error("Missing input DataFrame")]
    MissingDataFrame,

    #[error("Missing session state")]
    MissingSessionState,

    #[error("Schema validation failed: {message}")]
    SchemaValidation { message: String },

    #[error("Commit failed: {message}")]
    CommitFailed { message: String },
}

impl From<DataFusionError> for WriteError {
    fn from(err: DataFusionError) -> Self {
        WriteError::PhysicalPlan { source: err }
    }
}

impl From<DeltaTableError> for WriteError {
    fn from(err: DeltaTableError) -> Self {
        WriteError::DeltaWriter { source: err }
    }
}

impl From<WriteError> for DeltaTableError {
    fn from(err: WriteError) -> Self {
        match err {
            WriteError::DeltaWriter { source } => source,
            _ => DeltaTableError::GenericError {
                source: Box::new(err),
            },
        }
    }
}
