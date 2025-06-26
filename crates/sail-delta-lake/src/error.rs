use deltalake::DeltaTableError;

/// Result type for sail-delta-lake operations using delta-rs native errors
pub type DeltaResult<T> = Result<T, DeltaTableError>;
