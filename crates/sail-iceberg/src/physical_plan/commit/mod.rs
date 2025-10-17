pub mod commit_exec;
pub mod types;
// TODO: Implement IcebergCommitExec to consume writer JSON, create manifests via Transaction,
// and return a count batch. Also update table metadata.json and version-hint.
