use deltalake::kernel::Action;
use deltalake::protocol::DeltaOperation;
use serde::{Deserialize, Serialize};

mod commit_exec;
mod plan_builder;
mod project_exec;
mod repartition_exec;
mod sort_exec;
mod writer_exec;

pub use commit_exec::DeltaCommitExec;
pub use plan_builder::DeltaPlanBuilder;
pub use project_exec::DeltaProjectExec;
pub use repartition_exec::DeltaRepartitionExec;
pub use sort_exec::DeltaSortExec;
pub use writer_exec::DeltaWriterExec;

/// Helper struct for serializing commit information into a single JSON field
#[derive(Serialize, Deserialize)]
pub struct CommitInfo {
    pub row_count: u64,
    pub add_actions: Vec<deltalake::kernel::Add>,
    pub schema_actions: Vec<Action>,
    pub initial_actions: Vec<Action>,
    pub operation: Option<DeltaOperation>,
}
