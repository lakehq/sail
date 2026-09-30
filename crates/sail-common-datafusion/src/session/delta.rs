/// Session defaults for Delta Lake transactions.
#[derive(Debug, Default)]
pub struct DeltaSessionConfig {
    pub user_metadata: Option<String>,
}
