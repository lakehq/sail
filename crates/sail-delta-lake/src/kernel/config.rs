/// Configuration options for the local Delta table integration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaTableConfig {
    /// Whether snapshots should eagerly load file level metadata.
    pub require_files: bool,
    /// Number of log files buffered concurrently when replaying the Delta log.
    pub log_buffer_size: usize,
    /// Number of log entries pulled per batch when materializing logs.
    pub log_batch_size: usize,
}

impl Default for DeltaTableConfig {
    fn default() -> Self {
        Self {
            require_files: true,
            log_buffer_size: default_parallelism() * 4,
            log_batch_size: 1024,
        }
    }
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
