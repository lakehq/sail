use crate::extension::SessionExtension;

/// Whether the references to a CTE definition can share one evaluation of it.
#[derive(Debug)]
pub struct SharedCteConfig {
    enabled: bool,
    spills_kept_rows: bool,
}

impl SharedCteConfig {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            spills_kept_rows: true,
        }
    }

    /// Sets whether the rows that a shared CTE keeps for its references can be written
    /// to temporary files. They cannot in cluster mode, where the task streams keep them.
    pub fn with_spills_kept_rows(mut self, spills_kept_rows: bool) -> Self {
        self.spills_kept_rows = spills_kept_rows;
        self
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn spills_kept_rows(&self) -> bool {
        self.spills_kept_rows
    }
}

impl SessionExtension for SharedCteConfig {
    fn name() -> &'static str {
        "SharedCteConfig"
    }
}
