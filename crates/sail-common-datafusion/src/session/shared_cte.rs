use crate::extension::SessionExtension;

/// Whether the references to a CTE definition can share one evaluation of it.
#[derive(Debug)]
pub struct SharedCteConfig {
    enabled: bool,
    spill: bool,
}

impl SharedCteConfig {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            spill: true,
        }
    }

    /// Sets whether the rows that a shared CTE keeps for its references can be written
    /// to temporary files. They cannot in cluster mode, where the task streams keep them.
    pub fn with_spill(mut self, spill: bool) -> Self {
        self.spill = spill;
        self
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn spill(&self) -> bool {
        self.spill
    }
}

impl SessionExtension for SharedCteConfig {
    fn name() -> &'static str {
        "SharedCteConfig"
    }
}
