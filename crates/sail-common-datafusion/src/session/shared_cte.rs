use crate::extension::SessionExtension;

/// Whether the references to a CTE definition can share one evaluation of it.
#[derive(Debug)]
pub struct SharedCteConfig {
    enabled: bool,
}

impl SharedCteConfig {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }
}

impl SessionExtension for SharedCteConfig {
    fn name() -> &'static str {
        "SharedCteConfig"
    }
}
