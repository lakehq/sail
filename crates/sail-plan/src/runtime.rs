use tokio::runtime::Handle;

#[derive(Debug, Default)]
pub struct RuntimeExtension {
    handle: Option<Handle>,
}

impl RuntimeExtension {
    pub fn new(handle: Handle) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    pub fn handle(&self) -> Option<Handle> {
        self.handle.clone()
    }
}
