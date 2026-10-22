use crate::extension::SessionExtension;

pub const DEFAULT_REPARTITION_BUFFER_SIZE: usize = 16;

#[derive(Debug)]
pub struct RepartitionBufferConfig {
    buffer_size: usize,
}

impl Default for RepartitionBufferConfig {
    fn default() -> Self {
        Self::new(DEFAULT_REPARTITION_BUFFER_SIZE)
    }
}

impl RepartitionBufferConfig {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size: buffer_size.max(1),
        }
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

impl SessionExtension for RepartitionBufferConfig {
    fn name() -> &'static str {
        "RepartitionBufferConfig"
    }
}
