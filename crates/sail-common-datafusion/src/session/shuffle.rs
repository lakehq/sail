use crate::extension::SessionExtension;

/// The buffer size of a shuffle write in a session without [`ShuffleWriteConfig`].
/// The default of `cluster.shuffle_write_buffer_size` is in the application configuration.
pub const DEFAULT_SHUFFLE_WRITE_BUFFER_SIZE: usize = 32 * 1024 * 1024;

#[derive(Debug)]
pub struct ShuffleWriteConfig {
    buffer_size: usize,
}

impl Default for ShuffleWriteConfig {
    fn default() -> Self {
        Self::new(DEFAULT_SHUFFLE_WRITE_BUFFER_SIZE)
    }
}

impl ShuffleWriteConfig {
    pub fn new(buffer_size: usize) -> Self {
        Self { buffer_size }
    }

    /// The size in bytes of the rows that a task holds for a shuffle write,
    /// or `0` for no limit on the size.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

impl SessionExtension for ShuffleWriteConfig {
    fn name() -> &'static str {
        "ShuffleWriteConfig"
    }
}
