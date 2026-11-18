use crate::extension::SessionExtension;

#[derive(Debug)]
pub struct ShuffleWriteConfig {
    buffer_size: usize,
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
