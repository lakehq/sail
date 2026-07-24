#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShuffleServiceKind {
    None,
    Storage {
        path: String,
        max_file_size: usize,
        compression: ShuffleCompression,
    },
}

impl From<&sail_common::config::ShuffleService> for ShuffleServiceKind {
    fn from(value: &sail_common::config::ShuffleService) -> Self {
        match value {
            sail_common::config::ShuffleService::None => Self::None,
            sail_common::config::ShuffleService::Storage(storage) => Self::Storage {
                path: storage.path.clone(),
                max_file_size: storage.max_file_size,
                compression: storage.compression.into(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShuffleCompression {
    None,
    Lz4,
    Zstd,
}

impl From<sail_common::config::ShuffleCompression> for ShuffleCompression {
    fn from(value: sail_common::config::ShuffleCompression) -> Self {
        match value {
            sail_common::config::ShuffleCompression::None => Self::None,
            sail_common::config::ShuffleCompression::Lz4 => Self::Lz4,
            sail_common::config::ShuffleCompression::Zstd => Self::Zstd,
        }
    }
}
