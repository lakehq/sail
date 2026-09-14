use std::collections::HashMap;
use std::sync::Arc;

use sail_celeborn::common::{CompressionCodec, PartitionSplitMode};
use sail_celeborn::endpoint::{EndpointResolver, StaticEndpointResolver};
use sail_common::config::{CelebornCompressionCodec, CelebornPartitionSplitMode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShuffleBackendKind {
    Flight,
    Storage {
        path: Option<String>,
        max_file_size: usize,
        compression: ShuffleCompression,
    },
    Celeborn {
        master_endpoints: Vec<String>,
        compression: CompressionCodec,
        heartbeat_interval_secs: u64,
        endpoint_overrides: Vec<ShuffleEndpointOverride>,
        partition_split_threshold: i64,
        partition_split_mode: PartitionSplitMode,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ShuffleEndpointOverride {
    pub internal: String,
    pub external: String,
}

impl From<&sail_common::config::ShuffleBackend> for ShuffleBackendKind {
    fn from(value: &sail_common::config::ShuffleBackend) -> Self {
        match value {
            sail_common::config::ShuffleBackend::Flight => Self::Flight,
            sail_common::config::ShuffleBackend::Storage(storage) => Self::Storage {
                path: storage.path.clone(),
                max_file_size: storage.max_file_size,
                compression: storage.compression.clone().into(),
            },
            sail_common::config::ShuffleBackend::Celeborn(celeborn) => Self::Celeborn {
                master_endpoints: celeborn.master_endpoints.clone(),
                compression: match celeborn.compression {
                    CelebornCompressionCodec::None => CompressionCodec::None,
                    CelebornCompressionCodec::Lz4 => CompressionCodec::Lz4,
                    CelebornCompressionCodec::Zstd { level } => CompressionCodec::Zstd { level },
                },
                heartbeat_interval_secs: celeborn.heartbeat_interval_secs,
                endpoint_overrides: celeborn
                    .endpoint_overrides
                    .iter()
                    .map(|r#override| ShuffleEndpointOverride {
                        internal: r#override.internal.clone(),
                        external: r#override.external.clone(),
                    })
                    .collect(),
                partition_split_threshold: celeborn.partition_split_threshold,
                partition_split_mode: match celeborn.partition_split_mode {
                    CelebornPartitionSplitMode::Soft => PartitionSplitMode::Soft,
                    CelebornPartitionSplitMode::Hard => PartitionSplitMode::Hard,
                },
            },
        }
    }
}

pub fn celeborn_application_id(session_id: &str) -> String {
    format!("sail-session-{session_id}")
}

impl ShuffleBackendKind {
    pub fn celeborn_master_endpoints_string(&self) -> String {
        let Self::Celeborn {
            master_endpoints, ..
        } = self
        else {
            return "[]".to_string();
        };
        #[expect(
            clippy::expect_used,
            reason = "Celeborn master endpoints derive Serialize and TOML values support arrays"
        )]
        toml::Value::try_from(master_endpoints)
            .expect("serializing Celeborn master endpoints")
            .to_string()
    }

    pub fn celeborn_endpoint_overrides_string(&self) -> String {
        let Self::Celeborn {
            endpoint_overrides, ..
        } = self
        else {
            return "[]".to_string();
        };
        #[expect(
            clippy::expect_used,
            reason = "Celeborn endpoint overrides derive Serialize and TOML values support arrays of inline tables"
        )]
        toml::Value::try_from(endpoint_overrides)
            .expect("serializing Celeborn endpoint overrides")
            .to_string()
    }

    pub fn celeborn_endpoint_resolver(&self) -> Option<Arc<dyn EndpointResolver>> {
        let Self::Celeborn {
            endpoint_overrides, ..
        } = self
        else {
            return None;
        };
        if endpoint_overrides.is_empty() {
            return None;
        }
        let overrides = endpoint_overrides
            .iter()
            .map(|r#override| (r#override.internal.clone(), r#override.external.clone()))
            .collect::<HashMap<_, _>>();
        Some(Arc::new(StaticEndpointResolver::from_mappings(overrides)))
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

#[cfg(test)]
mod tests {
    use super::{
        CompressionCodec, PartitionSplitMode, ShuffleBackendKind, ShuffleEndpointOverride,
    };

    #[test]
    fn test_celeborn_endpoint_overrides_string() {
        let backend = ShuffleBackendKind::Celeborn {
            master_endpoints: vec!["master:12097".to_string()],
            compression: CompressionCodec::Lz4,
            heartbeat_interval_secs: 10,
            endpoint_overrides: vec![ShuffleEndpointOverride {
                internal: "celeborn-worker:12000".to_string(),
                external: "127.0.0.1:32000".to_string(),
            }],
            partition_split_threshold: 1_i64 << 30,
            partition_split_mode: PartitionSplitMode::Soft,
        };

        assert_eq!(
            backend.celeborn_endpoint_overrides_string(),
            "[{ external = \"127.0.0.1:32000\", internal = \"celeborn-worker:12000\" }]"
        );
    }

    #[test]
    fn test_celeborn_master_endpoints_string() {
        let backend = ShuffleBackendKind::Celeborn {
            master_endpoints: vec!["master-1:12097".to_string(), "master-2:12097".to_string()],
            compression: CompressionCodec::Lz4,
            heartbeat_interval_secs: 10,
            endpoint_overrides: vec![],
            partition_split_threshold: 1_i64 << 30,
            partition_split_mode: PartitionSplitMode::Soft,
        };

        assert_eq!(
            backend.celeborn_master_endpoints_string(),
            "[\"master-1:12097\", \"master-2:12097\"]"
        );
    }

    #[test]
    fn test_non_celeborn_endpoint_overrides_string_is_empty() {
        assert_eq!(
            ShuffleBackendKind::Flight.celeborn_endpoint_overrides_string(),
            "[]"
        );
    }

    #[test]
    fn test_non_celeborn_master_endpoints_string_is_empty() {
        assert_eq!(
            ShuffleBackendKind::Flight.celeborn_master_endpoints_string(),
            "[]"
        );
    }
}
