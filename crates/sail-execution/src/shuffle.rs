use std::collections::HashMap;
use std::sync::Arc;

use sail_celeborn::endpoint::{EndpointResolver, StaticEndpointResolver};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShuffleBackendKind {
    Flight,
    Storage {
        path: Option<String>,
        max_file_size: usize,
        compression: ShuffleCompression,
    },
    Celeborn {
        master_host: String,
        master_port: u16,
        endpoint_overrides: Vec<ShuffleEndpointOverride>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ShuffleEndpointOverride {
    pub internal_host: String,
    pub internal_port: u16,
    pub external_host: String,
    pub external_port: u16,
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
                master_host: celeborn.master_host.clone(),
                master_port: celeborn.master_port,
                endpoint_overrides: celeborn
                    .endpoint_overrides
                    .iter()
                    .map(|r#override| ShuffleEndpointOverride {
                        internal_host: r#override.internal_host.clone(),
                        internal_port: r#override.internal_port,
                        external_host: r#override.external_host.clone(),
                        external_port: r#override.external_port,
                    })
                    .collect(),
            },
        }
    }
}

pub fn celeborn_application_id(session_id: &str) -> String {
    format!("sail-session-{session_id}")
}

impl ShuffleBackendKind {
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
            .map(|r#override| {
                (
                    (r#override.internal_host.clone(), r#override.internal_port),
                    (r#override.external_host.clone(), r#override.external_port),
                )
            })
            .collect::<HashMap<_, _>>();
        Some(Arc::new(StaticEndpointResolver::new(overrides)))
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
    use super::{ShuffleBackendKind, ShuffleEndpointOverride};

    #[test]
    fn test_celeborn_endpoint_overrides_string() {
        let backend = ShuffleBackendKind::Celeborn {
            master_host: "master".to_string(),
            master_port: 12097,
            endpoint_overrides: vec![ShuffleEndpointOverride {
                internal_host: "celeborn-worker".to_string(),
                internal_port: 12000,
                external_host: "127.0.0.1".to_string(),
                external_port: 32000,
            }],
        };

        assert_eq!(
            backend.celeborn_endpoint_overrides_string(),
            "[{ external_host = \"127.0.0.1\", external_port = 32000, internal_host = \"celeborn-worker\", internal_port = 12000 }]"
        );
    }

    #[test]
    fn test_non_celeborn_endpoint_overrides_string_is_empty() {
        assert_eq!(
            ShuffleBackendKind::Flight.celeborn_endpoint_overrides_string(),
            "[]"
        );
    }
}
