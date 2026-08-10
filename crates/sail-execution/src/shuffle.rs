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
    pub advertised_host: String,
    pub advertised_port: u16,
    pub host: String,
    pub port: u16,
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
                    .map(|override_| ShuffleEndpointOverride {
                        advertised_host: override_.advertised_host.clone(),
                        advertised_port: override_.advertised_port,
                        host: override_.host.clone(),
                        port: override_.port,
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
            .map(|override_| {
                (
                    (override_.advertised_host.clone(), override_.advertised_port),
                    (override_.host.clone(), override_.port),
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
