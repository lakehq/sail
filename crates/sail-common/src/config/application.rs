use figment::providers::{Env, Format, Toml};
use figment::value::{Dict, Empty, Map, Tag, Value};
use figment::{Error, Figment, Metadata, Profile, Provider};
use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

const DEFAULT_CONFIG: &str = include_str!("default.toml");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub mode: ExecutionMode,
    pub cluster: ClusterConfig,
    pub execution: ExecutionConfig,
    pub kubernetes: KubernetesConfig,
    pub parquet: ParquetConfig,
    /// Reserved for internal use.
    /// This field ensures that environment variables with prefix `SAIL_INTERNAL_`
    /// can only be used for internal configuration.
    /// Such environment variables are ignored by application configuration.
    pub internal: (),
}

/// A configuration provider that injects placeholder internal configuration.
struct InternalConfigPlaceholder;

impl Provider for InternalConfigPlaceholder {
    fn metadata(&self) -> Metadata {
        Metadata::named("Internal")
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        Ok(Map::from([(
            Profile::Default,
            Dict::from([(
                "internal".to_string(),
                Value::Empty(Tag::Default, Empty::Unit),
            )]),
        )]))
    }
}

impl AppConfig {
    pub fn load() -> CommonResult<Self> {
        Figment::from(Toml::string(DEFAULT_CONFIG))
            .merge(InternalConfigPlaceholder)
            .merge(Env::prefixed("SAIL_").map(|p| p.as_str().replace("__", ".").into()))
            .extract()
            .map_err(|e| CommonError::InvalidArgument(e.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionMode {
    Local,
    LocalCluster,
    KubernetesCluster,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_id: u64,
    pub worker_listen_host: String,
    pub worker_listen_port: u16,
    pub worker_external_host: String,
    pub worker_external_port: u16,
    pub worker_initial_count: usize,
    pub worker_max_count: usize,
    pub worker_max_idle_time_secs: u64,
    pub worker_launch_timeout_secs: u64,
    pub worker_task_slots: usize,
    pub task_launch_timeout_secs: u64,
    pub job_output_buffer: usize,
    pub memory_stream_buffer: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    pub maximum_parallel_row_group_writers: usize,
    pub maximum_buffered_record_batches_per_stream: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubernetesConfig {
    pub image: String,
    pub image_pull_policy: String,
    pub namespace: String,
    /// The name of the pod that runs the driver,
    /// or empty if the driver pod name is not known.
    /// This is used to set owner references for worker pods.
    pub driver_pod_name: String,
    /// The prefix of the name of worker pods.
    /// This should usually end with a hyphen (`-`).
    pub worker_pod_name_prefix: String,
}

/// Environment variables for application cluster configuration.
pub struct ClusterConfigEnv;

impl ClusterConfigEnv {
    pub const ENABLE_TLS: &'static str = "SAIL_CLUSTER__ENABLE_TLS";
    pub const WORKER_ID: &'static str = "SAIL_CLUSTER__WORKER_ID";
    pub const WORKER_LISTEN_HOST: &'static str = "SAIL_CLUSTER__WORKER_LISTEN_HOST";
    pub const WORKER_EXTERNAL_HOST: &'static str = "SAIL_CLUSTER__WORKER_EXTERNAL_HOST";
    pub const DRIVER_EXTERNAL_HOST: &'static str = "SAIL_CLUSTER__DRIVER_EXTERNAL_HOST";
    pub const DRIVER_EXTERNAL_PORT: &'static str = "SAIL_CLUSTER__DRIVER_EXTERNAL_PORT";
}
