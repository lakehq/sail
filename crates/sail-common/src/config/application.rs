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
    pub spark: SparkConfig,
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
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Local,
    #[serde(alias = "local-cluster")]
    LocalCluster,
    #[serde(
        alias = "kubernetes-cluster",
        alias = "k8s-cluster",
        alias = "k8s_cluster",
        alias = "kube-cluster",
        alias = "kube_cluster"
    )]
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
    pub worker_heartbeat_interval_secs: u64,
    pub worker_heartbeat_timeout_secs: u64,
    pub worker_launch_timeout_secs: u64,
    pub worker_task_slots: usize,
    pub worker_stream_buffer: usize,
    pub task_launch_timeout_secs: u64,
    pub job_output_buffer: usize,
    pub rpc_retry_strategy: RetryStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryStrategy {
    Fixed {
        max_count: usize,
        delay_secs: u64,
    },
    ExponentialBackoff {
        max_count: usize,
        initial_delay_secs: u64,
        max_delay_secs: u64,
        factor: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub batch_size: usize,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    pub maximum_parallel_row_group_writers: usize,
    pub maximum_buffered_record_batches_per_stream: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkConfig {
    pub session_timeout_secs: u64,
}

/// Environment variables for application cluster configuration.
pub struct ClusterConfigEnv;

macro_rules! define_cluster_config_env {
    ($($name:ident),* $(,)?) => {
        $(pub const $name: &'static str = concat!("SAIL_CLUSTER__", stringify!($name));)*
    };
}

impl ClusterConfigEnv {
    define_cluster_config_env! {
        ENABLE_TLS,
        DRIVER_EXTERNAL_HOST,
        DRIVER_EXTERNAL_PORT,
        WORKER_ID,
        WORKER_LISTEN_HOST,
        WORKER_EXTERNAL_HOST,
        WORKER_HEARTBEAT_INTERVAL_SECS,
        WORKER_STREAM_BUFFER,
        RPC_RETRY_STRATEGY,
    }
}
