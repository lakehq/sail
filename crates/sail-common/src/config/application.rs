use figment::providers::Env;
use figment::value::{Dict, Empty, Map, Tag, Value};
use figment::{Error, Figment, Metadata, Profile, Provider};
use serde::Deserialize;

use crate::config::loader::{
    deserialize_non_empty_string, deserialize_non_zero, deserialize_unknown_unit, ConfigDefinition,
};
use crate::error::{CommonError, CommonResult};

const APP_CONFIG: &str = include_str!("application.yaml");

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub mode: ExecutionMode,
    pub runtime: RuntimeConfig,
    pub cluster: ClusterConfig,
    pub execution: ExecutionConfig,
    pub kubernetes: KubernetesConfig,
    pub parquet: ParquetConfig,
    pub spark: SparkConfig,
    /// Reserved for internal use.
    /// This field ensures that environment variables with prefix `SAIL_INTERNAL_`
    /// can only be used for internal configuration.
    /// Such environment variables are ignored by application configuration.
    #[serde(deserialize_with = "deserialize_unknown_unit")]
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
        Figment::from(ConfigDefinition::new(APP_CONFIG))
            .merge(InternalConfigPlaceholder)
            .merge(Env::prefixed("SAIL_").map(|p| p.as_str().replace("__", ".").into()))
            .extract()
            .map_err(|e| CommonError::InvalidArgument(e.to_string()))
    }
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub stack_size: usize,
    pub enable_secondary: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(from = "retry_strategy::RetryStrategy")]
pub enum RetryStrategy {
    Fixed(FixedRetryStrategy),
    ExponentialBackoff(ExponentialBackoffRetryStrategy),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixedRetryStrategy {
    pub max_count: usize,
    pub delay_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExponentialBackoffRetryStrategy {
    pub max_count: usize,
    pub initial_delay_secs: u64,
    pub max_delay_secs: u64,
    pub factor: u32,
}

mod retry_strategy {
    use serde::Deserialize;

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Type {
        Fixed,
        #[serde(alias = "exponential-backoff")]
        ExponentialBackoff,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RetryStrategy {
        r#type: Type,
        fixed: super::FixedRetryStrategy,
        exponential_backoff: super::ExponentialBackoffRetryStrategy,
    }

    impl From<RetryStrategy> for super::RetryStrategy {
        fn from(value: RetryStrategy) -> Self {
            match value.r#type {
                Type::Fixed => super::RetryStrategy::Fixed(value.fixed),
                Type::ExponentialBackoff => {
                    super::RetryStrategy::ExponentialBackoff(value.exponential_backoff)
                }
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub batch_size: usize,
    pub collect_statistics: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesConfig {
    pub image: String,
    pub image_pull_policy: String,
    pub namespace: String,
    pub driver_pod_name: String,
    pub worker_pod_name_prefix: String,
    pub worker_service_account_name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetConfig {
    pub enable_page_index: bool,
    pub pruning: bool,
    pub skip_metadata: bool,
    pub metadata_size_hint: Option<usize>,
    pub pushdown_filters: bool,
    pub reorder_filters: bool,
    pub schema_force_view_types: bool,
    pub binary_as_string: bool,
    pub data_page_size_limit: usize,
    pub write_batch_size: usize,
    pub writer_version: String,
    pub skip_arrow_metadata: bool,
    pub compression: String,
    pub dictionary_enabled: bool,
    pub dictionary_page_size_limit: usize,
    pub statistics_enabled: String,
    pub max_row_group_size: usize,
    #[serde(deserialize_with = "deserialize_non_zero")]
    pub column_index_truncate_length: Option<usize>,
    #[serde(deserialize_with = "deserialize_non_zero")]
    pub statistics_truncate_length: Option<usize>,
    pub data_page_row_count_limit: usize,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub encoding: Option<String>,
    pub bloom_filter_on_read: bool,
    pub bloom_filter_on_write: bool,
    pub bloom_filter_fpp: f64,
    pub bloom_filter_ndv: u64,
    pub allow_single_file_parallelism: bool,
    pub maximum_parallel_row_group_writers: usize,
    pub maximum_buffered_record_batches_per_stream: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
