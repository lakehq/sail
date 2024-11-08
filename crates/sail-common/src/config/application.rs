use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

const DEFAULT_CONFIG: &str = include_str!("default.toml");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub deployment: DeploymentKind,
    pub driver: DriverConfig,
    pub worker: WorkerConfig,
    pub network: NetworkConfig,
    pub execution: ExecutionConfig,
}

impl AppConfig {
    pub fn load() -> CommonResult<Self> {
        Figment::from(Toml::string(DEFAULT_CONFIG))
            .admerge(Env::prefixed("SAIL__").map(|p| p.as_str().replace("__", ".").into()))
            .extract()
            .map_err(|e| CommonError::InvalidArgument(e.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentKind {
    Local,
    LocalCluster,
    KubeCluster,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverConfig {
    pub listen_host: String,
    pub listen_port: u16,
    pub external_host: String,
    pub external_port: Option<u16>,
    pub worker_count_per_job: usize,
    pub job_output_buffer: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub id: u64,
    pub listen_host: String,
    pub listen_port: u16,
    pub external_host: String,
    pub external_port: Option<u16>,
    pub memory_stream_buffer: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub enable_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub batch_size: usize,
    pub parquet: ParquetConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    pub maximum_parallel_row_group_writers: usize,
    pub maximum_buffered_record_batches_per_stream: usize,
}
