use sail_common::config::{AppConfig, DeploymentKind};

use crate::error::{ExecutionError, ExecutionResult};

#[derive(Debug)]
pub struct DriverOptions {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: Option<u16>,
    // TODO: support dynamic worker allocation
    pub worker_count_per_job: usize,
    pub job_output_buffer: usize,
    pub worker_manager_kind: WorkerManagerKind,
}

#[derive(Debug)]
pub enum WorkerManagerKind {
    Local,
    Kubernetes,
}

impl TryFrom<&AppConfig> for DriverOptions {
    type Error = ExecutionError;
    fn try_from(config: &AppConfig) -> ExecutionResult<Self> {
        let worker_manager_kind = match config.deployment {
            DeploymentKind::Local => {
                return Err(ExecutionError::InvalidArgument(
                    "local deployment is not supposed to work with the driver".to_string(),
                ))
            }
            DeploymentKind::LocalCluster => WorkerManagerKind::Local,
            DeploymentKind::KubeCluster => WorkerManagerKind::Kubernetes,
        };
        Ok(Self {
            enable_tls: config.network.enable_tls,
            driver_listen_host: config.driver.listen_host.clone(),
            driver_listen_port: config.driver.listen_port,
            driver_external_host: config.driver.external_host.clone(),
            driver_external_port: config.driver.external_port,
            worker_count_per_job: config.driver.worker_count_per_job,
            job_output_buffer: config.driver.job_output_buffer,
            worker_manager_kind,
        })
    }
}
