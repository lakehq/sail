use std::time::Duration;

use sail_common::config::{AppConfig, DeploymentKind};

use crate::error::{ExecutionError, ExecutionResult};

#[derive(Debug)]
pub struct DriverOptions {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: Option<u16>,
    pub worker_initial_count: usize,
    pub worker_max_count: Option<usize>,
    pub worker_task_slots: usize,
    pub worker_max_idle_time: Duration,
    pub worker_launch_timeout: Duration,
    pub task_launch_timeout: Duration,
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
            worker_initial_count: config.driver.worker_initial_count,
            worker_max_count: config.driver.worker_max_count,
            worker_task_slots: config.driver.worker_task_slots,
            worker_max_idle_time: Duration::from_secs(config.driver.worker_max_idle_time_secs),
            worker_launch_timeout: Duration::from_secs(config.driver.worker_launch_timeout_secs),
            task_launch_timeout: Duration::from_secs(config.driver.task_launch_timeout_secs),
            job_output_buffer: config.driver.job_output_buffer,
            worker_manager_kind,
        })
    }
}
