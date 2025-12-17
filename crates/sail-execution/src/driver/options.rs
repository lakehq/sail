use std::time::Duration;

use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_server::RetryStrategy;

use crate::error::{ExecutionError, ExecutionResult};
use crate::worker_manager::KubernetesWorkerManagerOptions;

#[derive(Debug)]
#[readonly::make]
pub struct DriverOptions {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_initial_count: usize,
    pub worker_max_count: usize,
    pub worker_task_slots: usize,
    pub worker_max_idle_time: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub worker_launch_timeout: Duration,
    pub worker_stream_buffer: usize,
    pub task_launch_timeout: Duration,
    pub job_output_buffer: usize,
    pub rpc_retry_strategy: RetryStrategy,
    pub worker_manager: WorkerManagerOptions,
    pub runtime: RuntimeHandle,
}

#[derive(Debug)]
pub enum WorkerManagerOptions {
    Local,
    Kubernetes(KubernetesWorkerManagerOptions),
}

impl DriverOptions {
    pub fn try_new(config: &AppConfig, runtime: RuntimeHandle) -> ExecutionResult<Self> {
        let worker_manager = match config.mode {
            ExecutionMode::Local => {
                return Err(ExecutionError::InvalidArgument(
                    "local deployment is not supposed to work with the driver".to_string(),
                ))
            }
            ExecutionMode::LocalCluster => WorkerManagerOptions::Local,
            ExecutionMode::KubernetesCluster => {
                WorkerManagerOptions::Kubernetes(KubernetesWorkerManagerOptions {
                    image: config.kubernetes.image.clone(),
                    image_pull_policy: config.kubernetes.image_pull_policy.clone(),
                    namespace: config.kubernetes.namespace.clone(),
                    driver_pod_name: config.kubernetes.driver_pod_name.clone(),
                    worker_pod_name_prefix: config.kubernetes.worker_pod_name_prefix.clone(),
                    worker_service_account_name: config
                        .kubernetes
                        .worker_service_account_name
                        .clone(),
                    worker_pod_template: config.kubernetes.worker_pod_template.clone(),
                })
            }
        };
        Ok(Self {
            enable_tls: config.cluster.enable_tls,
            driver_listen_host: config.cluster.driver_listen_host.clone(),
            driver_listen_port: config.cluster.driver_listen_port,
            driver_external_host: config.cluster.driver_external_host.clone(),
            driver_external_port: config.cluster.driver_external_port,
            worker_initial_count: config.cluster.worker_initial_count,
            worker_max_count: config.cluster.worker_max_count,
            worker_task_slots: config.cluster.worker_task_slots,
            worker_max_idle_time: Duration::from_secs(config.cluster.worker_max_idle_time_secs),
            worker_heartbeat_interval: Duration::from_secs(
                config.cluster.worker_heartbeat_interval_secs,
            ),
            worker_heartbeat_timeout: Duration::from_secs(
                config.cluster.worker_heartbeat_timeout_secs,
            ),
            worker_launch_timeout: Duration::from_secs(config.cluster.worker_launch_timeout_secs),
            rpc_retry_strategy: (&config.cluster.rpc_retry_strategy).into(),
            worker_stream_buffer: config.cluster.worker_stream_buffer,
            task_launch_timeout: Duration::from_secs(config.cluster.task_launch_timeout_secs),
            job_output_buffer: config.cluster.job_output_buffer,
            worker_manager,
            runtime,
        })
    }
}
