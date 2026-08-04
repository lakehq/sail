use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

use datafusion::common::{Result, internal_err};
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::job::{JobRunner, JobRunnerHistoryReporter};
use sail_execution::DriverId;
use sail_execution::driver::{DriverComponents, DriverHandle, DriverOptions};
use sail_execution::job_runner::{ClusterJobRunner, LocalJobRunner};
use sail_execution::worker_manager::{
    KubernetesWorkerManager, KubernetesWorkerManagerOptions, LocalWorkerManager,
};
use sail_server::actor::ActorSystem;

use crate::session_factory::{SessionFactory, WorkerSessionFactory};

pub struct SessionJobRunner {
    runner: Box<dyn JobRunner>,
    driver: Option<DriverHandle>,
}

impl SessionJobRunner {
    fn local(runner: LocalJobRunner) -> Self {
        Self {
            runner: Box::new(runner),
            driver: None,
        }
    }

    fn cluster(runner: ClusterJobRunner) -> Self {
        let driver = runner.driver();
        Self {
            runner: Box::new(runner),
            driver: Some(driver),
        }
    }

    pub(crate) fn into_parts(self) -> (Box<dyn JobRunner>, Option<DriverHandle>) {
        (self.runner, self.driver)
    }
}

pub struct SessionJobRunnerInfo {
    pub session_id: String,
    pub driver_id: DriverId,
    pub driver_server_port: Option<u16>,
    pub history_reporter: Box<dyn JobRunnerHistoryReporter>,
}

pub trait SessionJobRunnerFactory: Send {
    fn create(&mut self, info: SessionJobRunnerInfo) -> Result<SessionJobRunner>;
}

pub struct ServerSessionJobRunnerFactory {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: Arc<Mutex<ActorSystem>>,
}

impl ServerSessionJobRunnerFactory {
    pub fn new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        system: Arc<Mutex<ActorSystem>>,
    ) -> Self {
        Self {
            config,
            runtime,
            system,
        }
    }

    fn create_cluster_runner(
        &self,
        info: SessionJobRunnerInfo,
        worker_manager: Box<dyn sail_execution::worker_manager::WorkerManager>,
    ) -> Result<SessionJobRunner> {
        let Some(port) = info.driver_server_port else {
            return internal_err!("driver gateway is not available");
        };
        let options = DriverOptions::new(
            &self.config,
            self.runtime.clone(),
            info.session_id,
            info.driver_id,
            port,
        );
        let components = DriverComponents {
            worker_manager,
            history_reporter: info.history_reporter,
        };
        let mut system = self
            .system
            .lock()
            .map_err(|e| datafusion::common::internal_datafusion_err!("{e}"))?;
        Ok(SessionJobRunner::cluster(ClusterJobRunner::new(
            system.deref_mut(),
            options,
            components,
        )))
    }
}

impl SessionJobRunnerFactory for ServerSessionJobRunnerFactory {
    fn create(&mut self, info: SessionJobRunnerInfo) -> Result<SessionJobRunner> {
        match self.config.mode {
            ExecutionMode::Local => Ok(SessionJobRunner::local(LocalJobRunner::new(
                info.history_reporter,
            ))),
            ExecutionMode::LocalCluster => {
                let worker_session =
                    WorkerSessionFactory::new(self.config.clone(), self.runtime.clone())
                        .create(())?;
                self.create_cluster_runner(
                    info,
                    Box::new(LocalWorkerManager::new(
                        self.runtime.clone(),
                        worker_session,
                    )),
                )
            }
            ExecutionMode::KubernetesCluster => {
                let options = KubernetesWorkerManagerOptions {
                    image: self.config.kubernetes.image.clone(),
                    image_pull_policy: self.config.kubernetes.image_pull_policy.clone(),
                    namespace: self.config.kubernetes.namespace.clone(),
                    driver_pod_name: self.config.kubernetes.driver_pod_name.clone(),
                    worker_pod_name_prefix: self.config.kubernetes.worker_pod_name_prefix.clone(),
                    worker_service_account_name: self
                        .config
                        .kubernetes
                        .worker_service_account_name
                        .clone(),
                    worker_pod_template: self.config.kubernetes.worker_pod_template.clone(),
                };
                self.create_cluster_runner(info, Box::new(KubernetesWorkerManager::new(options)))
            }
        }
    }
}
