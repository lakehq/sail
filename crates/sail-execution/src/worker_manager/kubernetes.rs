use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, ObjectFieldSelector, Pod, PodSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Api;
use tokio::sync::OnceCell;

use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

pub struct KubernetesWorkerManager {
    pods: OnceCell<Api<Pod>>,
}

impl KubernetesWorkerManager {
    pub fn new() -> Self {
        Self {
            pods: OnceCell::new(),
        }
    }

    async fn pods(&self) -> ExecutionResult<&Api<Pod>> {
        let pods = self
            .pods
            .get_or_try_init(|| async {
                kube::Client::try_default()
                    .await
                    .map(Api::default_namespaced)
            })
            .await?;
        Ok(pods)
    }
}

#[tonic::async_trait]
impl WorkerManager for KubernetesWorkerManager {
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        let p = Pod {
            metadata: ObjectMeta {
                name: Some(format!("worker-{}", id)),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "worker".to_string(),
                    args: Some(vec!["worker".to_string()]),
                    env: Some(vec![
                        EnvVar {
                            name: "RUST_LOG".to_string(),
                            value: Some("debug".to_string()),
                            value_from: None,
                        },
                        EnvVar {
                            name: "SAIL__NETWORK__ENABLE_TLS".to_string(),
                            value: Some(if options.enable_tls {
                                "true".to_string()
                            } else {
                                "false".to_string()
                            }),
                            value_from: None,
                        },
                        EnvVar {
                            name: "SAIL__WORKER__ID".to_string(),
                            value: Some(u64::from(id).to_string()),
                            value_from: None,
                        },
                        EnvVar {
                            name: "SAIL__WORKER__LISTEN_HOST".to_string(),
                            value: Some("0.0.0.0".to_string()),
                            value_from: None,
                        },
                        EnvVar {
                            name: "SAIL__WORKER__EXTERNAL_HOST".to_string(),
                            value: None,
                            value_from: Some(EnvVarSource {
                                field_ref: Some(ObjectFieldSelector {
                                    field_path: "status.podIP".to_string(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                        },
                        EnvVar {
                            name: "SAIL__DRIVER__EXTERNAL_HOST".to_string(),
                            value: Some(options.driver_external_host),
                            value_from: None,
                        },
                        EnvVar {
                            name: "SAIL__DRIVER__EXTERNAL_PORT".to_string(),
                            value: Some(options.driver_external_port.to_string()),
                            value_from: None,
                        },
                    ]),
                    image: Some("sail:latest".to_string()),
                    image_pull_policy: Some("IfNotPresent".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };
        let pp = Default::default();
        self.pods().await?.create(&pp, &p).await?;
        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        Ok(())
    }
}
