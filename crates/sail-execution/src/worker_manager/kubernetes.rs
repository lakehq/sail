use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;

use fastrace::collector::SpanContext;
use futures::future::BoxFuture;
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, ObjectFieldSelector, Pod, PodSpec, PodTemplateSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::{DeepMerge, Resource};
use kube::Api;
use kube::api::{DeleteParams, ListParams};
use rand::RngExt;
use rand::distr::Uniform;
use sail_common::actor::ActorSystem;
use sail_common::config::{ClusterConfigEnv, ExecutionConfigEnv};
use sail_common::telemetry::ContextPropagationEnv;
use sail_common::utils::retry::RetryStrategy;
use tokio::sync::OnceCell;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::shuffle::ShuffleBackendKind;
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

#[derive(Debug, Clone)]
pub struct KubernetesWorkerManagerOptions {
    pub image: String,
    pub image_pull_policy: String,
    pub namespace: String,
    pub driver_pod_name: String,
    pub worker_pod_name_prefix: String,
    pub worker_service_account_name: String,
    pub worker_pod_template: String,
}

pub struct KubernetesWorkerService {
    /// An opaque name that can be used to create names to uniquely identify Kubernetes resources.
    name: String,
    options: KubernetesWorkerManagerOptions,
    pods: OnceCell<Api<Pod>>,
}

pub struct KubernetesWorkerManager {
    service: Arc<KubernetesWorkerService>,
}

impl KubernetesWorkerManager {
    pub fn new(options: KubernetesWorkerManagerOptions) -> Self {
        Self {
            service: Arc::new(KubernetesWorkerService::new(options)),
        }
    }
}

impl KubernetesWorkerService {
    fn new(options: KubernetesWorkerManagerOptions) -> Self {
        Self {
            name: Self::generate_name(),
            options,
            pods: OnceCell::new(),
        }
    }

    fn generate_name() -> String {
        #[expect(clippy::unwrap_used)]
        rand::rng()
            .sample_iter(Uniform::new(0, 36).unwrap())
            .take(10)
            .map(|i| if i < 10 { b'0' + i } else { b'a' + i - 10 })
            .map(char::from)
            .collect()
    }

    async fn pods(&self) -> ExecutionResult<&Api<Pod>> {
        let pods = self
            .pods
            .get_or_try_init(|| async {
                kube::Client::try_default()
                    .await
                    .map(|client| Api::namespaced(client, &self.options.namespace))
            })
            .await?;
        Ok(pods)
    }

    async fn get_owner_references(&self) -> ExecutionResult<Vec<OwnerReference>> {
        if self.options.driver_pod_name.is_empty() {
            // The driver pod name is not known.
            return Ok(vec![]);
        }
        let driver = self
            .pods()
            .await?
            .get(&self.options.driver_pod_name)
            .await?;
        Ok(vec![OwnerReference {
            api_version: Pod::API_VERSION.to_string(),
            kind: "Pod".to_string(),
            name: driver.metadata.name.ok_or_else(|| {
                ExecutionError::InternalError("driver pod name is missing".to_string())
            })?,
            uid: driver.metadata.uid.ok_or_else(|| {
                ExecutionError::InternalError("driver pod UID is missing".to_string())
            })?,
            ..Default::default()
        }])
    }

    fn build_pod_labels(&self, id: WorkerId) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("app.kubernetes.io/name".to_string(), "sail".to_string()),
            (
                "app.kubernetes.io/component".to_string(),
                "worker".to_string(),
            ),
            (
                "app.kubernetes.io/instance".to_string(),
                format!("{}-{id}", self.name),
            ),
            (
                "sail.lakesail.com/worker-manager".to_string(),
                self.name.clone(),
            ),
        ])
    }

    fn build_pod_env(&self, id: WorkerId, options: WorkerLaunchOptions) -> Vec<EnvVar> {
        let WorkerLaunchOptions {
            enable_tls,
            batch_size,
            driver_id,
            session_id,
            driver_external_host,
            driver_external_port,
            worker_heartbeat_interval,
            task_stream_buffer,
            task_stream_creation_timeout,
            rpc_retry_strategy,
            shuffle_backend,
        } = options;
        let w3c_traceparent =
            SpanContext::current_local_parent().map(|x| x.encode_w3c_traceparent());

        // There is no guarantee that serializing a Rust data structure produces an inline table,
        // so we have to construct the nested TOML value manually.
        let rpc_retry_strategy = match rpc_retry_strategy {
            RetryStrategy::ExponentialBackoff {
                max_count,
                initial_delay,
                max_delay,
                factor,
            } => {
                format!(
                    "{{ exponential_backoff = {{ max_count = {}, initial_delay_secs = {}, max_delay_secs = {}, factor = {} }} }}",
                    max_count,
                    initial_delay.as_secs(),
                    max_delay.as_secs(),
                    factor,
                )
            }
            RetryStrategy::Fixed { max_count, delay } => {
                format!(
                    "{{ fixed = {{ max_count = {}, delay_secs = {} }} }}",
                    max_count,
                    delay.as_secs()
                )
            }
        };
        let mut env = vec![
            EnvVar {
                name: "RUST_LOG".to_string(),
                value: Some(env::var("RUST_LOG").unwrap_or("info".to_string())),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::ENABLE_TLS.to_string(),
                value: Some(enable_tls.to_string()),
                value_from: None,
            },
            EnvVar {
                name: ExecutionConfigEnv::BATCH_SIZE.to_string(),
                value: Some(batch_size.to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::DRIVER_EXTERNAL_HOST.to_string(),
                value: Some(driver_external_host),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::DRIVER_EXTERNAL_PORT.to_string(),
                value: Some(driver_external_port.to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::DRIVER_ID.to_string(),
                value: Some(u64::from(driver_id).to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::SESSION_ID.to_string(),
                value: Some(session_id),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::WORKER_ID.to_string(),
                value: Some(u64::from(id).to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::WORKER_LISTEN_HOST.to_string(),
                value: Some("0.0.0.0".to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::WORKER_EXTERNAL_HOST.to_string(),
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
                name: ClusterConfigEnv::WORKER_HEARTBEAT_INTERVAL_SECS.to_string(),
                value: Some(worker_heartbeat_interval.as_secs().to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::TASK_STREAM_BUFFER.to_string(),
                value: Some(task_stream_buffer.to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::TASK_STREAM_CREATION_TIMEOUT_SECS.to_string(),
                value: Some(task_stream_creation_timeout.as_secs().to_string()),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::RPC_RETRY_STRATEGY.to_string(),
                value: Some(rpc_retry_strategy),
                value_from: None,
            },
            EnvVar {
                name: ClusterConfigEnv::SHUFFLE_BACKEND__TYPE.to_string(),
                value: Some(
                    match &shuffle_backend {
                        ShuffleBackendKind::Flight { .. } => "flight",
                        ShuffleBackendKind::Storage { .. } => "storage",
                        ShuffleBackendKind::Celeborn { .. } => "celeborn",
                    }
                    .to_string(),
                ),
                value_from: None,
            },
        ];
        if let ShuffleBackendKind::Flight {
            compression,
            connection_count,
            initial_stream_window_size,
            initial_connection_window_size,
        } = &shuffle_backend
        {
            env.push(EnvVar {
                name: ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__COMPRESSION.to_string(),
                value: Some(compression.to_string()),
                value_from: None,
            });
            env.push(EnvVar {
                name: ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__CONNECTION_COUNT.to_string(),
                value: Some(connection_count.to_string()),
                value_from: None,
            });
            env.push(EnvVar {
                name: ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__INITIAL_STREAM_WINDOW_SIZE
                    .to_string(),
                value: Some(initial_stream_window_size.unwrap_or(0).to_string()),
                value_from: None,
            });
            env.push(EnvVar {
                name: ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__INITIAL_CONNECTION_WINDOW_SIZE
                    .to_string(),
                value: Some(initial_connection_window_size.unwrap_or(0).to_string()),
                value_from: None,
            });
        }
        if let ShuffleBackendKind::Storage {
            path,
            max_file_size,
            compression,
        } = &shuffle_backend
        {
            if let Some(path) = path {
                env.push(EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__PATH.to_string(),
                    value: Some(path.clone()),
                    value_from: None,
                });
            }
            env.extend([
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__MAX_FILE_SIZE.to_string(),
                    value: Some(max_file_size.to_string()),
                    value_from: None,
                },
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__COMPRESSION.to_string(),
                    value: Some(compression.to_string()),
                    value_from: None,
                },
            ]);
        }
        if let ShuffleBackendKind::Celeborn {
            compression,
            heartbeat_interval_secs,
            partition_split_threshold,
            partition_split_mode,
            ..
        } = &shuffle_backend
        {
            env.extend([
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS.to_string(),
                    value: Some(shuffle_backend.celeborn_master_endpoints_string()),
                    value_from: None,
                },
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__COMPRESSION.to_string(),
                    value: Some(compression.to_string()),
                    value_from: None,
                },
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__HEARTBEAT_INTERVAL_SECS
                        .to_string(),
                    value: Some(heartbeat_interval_secs.to_string()),
                    value_from: None,
                },
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES
                        .to_string(),
                    value: Some(shuffle_backend.celeborn_endpoint_overrides_string()),
                    value_from: None,
                },
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__PARTITION_SPLIT_THRESHOLD
                        .to_string(),
                    value: Some(partition_split_threshold.to_string()),
                    value_from: None,
                },
                EnvVar {
                    name: ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__PARTITION_SPLIT_MODE
                        .to_string(),
                    value: Some(partition_split_mode.to_string()),
                    value_from: None,
                },
            ]);
        }
        if let Some(traceparent) = w3c_traceparent {
            env.push(EnvVar {
                name: ContextPropagationEnv::TRACEPARENT.to_string(),
                value: Some(traceparent),
                value_from: None,
            });
        }
        env
    }
}

#[tonic::async_trait]
impl WorkerManager for KubernetesWorkerManager {
    fn launch_worker(
        &self,
        _system: &mut ActorSystem,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> BoxFuture<'static, ExecutionResult<()>> {
        let service = self.service.clone();
        Box::pin(async move { service.launch_worker(id, options).await })
    }

    async fn stop(&self) -> ExecutionResult<()> {
        self.service.stop().await
    }
}

impl KubernetesWorkerService {
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        let name = format!(
            "{}{}-{}",
            self.options.worker_pod_name_prefix, self.name, id
        );
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "worker".to_string(),
                command: Some(vec!["sail".to_string()]),
                args: Some(vec!["worker".to_string()]),
                env: Some(self.build_pod_env(id, options)),
                image: Some(self.options.image.clone()),
                image_pull_policy: Some(self.options.image_pull_policy.clone()),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            service_account_name: Some(self.options.worker_service_account_name.clone()),
            ..Default::default()
        };
        let mut labels = BTreeMap::new();
        let mut annotations = None;
        if !self.options.worker_pod_template.is_empty() {
            let template: PodTemplateSpec = serde_json::from_str(&self.options.worker_pod_template)
                .map_err(|e| {
                    ExecutionError::InternalError(format!(
                        "failed to parse worker pod template: {e}",
                    ))
                })?;
            if let Some(metadata) = &template.metadata {
                if let Some(template_labels) = &metadata.labels {
                    labels.extend(template_labels.clone());
                }
                annotations = metadata.annotations.clone();
            }
            if let Some(s) = template.spec {
                spec.merge_from(s);
            }
        }
        labels.extend(self.build_pod_labels(id));
        let p = Pod {
            metadata: ObjectMeta {
                name: Some(name),
                labels: Some(labels),
                annotations,
                owner_references: Some(self.get_owner_references().await?),
                ..Default::default()
            },
            spec: Some(spec),
            status: None,
        };
        let pp = Default::default();
        self.pods().await?.create(&pp, &p).await?;
        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        self.pods()
            .await?
            .delete_collection(
                &DeleteParams::default(),
                &ListParams::default()
                    .labels(&format!("sail.lakesail.com/worker-manager={}", self.name)),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[expect(clippy::unwrap_used)]
    async fn test_launch_worker_preserves_template_annotations() {
        use std::sync::Mutex;
        use std::time::Duration;

        use hyper::{Method, Request, Response};
        use kube::client::Body;
        use serde_json::json;

        for labels in [
            None,
            Some(json!({"custom": "value", "sail.lakesail.com/worker-manager": "other"})),
        ] {
            let created = Arc::new(Mutex::new(None::<Pod>));
            let capture = created.clone();
            let client = kube::Client::new(
                tower::service_fn(move |request: Request<Body>| {
                    let capture = capture.clone();
                    async move {
                        let response = if request.method() == Method::GET {
                            assert_eq!(
                                request.uri().path(),
                                "/api/v1/namespaces/workload-a/pods/driver"
                            );
                            json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "driver", "uid": "driver-uid"}})
                        } else {
                            assert_eq!(request.method(), Method::POST);
                            assert_eq!(request.uri().path(), "/api/v1/namespaces/workload-a/pods");
                            let bytes = request.into_body().collect_bytes().await.unwrap();
                            let pod: Pod = serde_json::from_slice(&bytes).unwrap();
                            *capture.lock().unwrap() = Some(pod.clone());
                            serde_json::to_value(pod).unwrap()
                        };
                        Ok::<_, std::convert::Infallible>(Response::new(Body::from(
                            response.to_string().into_bytes(),
                        )))
                    }
                }),
                "workload-a",
            );
            let annotations = BTreeMap::from([
                (
                    "karpenter.sh/do-not-disrupt".to_string(),
                    "true".to_string(),
                ),
                (
                    "customer.example/key".to_string(),
                    "arbitrary value".to_string(),
                ),
            ]);
            let template = json!({"metadata": {
                "labels": labels,
                "annotations": annotations,
                "name": "other-name",
                "namespace": "other-namespace",
                "uid": "other-uid",
                "ownerReferences": [{"apiVersion": "v1", "kind": "Pod", "name": "other", "uid": "other"}]
            }});
            let service = KubernetesWorkerService {
                name: "manager".to_string(),
                options: KubernetesWorkerManagerOptions {
                    image: "sail:test".to_string(),
                    image_pull_policy: "IfNotPresent".to_string(),
                    namespace: "workload-a".to_string(),
                    driver_pod_name: "driver".to_string(),
                    worker_pod_name_prefix: "worker-".to_string(),
                    worker_service_account_name: "worker".to_string(),
                    worker_pod_template: template.to_string(),
                },
                pods: OnceCell::new_with(Some(Api::namespaced(client, "workload-a"))),
            };
            let options = WorkerLaunchOptions {
                enable_tls: false,
                batch_size: 1024,
                session_id: "session".to_string(),
                driver_id: 1.into(),
                driver_external_host: "driver".to_string(),
                driver_external_port: 1234,
                worker_heartbeat_interval: Duration::from_secs(1),
                task_stream_buffer: 1,
                task_stream_creation_timeout: Duration::from_secs(1),
                rpc_retry_strategy: RetryStrategy::Fixed {
                    max_count: 1,
                    delay: Duration::from_secs(1),
                },
                shuffle_backend: ShuffleBackendKind::Storage {
                    path: None,
                    max_file_size: 1024,
                    compression: crate::shuffle::ShuffleCompression::None,
                },
            };
            service.launch_worker(1.into(), options).await.unwrap();
            let pod = created.lock().unwrap().take().unwrap();
            assert_eq!(pod.metadata.annotations, Some(annotations));
            assert_eq!(pod.metadata.name.as_deref(), Some("worker-manager-1"));
            assert_eq!(pod.metadata.namespace, None);
            assert_eq!(pod.metadata.uid, None);
            let labels = pod.metadata.labels.unwrap();
            assert_eq!(labels["sail.lakesail.com/worker-manager"], "manager");
            let owners = pod.metadata.owner_references.unwrap();
            assert_eq!(owners.len(), 1);
            assert_eq!(owners[0].uid, "driver-uid");
        }
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_label_merging_from_template() {
        // Test that labels from worker_pod_template are properly merged with default labels

        // Create a template with custom labels
        let mut template_labels = BTreeMap::new();
        template_labels.insert("custom-label".to_string(), "custom-value".to_string());
        template_labels.insert(
            "app.kubernetes.io/name".to_string(),
            "should-be-overridden".to_string(),
        );

        let template = PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(template_labels.clone()),
                ..Default::default()
            }),
            spec: None,
        };

        let template_json = serde_json::to_string(&template).unwrap();

        // Parse and merge labels (simulating the logic from launch_worker)
        let mut labels = BTreeMap::new();
        let parsed_template: PodTemplateSpec = serde_json::from_str(&template_json).unwrap();

        if let Some(metadata) = &parsed_template.metadata
            && let Some(template_labels) = &metadata.labels
        {
            labels.extend(template_labels.clone());
        }

        // Add default labels (simulating build_pod_labels)
        let default_labels = BTreeMap::from([
            ("app.kubernetes.io/name".to_string(), "sail".to_string()),
            (
                "app.kubernetes.io/component".to_string(),
                "worker".to_string(),
            ),
            (
                "app.kubernetes.io/instance".to_string(),
                "test-instance".to_string(),
            ),
            (
                "sail.lakesail.com/worker-manager".to_string(),
                "test-manager".to_string(),
            ),
        ]);
        labels.extend(default_labels.clone());

        // Verify custom labels are present
        assert_eq!(
            labels.get("custom-label"),
            Some(&"custom-value".to_string())
        );

        // Verify default labels override template labels
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"sail".to_string())
        );
        assert_ne!(
            labels.get("app.kubernetes.io/name"),
            Some(&"should-be-overridden".to_string())
        );

        // Verify all default labels are present
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"worker".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"test-instance".to_string())
        );
        assert_eq!(
            labels.get("sail.lakesail.com/worker-manager"),
            Some(&"test-manager".to_string())
        );
    }
}
