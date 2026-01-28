use std::collections::BTreeMap;
use std::env;

use fastrace::collector::SpanContext;
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, ObjectFieldSelector, Pod, PodSpec, PodTemplateSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::{DeepMerge, Resource};
use kube::Api;
use rand::distr::Uniform;
use rand::Rng;
use sail_common::config::ClusterConfigEnv;
use sail_server::RetryStrategy;
use sail_telemetry::common::ContextPropagationEnv;
use tokio::sync::OnceCell;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
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

pub struct KubernetesWorkerManager {
    /// An opaque name that can be used to create names to uniquely identify Kubernetes resources.
    name: String,
    options: KubernetesWorkerManagerOptions,
    pods: OnceCell<Api<Pod>>,
}

impl KubernetesWorkerManager {
    pub fn new(options: KubernetesWorkerManagerOptions) -> Self {
        Self {
            name: Self::generate_name(),
            options,
            pods: OnceCell::new(),
        }
    }

    pub fn generate_name() -> String {
        #[allow(clippy::unwrap_used)]
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
                format!("{}-{}", self.name, id),
            ),
        ])
    }

    fn build_pod_env(&self, id: WorkerId, options: WorkerLaunchOptions) -> Vec<EnvVar> {
        let WorkerLaunchOptions {
            enable_tls,
            driver_external_host,
            driver_external_port,
            worker_heartbeat_interval,
            worker_stream_buffer,
            task_stream_creation_timeout,
            rpc_retry_strategy,
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
                name: ClusterConfigEnv::WORKER_STREAM_BUFFER.to_string(),
                value: Some(worker_stream_buffer.to_string()),
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
        ];
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unwrap_used)]
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

        if let Some(metadata) = &parsed_template.metadata {
            if let Some(template_labels) = &metadata.labels {
                labels.extend(template_labels.clone());
            }
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
    }
}
