use std::collections::BTreeMap;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};

use fastrace::collector::SpanContext;
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, ObjectFieldSelector, Pod, PodSpec, PodTemplateSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::{DeepMerge, Resource};
use kube::Api;
use kube::api::{DeleteParams, ListParams};
use rand::RngExt;
use rand::distr::Uniform;
use sail_common::config::ClusterConfigEnv;
use sail_server::RetryStrategy;
use sail_telemetry::common::ContextPropagationEnv;
use tokio::sync::{OnceCell, RwLock, RwLockReadGuard, RwLockWriteGuard};

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
    pub artifact_transfer_timeout: std::time::Duration,
}

pub struct KubernetesWorkerManager {
    /// An opaque name that can be used to create names to uniquely identify Kubernetes resources.
    name: String,
    options: KubernetesWorkerManagerOptions,
    pods: OnceCell<Api<Pod>>,
    lifecycle: KubernetesWorkerLifecycle,
}

#[derive(Default)]
struct KubernetesWorkerLifecycle {
    stopping: AtomicBool,
    launches: RwLock<()>,
}

impl KubernetesWorkerLifecycle {
    async fn enter_launch(&self) -> ExecutionResult<RwLockReadGuard<'_, ()>> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ExecutionError::InvalidArgument(
                "Kubernetes worker manager is stopping".to_string(),
            ));
        }
        let launch = self.launches.read().await;
        if self.stopping.load(Ordering::Acquire) {
            return Err(ExecutionError::InvalidArgument(
                "Kubernetes worker manager is stopping".to_string(),
            ));
        }
        Ok(launch)
    }

    fn stop_new_launches(&self) {
        self.stopping.store(true, Ordering::Release);
    }

    async fn wait_for_launches(&self) -> RwLockWriteGuard<'_, ()> {
        self.launches.write().await
    }
}

async fn run_kubernetes_stop_barrier<T>(
    lifecycle: &KubernetesWorkerLifecycle,
    launch_drain_timeout: std::time::Duration,
    cleanup_timeout: std::time::Duration,
    cleanup: impl std::future::Future<Output = ExecutionResult<T>>,
) -> ExecutionResult<T> {
    let _launches = tokio::time::timeout(launch_drain_timeout, lifecycle.wait_for_launches())
        .await
        .map_err(|_| {
            ExecutionError::InternalError(
                "timed out waiting for Kubernetes worker launches".to_string(),
            )
        })?;
    tokio::time::timeout(cleanup_timeout, cleanup)
        .await
        .unwrap_or_else(|_| {
            Err(ExecutionError::InternalError(
                "timed out deleting Kubernetes workers".to_string(),
            ))
        })
}

async fn sweep_kubernetes_worker_pods<
    Delete,
    DeleteFuture,
    Remaining,
    RemainingFuture,
    Now,
    Wait,
    WaitFuture,
>(
    late_create_window: std::time::Duration,
    poll_interval: std::time::Duration,
    mut delete_workers: Delete,
    mut workers_remaining: Remaining,
    now: Now,
    mut wait: Wait,
) -> ExecutionResult<()>
where
    Delete: FnMut() -> DeleteFuture,
    DeleteFuture: std::future::Future<Output = ExecutionResult<()>>,
    Remaining: FnMut() -> RemainingFuture,
    RemainingFuture: std::future::Future<Output = ExecutionResult<bool>>,
    Now: Fn() -> tokio::time::Instant,
    Wait: FnMut(std::time::Duration) -> WaitFuture,
    WaitFuture: std::future::Future<Output = ()>,
{
    let late_create_deadline = now() + late_create_window;
    loop {
        delete_workers().await?;
        let remaining = workers_remaining().await?;
        let current = now();
        if current >= late_create_deadline && !remaining {
            return Ok(());
        }
        let delay = if current < late_create_deadline {
            poll_interval.min(late_create_deadline.saturating_duration_since(current))
        } else {
            poll_interval
        };
        wait(delay).await;
    }
}

impl KubernetesWorkerManager {
    pub fn new(options: KubernetesWorkerManagerOptions) -> Self {
        Self {
            name: Self::generate_name(),
            options,
            pods: OnceCell::new(),
            lifecycle: KubernetesWorkerLifecycle::default(),
        }
    }

    pub fn generate_name() -> String {
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
                format!("{}-{}", self.name, id),
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
            driver_external_host,
            driver_external_port,
            worker_heartbeat_interval,
            task_stream_buffer,
            task_stream_creation_timeout,
            artifact_transfer_timeout,
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
                name: "SAIL_SPARK__ARTIFACT_TRANSFER_TIMEOUT_SECS".to_string(),
                value: Some(artifact_transfer_timeout.as_secs().to_string()),
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
        let _launch = self.lifecycle.enter_launch().await?;
        let deadline = tokio::time::Instant::now() + self.options.artifact_transfer_timeout;
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
            if let Some(metadata) = &template.metadata
                && let Some(template_labels) = &metadata.labels
            {
                labels.extend(template_labels.clone());
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
                owner_references: Some(
                    tokio::time::timeout_at(deadline, self.get_owner_references())
                        .await
                        .map_err(|_| {
                            ExecutionError::InternalError(
                                "timed out resolving Kubernetes worker ownership".to_string(),
                            )
                        })??,
                ),
                ..Default::default()
            },
            spec: Some(spec),
            status: None,
        };
        let pp = Default::default();
        let pods = tokio::time::timeout_at(deadline, self.pods())
            .await
            .map_err(|_| {
                ExecutionError::InternalError(
                    "timed out creating Kubernetes worker client".to_string(),
                )
            })??;
        tokio::time::timeout_at(deadline, pods.create(&pp, &p))
            .await
            .map_err(|_| {
                ExecutionError::InternalError("timed out creating Kubernetes worker".to_string())
            })??;
        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        self.lifecycle.stop_new_launches();
        let transfer_timeout = self.options.artifact_transfer_timeout;
        let stop_margin = transfer_timeout.min(std::time::Duration::from_millis(250));
        let launch_drain_timeout = transfer_timeout.saturating_add(stop_margin);
        let cleanup_timeout = transfer_timeout
            .saturating_mul(2)
            .saturating_add(stop_margin);
        run_kubernetes_stop_barrier(
            &self.lifecycle,
            launch_drain_timeout,
            cleanup_timeout,
            async {
                let pods = self.pods().await?.clone();
                let selector = format!("sail.lakesail.com/worker-manager={}", self.name);
                let delete_pods = pods.clone();
                let delete_selector = selector.clone();
                sweep_kubernetes_worker_pods(
                    transfer_timeout,
                    std::time::Duration::from_millis(100),
                    move || {
                        let pods = delete_pods.clone();
                        let selector = delete_selector.clone();
                        async move {
                            pods.delete_collection(
                                &DeleteParams::default(),
                                &ListParams::default().labels(&selector),
                            )
                            .await?;
                            Ok(())
                        }
                    },
                    move || {
                        let pods = pods.clone();
                        let selector = selector.clone();
                        async move {
                            Ok(!pods
                                .list(&ListParams::default().labels(&selector))
                                .await?
                                .items
                                .is_empty())
                        }
                    },
                    tokio::time::Instant::now,
                    tokio::time::sleep,
                )
                .await
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn stop_gate_waits_for_launches_and_rejects_new_launches() {
        let lifecycle = std::sync::Arc::new(KubernetesWorkerLifecycle::default());
        let launch = lifecycle.enter_launch().await;
        assert!(launch.is_ok());
        let Some(launch) = launch.ok() else {
            return;
        };
        lifecycle.stop_new_launches();
        assert!(lifecycle.enter_launch().await.is_err());

        let waiting = std::sync::Arc::clone(&lifecycle);
        let cleanup_called = std::sync::Arc::new(AtomicBool::new(false));
        let called = std::sync::Arc::clone(&cleanup_called);
        let mut stop = tokio::spawn(async move {
            run_kubernetes_stop_barrier(
                &waiting,
                std::time::Duration::from_millis(280),
                std::time::Duration::from_millis(30),
                async move {
                    called.store(true, Ordering::Release);
                    Ok(())
                },
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut stop)
                .await
                .is_err()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(launch);
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(1), stop).await;
        assert!(matches!(stopped, Ok(Ok(Ok(())))));
        assert!(cleanup_called.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn selector_sweep_deletes_worker_created_at_launch_timeout_boundary() {
        let base = tokio::time::Instant::now();
        let elapsed_millis = Arc::new(AtomicU64::new(0));
        let worker_present = Arc::new(AtomicBool::new(false));
        let late_worker_created = Arc::new(AtomicBool::new(false));
        let late_worker_deleted = Arc::new(AtomicBool::new(false));
        let delete_count = Arc::new(AtomicUsize::new(0));

        let deleted_present = Arc::clone(&worker_present);
        let deleted_late = Arc::clone(&late_worker_created);
        let observed_late_delete = Arc::clone(&late_worker_deleted);
        let deletes = Arc::clone(&delete_count);
        let listed_present = Arc::clone(&worker_present);
        let clock = Arc::clone(&elapsed_millis);
        let waited_clock = Arc::clone(&elapsed_millis);
        let created_present = Arc::clone(&worker_present);
        let created_late = Arc::clone(&late_worker_created);

        let result = sweep_kubernetes_worker_pods(
            std::time::Duration::from_millis(20),
            std::time::Duration::from_millis(5),
            move || {
                deletes.fetch_add(1, Ordering::AcqRel);
                if deleted_present.swap(false, Ordering::AcqRel)
                    && deleted_late.load(Ordering::Acquire)
                {
                    observed_late_delete.store(true, Ordering::Release);
                }
                std::future::ready(Ok(()))
            },
            move || std::future::ready(Ok(listed_present.load(Ordering::Acquire))),
            move || base + std::time::Duration::from_millis(clock.load(Ordering::Acquire)),
            move |delay| {
                let millis = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
                let current = waited_clock
                    .fetch_add(millis, Ordering::AcqRel)
                    .saturating_add(millis);
                if current >= 20 && !created_late.swap(true, Ordering::AcqRel) {
                    created_present.store(true, Ordering::Release);
                }
                std::future::ready(())
            },
        )
        .await;

        assert!(result.is_ok());
        assert!(late_worker_deleted.load(Ordering::Acquire));
        assert!(!worker_present.load(Ordering::Acquire));
        assert!(delete_count.load(Ordering::Acquire) >= 5);
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
