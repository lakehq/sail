use std::collections::HashMap;

use sail_python_udf::config::PySparkArtifactKind;

use crate::error::ExecutionError;
use crate::id::{JobId, TaskKey, WorkerId};
use crate::task::definition::{TaskDefinition, TaskLaunchContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TaskResourceStagingId(pub(crate) u64);

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct TaskResourceStagingKey {
    job_id: JobId,
    stage: usize,
    inline_total_max_bytes: usize,
    python_artifacts: Vec<PythonArtifactIdentity>,
    local_relations: Vec<LocalRelationIdentity>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct PythonArtifactIdentity {
    scope_id: String,
    name: String,
    python_path: String,
    uri: Option<String>,
    sha256: String,
    size: u64,
    kind: PySparkArtifactKind,
    has_data: bool,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct LocalRelationIdentity {
    key: String,
    uri: Option<String>,
    sha256: String,
    size: u64,
    has_data: bool,
}

impl TaskResourceStagingKey {
    pub(super) fn new(
        job_id: JobId,
        stage: usize,
        inline_total_max_bytes: usize,
        launch_context: &TaskLaunchContext,
    ) -> Self {
        let python_artifacts = launch_context
            .resources
            .python_artifacts
            .iter()
            .map(|artifact| PythonArtifactIdentity {
                scope_id: artifact.scope_id.clone(),
                name: artifact.name.clone(),
                python_path: artifact.python_path.clone(),
                uri: artifact.uri.clone(),
                sha256: artifact.sha256.clone(),
                size: artifact.size,
                kind: artifact.kind,
                has_data: artifact.data.is_some(),
            })
            .collect();
        let local_relations = launch_context
            .resources
            .local_relation_resources
            .iter()
            .map(|resource| LocalRelationIdentity {
                key: resource.key.clone(),
                uri: resource.uri.clone(),
                sha256: resource.sha256.clone(),
                size: resource.size,
                has_data: resource.data.is_some(),
            })
            .collect();
        Self {
            job_id,
            stage,
            inline_total_max_bytes,
            python_artifacts,
            local_relations,
        }
    }
}

pub(crate) struct PendingTaskResourceDispatch {
    pub(crate) worker_id: WorkerId,
    pub(crate) key: TaskKey,
    pub(crate) definition: TaskDefinition,
}

pub(crate) enum CompletedTaskResourceStaging {
    Ready {
        pending: Vec<PendingTaskResourceDispatch>,
        launch_context: TaskLaunchContext,
    },
    Failed {
        pending: Vec<PendingTaskResourceDispatch>,
        error: ExecutionError,
    },
}

pub(super) enum TaskResourceStagingRequest {
    Load,
    Wait,
    Reuse {
        launch_context: TaskLaunchContext,
        pending: Box<PendingTaskResourceDispatch>,
    },
}

pub(super) enum TaskResourceStagingCompletion {
    Pending(Vec<PendingTaskResourceDispatch>),
    Stale,
}

enum TaskResourceStagingEntry {
    Loading {
        staging_id: TaskResourceStagingId,
        pending: Vec<PendingTaskResourceDispatch>,
    },
    Ready(TaskLaunchContext),
}

#[derive(Default)]
pub(super) struct TaskResourceStagingCache {
    entries: HashMap<TaskResourceStagingKey, TaskResourceStagingEntry>,
    loading_keys: HashMap<TaskResourceStagingId, TaskResourceStagingKey>,
}

impl TaskResourceStagingCache {
    pub(super) fn request(
        &mut self,
        staging_id: TaskResourceStagingId,
        staging_key: TaskResourceStagingKey,
        pending: PendingTaskResourceDispatch,
    ) -> TaskResourceStagingRequest {
        match self.entries.get_mut(&staging_key) {
            Some(TaskResourceStagingEntry::Loading { pending: tasks, .. }) => {
                tasks.push(pending);
                TaskResourceStagingRequest::Wait
            }
            Some(TaskResourceStagingEntry::Ready(launch_context)) => {
                TaskResourceStagingRequest::Reuse {
                    launch_context: launch_context.clone(),
                    pending: Box::new(pending),
                }
            }
            None => {
                self.loading_keys.insert(staging_id, staging_key.clone());
                self.entries.insert(
                    staging_key,
                    TaskResourceStagingEntry::Loading {
                        staging_id,
                        pending: vec![pending],
                    },
                );
                TaskResourceStagingRequest::Load
            }
        }
    }

    pub(super) fn complete(
        &mut self,
        staging_id: TaskResourceStagingId,
        launch_context: TaskLaunchContext,
    ) -> TaskResourceStagingCompletion {
        let Some(staging_key) = self.loading_keys.remove(&staging_id) else {
            return TaskResourceStagingCompletion::Stale;
        };
        let Some(TaskResourceStagingEntry::Loading {
            staging_id: active_id,
            pending,
        }) = self.entries.remove(&staging_key)
        else {
            return TaskResourceStagingCompletion::Stale;
        };
        if active_id != staging_id {
            self.entries.insert(
                staging_key.clone(),
                TaskResourceStagingEntry::Loading {
                    staging_id: active_id,
                    pending,
                },
            );
            self.loading_keys.insert(active_id, staging_key);
            return TaskResourceStagingCompletion::Stale;
        }
        self.entries
            .insert(staging_key, TaskResourceStagingEntry::Ready(launch_context));
        TaskResourceStagingCompletion::Pending(pending)
    }

    pub(super) fn fail(
        &mut self,
        staging_id: TaskResourceStagingId,
    ) -> TaskResourceStagingCompletion {
        let Some(staging_key) = self.loading_keys.remove(&staging_id) else {
            return TaskResourceStagingCompletion::Stale;
        };
        let Some(TaskResourceStagingEntry::Loading {
            staging_id: active_id,
            pending,
        }) = self.entries.remove(&staging_key)
        else {
            return TaskResourceStagingCompletion::Stale;
        };
        if active_id != staging_id {
            self.entries.insert(
                staging_key.clone(),
                TaskResourceStagingEntry::Loading {
                    staging_id: active_id,
                    pending,
                },
            );
            self.loading_keys.insert(active_id, staging_key);
            return TaskResourceStagingCompletion::Stale;
        }
        TaskResourceStagingCompletion::Pending(pending)
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.loading_keys.clear();
    }
}

#[cfg(test)]
#[expect(clippy::panic)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::task::definition::{LocalRelationResource, TaskOutput, TaskResources};

    fn launch_context() -> TaskLaunchContext {
        TaskLaunchContext {
            resources: TaskResources {
                local_relation_resources: vec![LocalRelationResource {
                    key: "a".repeat(64),
                    data: Some(Arc::from([1_u8, 2, 3])),
                    uri: None,
                    sha256: "a".repeat(64),
                    size: 3,
                }],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn pending(job_id: u64, partition: usize) -> PendingTaskResourceDispatch {
        PendingTaskResourceDispatch {
            worker_id: 1_u64.into(),
            key: TaskKey {
                job_id: job_id.into(),
                stage: 2,
                partition,
                attempt: 0,
            },
            definition: TaskDefinition {
                plan: Arc::from([]),
                inputs: vec![],
                output: TaskOutput {
                    distribution: crate::task::definition::TaskOutputDistribution::RoundRobin {
                        channels: 1,
                    },
                    locator: crate::task::definition::TaskOutputLocator::Local { replicas: 1 },
                },
            },
        }
    }

    #[test]
    fn concurrent_tasks_start_one_resource_loader() {
        let mut cache = TaskResourceStagingCache::default();
        let context = launch_context();
        let key = TaskResourceStagingKey::new(1_u64.into(), 2, 1024, &context);
        let mut loads = 0;

        for partition in 0..32 {
            let request = cache.request(
                TaskResourceStagingId(partition as u64 + 1),
                key.clone(),
                pending(1, partition),
            );
            if matches!(request, TaskResourceStagingRequest::Load) {
                loads += 1;
            }
        }

        assert_eq!(loads, 1);
        let TaskResourceStagingCompletion::Pending(pending_tasks) =
            cache.complete(TaskResourceStagingId(1), context)
        else {
            panic!("the active loader must complete");
        };
        assert_eq!(pending_tasks.len(), 32);
        let TaskResourceStagingRequest::Reuse {
            launch_context,
            pending,
        } = cache.request(TaskResourceStagingId(100), key, pending(1, 100))
        else {
            panic!("a completed resource set must be reused");
        };
        assert_eq!(pending.key.partition, 100);
        assert_eq!(
            launch_context.resources.local_relation_resources[0]
                .data
                .as_deref(),
            Some(&[1_u8, 2, 3][..])
        );
    }

    #[test]
    fn failed_loader_allows_a_new_unique_attempt() {
        let mut cache = TaskResourceStagingCache::default();
        let context = launch_context();
        let key = TaskResourceStagingKey::new(1_u64.into(), 2, 1024, &context);

        assert!(matches!(
            cache.request(TaskResourceStagingId(7), key.clone(), pending(1, 0)),
            TaskResourceStagingRequest::Load
        ));
        assert!(matches!(
            cache.fail(TaskResourceStagingId(7)),
            TaskResourceStagingCompletion::Pending(_)
        ));
        assert!(matches!(
            cache.request(TaskResourceStagingId(8), key, pending(1, 0)),
            TaskResourceStagingRequest::Load
        ));
    }

    #[test]
    fn cleanup_discards_ready_and_loading_state() {
        let mut cache = TaskResourceStagingCache::default();
        let context = launch_context();
        let ready_key = TaskResourceStagingKey::new(1_u64.into(), 2, 1024, &context);
        assert!(matches!(
            cache.request(TaskResourceStagingId(1), ready_key.clone(), pending(1, 0)),
            TaskResourceStagingRequest::Load
        ));
        assert!(matches!(
            cache.complete(TaskResourceStagingId(1), context.clone()),
            TaskResourceStagingCompletion::Pending(_)
        ));
        let loading_key = TaskResourceStagingKey::new(1_u64.into(), 2, 512, &context);
        assert!(matches!(
            cache.request(TaskResourceStagingId(2), loading_key, pending(1, 1)),
            TaskResourceStagingRequest::Load
        ));

        cache.clear();

        assert!(matches!(
            cache.complete(TaskResourceStagingId(2), context),
            TaskResourceStagingCompletion::Stale
        ));
        assert!(matches!(
            cache.request(TaskResourceStagingId(3), ready_key, pending(1, 2)),
            TaskResourceStagingRequest::Load
        ));
    }

    #[test]
    fn identical_resources_from_different_jobs_do_not_share_staging() {
        let mut cache = TaskResourceStagingCache::default();
        let context = launch_context();
        let first_job_key = TaskResourceStagingKey::new(1_u64.into(), 2, 1024, &context);
        let second_job_key = TaskResourceStagingKey::new(2_u64.into(), 2, 1024, &context);

        assert!(matches!(
            cache.request(TaskResourceStagingId(1), first_job_key, pending(1, 0)),
            TaskResourceStagingRequest::Load
        ));
        assert!(matches!(
            cache.request(TaskResourceStagingId(2), second_job_key, pending(2, 0)),
            TaskResourceStagingRequest::Load
        ));
    }
}
