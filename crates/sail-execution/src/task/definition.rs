use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_common_datafusion::session::artifact::{
    ArtifactManifest, RuntimeArtifact, RuntimeArtifactKind,
};

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::proto::decode_remote_physical_expr;
use crate::stream::reader::TaskReadLocation;
use crate::stream::writer::{LocalStreamStorage, TaskWriteLocation};
use crate::task::r#gen;

#[derive(Debug, Clone)]
pub struct TaskDefinition {
    pub plan: Arc<[u8]>,
    pub inputs: Vec<TaskInput>,
    pub output: TaskOutput,
    pub artifact_manifest: ArtifactManifest,
}

#[derive(Debug, Clone)]
pub struct TaskInput {
    pub locator: TaskInputLocator,
}

#[derive(Debug, Clone)]
pub enum TaskInputLocator {
    Driver {
        stage: usize,
        keys: Vec<Vec<TaskInputKey>>,
    },
    Worker {
        stage: usize,
        keys: Vec<Vec<(WorkerId, TaskInputKey)>>,
    },
    Remote {
        uri: String,
        stage: usize,
        keys: Vec<Vec<TaskInputKey>>,
    },
}

#[derive(Debug, Clone)]
pub struct TaskInputKey {
    pub partition: usize,
    pub attempt: usize,
    pub channel: usize,
}

#[derive(Debug, Clone)]
pub struct TaskOutput {
    pub distribution: TaskOutputDistribution,
    pub locator: TaskOutputLocator,
}

#[derive(Debug, Clone)]
pub enum TaskOutputDistribution {
    Hash {
        keys: Vec<Arc<[u8]>>,
        channels: usize,
    },
    RoundRobin {
        channels: usize,
    },
    RoundRobinRow {
        channels: usize,
    },
}

#[derive(Debug, Clone)]
pub enum TaskOutputLocator {
    Local { replicas: usize },
    Remote { uri: String },
}

impl From<TaskDefinition> for r#gen::TaskDefinition {
    fn from(value: TaskDefinition) -> Self {
        let TaskDefinition {
            plan,
            inputs,
            output,
            artifact_manifest,
        } = value;
        r#gen::TaskDefinition {
            plan: plan.to_vec(),
            inputs: inputs.into_iter().map(|x| x.into()).collect(),
            output: Some(output.into()),
            artifact_manifest: artifact_manifest
                .is_present()
                .then(|| artifact_manifest.into()),
        }
    }
}

impl TryFrom<r#gen::TaskDefinition> for TaskDefinition {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskDefinition) -> Result<Self, Self::Error> {
        let inputs = value
            .inputs
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()?;
        let output = match value.output {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output".to_string(),
                ));
            }
        };
        let artifact_manifest = value
            .artifact_manifest
            .map(ArtifactManifest::try_from)
            .transpose()?
            .unwrap_or_default();
        Ok(TaskDefinition {
            plan: Arc::from(value.plan),
            inputs,
            output,
            artifact_manifest,
        })
    }
}

impl From<ArtifactManifest> for r#gen::ArtifactManifest {
    fn from(value: ArtifactManifest) -> Self {
        let ArtifactManifest {
            set_id,
            fingerprint,
            artifacts,
        } = value;
        Self {
            set_id,
            fingerprint: fingerprint.to_vec(),
            artifacts: artifacts.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<r#gen::ArtifactManifest> for ArtifactManifest {
    type Error = ExecutionError;

    fn try_from(value: r#gen::ArtifactManifest) -> Result<Self, Self::Error> {
        if value.set_id.is_empty() {
            return Err(ExecutionError::InvalidArgument(
                "artifact manifest set ID cannot be empty".to_string(),
            ));
        }
        let fingerprint = decode_sha256(value.fingerprint, "artifact manifest fingerprint")?;
        let artifacts = value
            .artifacts
            .into_iter()
            .map(RuntimeArtifact::try_from)
            .collect::<ExecutionResult<Vec<_>>>()?;
        Ok(Self {
            set_id: value.set_id,
            fingerprint,
            artifacts,
        })
    }
}

impl From<RuntimeArtifact> for r#gen::RuntimeArtifact {
    fn from(value: RuntimeArtifact) -> Self {
        let RuntimeArtifact {
            name,
            kind,
            archive_name,
            digest,
            data,
        } = value;
        let kind = match kind {
            RuntimeArtifactKind::PythonFile => r#gen::RuntimeArtifactKind::PythonFile,
            RuntimeArtifactKind::File => r#gen::RuntimeArtifactKind::File,
            RuntimeArtifactKind::Archive => r#gen::RuntimeArtifactKind::Archive,
        };
        Self {
            name,
            kind: kind.into(),
            archive_name,
            digest: digest.to_vec(),
            data: data.to_vec(),
        }
    }
}

impl TryFrom<r#gen::RuntimeArtifact> for RuntimeArtifact {
    type Error = ExecutionError;

    fn try_from(value: r#gen::RuntimeArtifact) -> Result<Self, Self::Error> {
        let kind = match r#gen::RuntimeArtifactKind::try_from(value.kind)? {
            r#gen::RuntimeArtifactKind::PythonFile => RuntimeArtifactKind::PythonFile,
            r#gen::RuntimeArtifactKind::File => RuntimeArtifactKind::File,
            r#gen::RuntimeArtifactKind::Archive => RuntimeArtifactKind::Archive,
            r#gen::RuntimeArtifactKind::Unspecified => {
                return Err(ExecutionError::InvalidArgument(
                    "runtime artifact kind is unspecified".to_string(),
                ));
            }
        };
        Ok(Self {
            name: value.name,
            kind,
            archive_name: value.archive_name,
            digest: decode_sha256(value.digest, "runtime artifact digest")?,
            data: value.data.into(),
        })
    }
}

fn decode_sha256(value: Vec<u8>, description: &str) -> ExecutionResult<[u8; 32]> {
    value.try_into().map_err(|value: Vec<u8>| {
        ExecutionError::InvalidArgument(format!(
            "{description} must contain 32 bytes, got {}",
            value.len()
        ))
    })
}

impl From<TaskInput> for r#gen::TaskInput {
    fn from(value: TaskInput) -> Self {
        let TaskInput { locator } = value;
        r#gen::TaskInput {
            locator: Some(locator.into()),
        }
    }
}

impl TryFrom<r#gen::TaskInput> for TaskInput {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInput) -> Result<Self, Self::Error> {
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task input locator".to_string(),
                ));
            }
        };
        Ok(TaskInput { locator })
    }
}

impl From<TaskInputLocator> for r#gen::TaskInputLocator {
    fn from(value: TaskInputLocator) -> Self {
        let kind = match value {
            TaskInputLocator::Driver { stage, keys } => {
                r#gen::task_input_locator::Kind::Driver(r#gen::TaskInputDriverLocator {
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Worker { stage, keys } => {
                r#gen::task_input_locator::Kind::Worker(r#gen::TaskInputWorkerLocator {
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Remote { uri, stage, keys } => {
                r#gen::task_input_locator::Kind::Remote(r#gen::TaskInputRemoteLocator {
                    uri,
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
        };
        r#gen::TaskInputLocator { kind: Some(kind) }
    }
}

impl TryFrom<r#gen::TaskInputLocator> for TaskInputLocator {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(r#gen::task_input_locator::Kind::Driver(r#gen::TaskInputDriverLocator {
                stage,
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Driver {
                    stage: stage as usize,
                    keys,
                })
            }
            Some(r#gen::task_input_locator::Kind::Worker(r#gen::TaskInputWorkerLocator {
                stage,
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Worker {
                    stage: stage as usize,
                    keys,
                })
            }
            Some(r#gen::task_input_locator::Kind::Remote(r#gen::TaskInputRemoteLocator {
                uri,
                stage,
                keys,
            })) => {
                let keys = keys
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(TaskInputLocator::Remote {
                    uri,
                    stage: stage as usize,
                    keys,
                })
            }
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task input locator".to_string(),
            )),
        }
    }
}

impl From<TaskInputKey> for r#gen::TaskInputDriverKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        r#gen::TaskInputDriverKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<r#gen::TaskInputDriverKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputDriverKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for r#gen::TaskInputDriverKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        r#gen::TaskInputDriverKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<r#gen::TaskInputDriverKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputDriverKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<(WorkerId, TaskInputKey)> for r#gen::TaskInputWorkerKey {
    fn from(value: (WorkerId, TaskInputKey)) -> Self {
        let (
            worker_id,
            TaskInputKey {
                partition,
                attempt,
                channel,
            },
        ) = value;
        r#gen::TaskInputWorkerKey {
            worker_id: worker_id.into(),
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<r#gen::TaskInputWorkerKey> for (WorkerId, TaskInputKey) {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputWorkerKey) -> Result<Self, Self::Error> {
        Ok((
            value.worker_id.into(),
            TaskInputKey {
                partition: value.partition as usize,
                attempt: value.attempt as usize,
                channel: value.channel as usize,
            },
        ))
    }
}

impl From<Vec<(WorkerId, TaskInputKey)>> for r#gen::TaskInputWorkerKeyList {
    fn from(value: Vec<(WorkerId, TaskInputKey)>) -> Self {
        r#gen::TaskInputWorkerKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<r#gen::TaskInputWorkerKeyList> for Vec<(WorkerId, TaskInputKey)> {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputWorkerKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskInputKey> for r#gen::TaskInputRemoteKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        r#gen::TaskInputRemoteKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<r#gen::TaskInputRemoteKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputRemoteKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for r#gen::TaskInputRemoteKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        r#gen::TaskInputRemoteKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<r#gen::TaskInputRemoteKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskInputRemoteKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskOutput> for r#gen::TaskOutput {
    fn from(value: TaskOutput) -> Self {
        let TaskOutput {
            distribution,
            locator,
        } = value;
        r#gen::TaskOutput {
            distribution: Some(distribution.into()),
            locator: Some(locator.into()),
        }
    }
}

impl TryFrom<r#gen::TaskOutput> for TaskOutput {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskOutput) -> Result<Self, Self::Error> {
        let distribution = match value.distribution {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output distribution".to_string(),
                ));
            }
        };
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output locator".to_string(),
                ));
            }
        };
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }
}

impl From<TaskOutputDistribution> for r#gen::TaskOutputDistribution {
    fn from(value: TaskOutputDistribution) -> Self {
        let kind = match value {
            TaskOutputDistribution::Hash { keys, channels } => {
                r#gen::task_output_distribution::Kind::Hash(r#gen::TaskOutputHashDistribution {
                    keys: keys.into_iter().map(|k| k.to_vec()).collect(),
                    channels: channels as u64,
                })
            }
            TaskOutputDistribution::RoundRobin { channels } => {
                r#gen::task_output_distribution::Kind::RoundRobin(
                    r#gen::TaskOutputRoundRobinDistribution {
                        channels: channels as u64,
                    },
                )
            }
            TaskOutputDistribution::RoundRobinRow { channels } => {
                r#gen::task_output_distribution::Kind::RoundRobinRow(
                    r#gen::TaskOutputRoundRobinRowDistribution {
                        channels: channels as u64,
                    },
                )
            }
        };
        r#gen::TaskOutputDistribution { kind: Some(kind) }
    }
}

impl TryFrom<r#gen::TaskOutputDistribution> for TaskOutputDistribution {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskOutputDistribution) -> Result<Self, Self::Error> {
        match value.kind {
            Some(r#gen::task_output_distribution::Kind::Hash(
                r#gen::TaskOutputHashDistribution { keys, channels },
            )) => Ok(TaskOutputDistribution::Hash {
                keys: keys.into_iter().map(Arc::from).collect(),
                channels: channels as usize,
            }),
            Some(r#gen::task_output_distribution::Kind::RoundRobin(
                r#gen::TaskOutputRoundRobinDistribution { channels },
            )) => Ok(TaskOutputDistribution::RoundRobin {
                channels: channels as usize,
            }),
            Some(r#gen::task_output_distribution::Kind::RoundRobinRow(
                r#gen::TaskOutputRoundRobinRowDistribution { channels },
            )) => Ok(TaskOutputDistribution::RoundRobinRow {
                channels: channels as usize,
            }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task output distribution".to_string(),
            )),
        }
    }
}

impl From<TaskOutputLocator> for r#gen::TaskOutputLocator {
    fn from(value: TaskOutputLocator) -> Self {
        let kind = match value {
            TaskOutputLocator::Local { replicas } => {
                r#gen::task_output_locator::Kind::Local(r#gen::TaskOutputLocalLocator {
                    replicas: replicas as u64,
                })
            }
            TaskOutputLocator::Remote { uri } => {
                r#gen::task_output_locator::Kind::Remote(r#gen::TaskOutputRemoteLocator { uri })
            }
        };
        r#gen::TaskOutputLocator { kind: Some(kind) }
    }
}

impl TryFrom<r#gen::TaskOutputLocator> for TaskOutputLocator {
    type Error = ExecutionError;

    fn try_from(value: r#gen::TaskOutputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(r#gen::task_output_locator::Kind::Local(r#gen::TaskOutputLocalLocator {
                replicas,
            })) => Ok(TaskOutputLocator::Local {
                replicas: replicas as usize,
            }),
            Some(r#gen::task_output_locator::Kind::Remote(r#gen::TaskOutputRemoteLocator {
                uri,
            })) => Ok(TaskOutputLocator::Remote { uri }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task output locator".to_string(),
            )),
        }
    }
}

impl TaskInput {
    pub fn locations(&self, job_id: JobId) -> Vec<Vec<TaskReadLocation>> {
        match &self.locator {
            TaskInputLocator::Driver { stage, keys } => keys
                .iter()
                .map(|keys| {
                    keys.iter()
                        .map(|key| TaskReadLocation::Driver {
                            key: TaskStreamKey {
                                job_id,
                                stage: *stage,
                                partition: key.partition,
                                attempt: key.attempt,
                                channel: key.channel,
                            },
                        })
                        .collect()
                })
                .collect(),
            TaskInputLocator::Worker { stage, keys } => keys
                .iter()
                .map(|keys| {
                    keys.iter()
                        .map(|(worker_id, key)| TaskReadLocation::Worker {
                            worker_id: *worker_id,
                            key: TaskStreamKey {
                                job_id,
                                stage: *stage,
                                partition: key.partition,
                                attempt: key.attempt,
                                channel: key.channel,
                            },
                        })
                        .collect()
                })
                .collect(),
            TaskInputLocator::Remote { uri, stage, keys } => keys
                .iter()
                .map(|keys| {
                    keys.iter()
                        .map(|key| TaskReadLocation::Remote {
                            uri: uri.clone(),
                            key: TaskStreamKey {
                                job_id,
                                stage: *stage,
                                partition: key.partition,
                                attempt: key.attempt,
                                channel: key.channel,
                            },
                        })
                        .collect()
                })
                .collect(),
        }
    }
}

impl TaskOutput {
    pub fn channels(&self) -> usize {
        match self.distribution {
            TaskOutputDistribution::Hash { channels, .. } => channels,
            TaskOutputDistribution::RoundRobin { channels, .. } => channels,
            TaskOutputDistribution::RoundRobinRow { channels, .. } => channels,
        }
    }

    pub fn locations(&self, key: &TaskKey) -> Vec<TaskWriteLocation> {
        let channels = self.channels();
        match &self.locator {
            TaskOutputLocator::Local { replicas } => (0..channels)
                .map(|channel| TaskWriteLocation::Local {
                    storage: LocalStreamStorage::Memory {
                        replicas: *replicas,
                    },
                    key: TaskStreamKey {
                        job_id: key.job_id,
                        stage: key.stage,
                        partition: key.partition,
                        attempt: key.attempt,
                        channel,
                    },
                })
                .collect(),
            TaskOutputLocator::Remote { uri } => (0..channels)
                .map(|channel| TaskWriteLocation::Remote {
                    uri: uri.clone(),
                    key: TaskStreamKey {
                        job_id: key.job_id,
                        stage: key.stage,
                        partition: key.partition,
                        attempt: key.attempt,
                        channel,
                    },
                })
                .collect(),
        }
    }

    pub fn partitioning(
        &self,
        ctx: &TaskContext,
        schema: &Schema,
        codec: &dyn PhysicalExtensionCodec,
    ) -> ExecutionResult<Partitioning> {
        match &self.distribution {
            TaskOutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|k| {
                        decode_remote_physical_expr(ctx, codec, k.as_ref(), schema)
                            .map_err(|e| e.into())
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(Partitioning::Hash(keys, *channels))
            }
            TaskOutputDistribution::RoundRobin { channels }
            | TaskOutputDistribution::RoundRobinRow { channels } => {
                Ok(Partitioning::RoundRobinBatch(*channels))
            }
        }
    }

    pub fn row_based(&self) -> bool {
        matches!(
            &self.distribution,
            TaskOutputDistribution::RoundRobinRow { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_manifest() -> ArtifactManifest {
        ArtifactManifest {
            set_id: "session-1".to_string(),
            fingerprint: [7; 32],
            artifacts: vec![RuntimeArtifact {
                name: "files/value.txt".to_string(),
                kind: RuntimeArtifactKind::File,
                archive_name: None,
                digest: [9; 32],
                data: Arc::from(b"value".as_slice()),
            }],
        }
    }

    fn task_definition(artifact_manifest: ArtifactManifest) -> TaskDefinition {
        TaskDefinition {
            plan: Arc::from(b"plan".as_slice()),
            inputs: vec![],
            output: TaskOutput {
                distribution: TaskOutputDistribution::RoundRobin { channels: 1 },
                locator: TaskOutputLocator::Local { replicas: 1 },
            },
            artifact_manifest,
        }
    }

    #[test]
    fn task_definition_round_trips_artifact_manifest() -> ExecutionResult<()> {
        let expected = artifact_manifest();
        let encoded = r#gen::TaskDefinition::from(task_definition(expected.clone()));
        assert!(encoded.artifact_manifest.is_some());

        let decoded = TaskDefinition::try_from(encoded)?;
        assert_eq!(decoded.artifact_manifest, expected);
        Ok(())
    }

    #[test]
    fn task_definition_omits_absent_artifact_manifest() {
        let encoded = r#gen::TaskDefinition::from(task_definition(ArtifactManifest::default()));
        assert!(encoded.artifact_manifest.is_none());

        let session_manifest = ArtifactManifest {
            set_id: "session-1".to_string(),
            fingerprint: [3; 32],
            artifacts: vec![],
        };
        let encoded = r#gen::TaskDefinition::from(task_definition(session_manifest));
        assert!(encoded.artifact_manifest.is_some());
    }

    #[test]
    fn rejects_invalid_artifact_hash_lengths_and_kind() {
        let invalid_fingerprint = r#gen::ArtifactManifest {
            set_id: "session-1".to_string(),
            fingerprint: vec![0; 31],
            artifacts: vec![],
        };
        assert!(matches!(
            ArtifactManifest::try_from(invalid_fingerprint),
            Err(ExecutionError::InvalidArgument(_))
        ));

        let missing_set_id = r#gen::ArtifactManifest {
            set_id: String::new(),
            fingerprint: vec![0; 32],
            artifacts: vec![],
        };
        assert!(matches!(
            ArtifactManifest::try_from(missing_set_id),
            Err(ExecutionError::InvalidArgument(_))
        ));

        let invalid_kind = r#gen::RuntimeArtifact {
            name: "files/value.txt".to_string(),
            kind: r#gen::RuntimeArtifactKind::Unspecified.into(),
            archive_name: None,
            digest: vec![0; 32],
            data: b"value".to_vec(),
        };
        assert!(matches!(
            RuntimeArtifact::try_from(invalid_kind),
            Err(ExecutionError::InvalidArgument(_))
        ));
    }
}
