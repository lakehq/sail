use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskReadLocation;
use crate::stream::writer::{LocalStreamStorage, TaskWriteLocation};
use crate::task::gen;

#[derive(Debug, Clone)]
pub struct TaskDefinition {
    pub plan: Arc<[u8]>,
    pub inputs: Vec<TaskInput>,
    pub output: TaskOutput,
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
}

#[derive(Debug, Clone)]
pub enum TaskOutputLocator {
    Local { replicas: usize },
    Remote { uri: String },
}

impl From<TaskDefinition> for gen::TaskDefinition {
    fn from(value: TaskDefinition) -> Self {
        let TaskDefinition {
            plan,
            inputs,
            output,
        } = value;
        gen::TaskDefinition {
            plan: plan.to_vec(),
            inputs: inputs.into_iter().map(|x| x.into()).collect(),
            output: Some(output.into()),
        }
    }
}

impl TryFrom<gen::TaskDefinition> for TaskDefinition {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskDefinition) -> Result<Self, Self::Error> {
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
                ))
            }
        };
        Ok(TaskDefinition {
            plan: Arc::from(value.plan),
            inputs,
            output,
        })
    }
}

impl From<TaskInput> for gen::TaskInput {
    fn from(value: TaskInput) -> Self {
        let TaskInput { locator } = value;
        gen::TaskInput {
            locator: Some(locator.into()),
        }
    }
}

impl TryFrom<gen::TaskInput> for TaskInput {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInput) -> Result<Self, Self::Error> {
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task input locator".to_string(),
                ))
            }
        };
        Ok(TaskInput { locator })
    }
}

impl From<TaskInputLocator> for gen::TaskInputLocator {
    fn from(value: TaskInputLocator) -> Self {
        let kind = match value {
            TaskInputLocator::Driver { stage, keys } => {
                gen::task_input_locator::Kind::Driver(gen::TaskInputDriverLocator {
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Worker { stage, keys } => {
                gen::task_input_locator::Kind::Worker(gen::TaskInputWorkerLocator {
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
            TaskInputLocator::Remote { uri, stage, keys } => {
                gen::task_input_locator::Kind::Remote(gen::TaskInputRemoteLocator {
                    uri,
                    stage: stage as u64,
                    keys: keys.into_iter().map(|x| x.into()).collect(),
                })
            }
        };
        gen::TaskInputLocator { kind: Some(kind) }
    }
}

impl TryFrom<gen::TaskInputLocator> for TaskInputLocator {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(gen::task_input_locator::Kind::Driver(gen::TaskInputDriverLocator {
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
            Some(gen::task_input_locator::Kind::Worker(gen::TaskInputWorkerLocator {
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
            Some(gen::task_input_locator::Kind::Remote(gen::TaskInputRemoteLocator {
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

impl From<TaskInputKey> for gen::TaskInputDriverKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        gen::TaskInputDriverKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<gen::TaskInputDriverKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInputDriverKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for gen::TaskInputDriverKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        gen::TaskInputDriverKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<gen::TaskInputDriverKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInputDriverKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<(WorkerId, TaskInputKey)> for gen::TaskInputWorkerKey {
    fn from(value: (WorkerId, TaskInputKey)) -> Self {
        let (
            worker_id,
            TaskInputKey {
                partition,
                attempt,
                channel,
            },
        ) = value;
        gen::TaskInputWorkerKey {
            worker_id: worker_id.into(),
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<gen::TaskInputWorkerKey> for (WorkerId, TaskInputKey) {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInputWorkerKey) -> Result<Self, Self::Error> {
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

impl From<Vec<(WorkerId, TaskInputKey)>> for gen::TaskInputWorkerKeyList {
    fn from(value: Vec<(WorkerId, TaskInputKey)>) -> Self {
        gen::TaskInputWorkerKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<gen::TaskInputWorkerKeyList> for Vec<(WorkerId, TaskInputKey)> {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInputWorkerKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskInputKey> for gen::TaskInputRemoteKey {
    fn from(value: TaskInputKey) -> Self {
        let TaskInputKey {
            partition,
            attempt,
            channel,
        } = value;
        gen::TaskInputRemoteKey {
            partition: partition as u64,
            attempt: attempt as u64,
            channel: channel as u64,
        }
    }
}

impl TryFrom<gen::TaskInputRemoteKey> for TaskInputKey {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInputRemoteKey) -> Result<Self, Self::Error> {
        Ok(TaskInputKey {
            partition: value.partition as usize,
            attempt: value.attempt as usize,
            channel: value.channel as usize,
        })
    }
}

impl From<Vec<TaskInputKey>> for gen::TaskInputRemoteKeyList {
    fn from(value: Vec<TaskInputKey>) -> Self {
        gen::TaskInputRemoteKeyList {
            keys: value.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl TryFrom<gen::TaskInputRemoteKeyList> for Vec<TaskInputKey> {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskInputRemoteKeyList) -> Result<Self, Self::Error> {
        value
            .keys
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()
    }
}

impl From<TaskOutput> for gen::TaskOutput {
    fn from(value: TaskOutput) -> Self {
        let TaskOutput {
            distribution,
            locator,
        } = value;
        gen::TaskOutput {
            distribution: Some(distribution.into()),
            locator: Some(locator.into()),
        }
    }
}

impl TryFrom<gen::TaskOutput> for TaskOutput {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskOutput) -> Result<Self, Self::Error> {
        let distribution = match value.distribution {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output distribution".to_string(),
                ))
            }
        };
        let locator = match value.locator {
            Some(x) => x.try_into()?,
            None => {
                return Err(ExecutionError::InvalidArgument(
                    "cannot decode empty task output locator".to_string(),
                ))
            }
        };
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }
}

impl From<TaskOutputDistribution> for gen::TaskOutputDistribution {
    fn from(value: TaskOutputDistribution) -> Self {
        let kind = match value {
            TaskOutputDistribution::Hash { keys, channels } => {
                gen::task_output_distribution::Kind::Hash(gen::TaskOutputHashDistribution {
                    keys: keys.into_iter().map(|k| k.to_vec()).collect(),
                    channels: channels as u64,
                })
            }
            TaskOutputDistribution::RoundRobin { channels } => {
                gen::task_output_distribution::Kind::RoundRobin(
                    gen::TaskOutputRoundRobinDistribution {
                        channels: channels as u64,
                    },
                )
            }
        };
        gen::TaskOutputDistribution { kind: Some(kind) }
    }
}

impl TryFrom<gen::TaskOutputDistribution> for TaskOutputDistribution {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskOutputDistribution) -> Result<Self, Self::Error> {
        match value.kind {
            Some(gen::task_output_distribution::Kind::Hash(gen::TaskOutputHashDistribution {
                keys,
                channels,
            })) => Ok(TaskOutputDistribution::Hash {
                keys: keys.into_iter().map(Arc::from).collect(),
                channels: channels as usize,
            }),
            Some(gen::task_output_distribution::Kind::RoundRobin(
                gen::TaskOutputRoundRobinDistribution { channels },
            )) => Ok(TaskOutputDistribution::RoundRobin {
                channels: channels as usize,
            }),
            None => Err(ExecutionError::InvalidArgument(
                "cannot decode empty task output distribution".to_string(),
            )),
        }
    }
}

impl From<TaskOutputLocator> for gen::TaskOutputLocator {
    fn from(value: TaskOutputLocator) -> Self {
        let kind = match value {
            TaskOutputLocator::Local { replicas } => {
                gen::task_output_locator::Kind::Local(gen::TaskOutputLocalLocator {
                    replicas: replicas as u64,
                })
            }
            TaskOutputLocator::Remote { uri } => {
                gen::task_output_locator::Kind::Remote(gen::TaskOutputRemoteLocator { uri })
            }
        };
        gen::TaskOutputLocator { kind: Some(kind) }
    }
}

impl TryFrom<gen::TaskOutputLocator> for TaskOutputLocator {
    type Error = ExecutionError;

    fn try_from(value: gen::TaskOutputLocator) -> Result<Self, Self::Error> {
        match value.kind {
            Some(gen::task_output_locator::Kind::Local(gen::TaskOutputLocalLocator {
                replicas,
            })) => Ok(TaskOutputLocator::Local {
                replicas: replicas as usize,
            }),
            Some(gen::task_output_locator::Kind::Remote(gen::TaskOutputRemoteLocator { uri })) => {
                Ok(TaskOutputLocator::Remote { uri })
            }
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
                        parse_physical_expr(
                            &PhysicalExprNode::decode(k.as_ref())?,
                            ctx,
                            schema,
                            codec,
                        )
                        .map_err(|e| e.into())
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?;
                Ok(Partitioning::Hash(keys, *channels))
            }
            TaskOutputDistribution::RoundRobin { channels } => {
                Ok(Partitioning::RoundRobinBatch(*channels))
            }
        }
    }
}
