use prost::Message;

use super::*;

#[test]
fn replay_task_protocol_keeps_named_fixed_attempts() -> ExecutionResult<()> {
    let id = ReplayId {
        scope: Default::default(),
        ordinal: 19,
    };
    for storage in [false, true] {
        let key = TaskInputKey {
            partition: 3,
            attempt: 11,
            channel: 0,
        };
        let locator = if storage {
            TaskInputLocator::Storage {
                keys: vec![vec![key]],
            }
        } else {
            TaskInputLocator::Worker {
                keys: vec![vec![(WorkerId::from(8), key)]],
            }
        };
        let definition = TaskDefinition {
            plan: Arc::from([1, 2, 3]),
            inputs: vec![TaskInput {
                replay: Some(id),
                stage: 7,
                locator: Arc::new(locator),
            }],
            output: TaskOutput {
                distribution: TaskOutputDistribution::RoundRobinBatch { channels: 1 },
                locator: if storage {
                    TaskOutputLocator::ReplayStorage { id }
                } else {
                    TaskOutputLocator::ReplayLocal { id }
                },
            },
        };
        let bytes = r#gen::TaskDefinition::from(definition).encode_to_vec();
        let decoded = TaskDefinition::try_from(r#gen::TaskDefinition::decode(bytes.as_slice())?)?;
        assert_eq!(decoded.inputs[0].replay, Some(id));
        assert_eq!(decoded.inputs[0].stage, 7);
        let keys = match decoded.inputs[0].locator.as_ref() {
            TaskInputLocator::Storage { keys } => keys.clone(),
            TaskInputLocator::Worker { keys } => {
                assert_eq!(keys[0][0].0, WorkerId::from(8));
                keys.iter()
                    .map(|group| group.iter().map(|(_, key)| key.clone()).collect())
                    .collect()
            }
            _ => return Err(ExecutionError::InternalError("unexpected locator".into())),
        };
        assert_eq!(
            (keys[0][0].partition, keys[0][0].attempt, keys[0][0].channel),
            (3, 11, 0)
        );
        match decoded.output.locator {
            TaskOutputLocator::ReplayStorage { id: actual } if storage => assert_eq!(actual, id),
            TaskOutputLocator::ReplayLocal { id: actual } if !storage => assert_eq!(actual, id),
            _ => {
                return Err(ExecutionError::InternalError(
                    "unexpected output locator".into(),
                ));
            }
        }
    }
    Ok(())
}

#[test]
fn replay_task_protocol_rejects_invalid_scope() {
    let input = r#gen::TaskInput {
        stage: 0,
        replay: Some(r#gen::ReplayId {
            scope: "not-a-uuid".into(),
            ordinal: 0,
        }),
        locator: Some(TaskInputLocator::Storage { keys: vec![] }.into()),
    };
    assert!(TaskInput::try_from(input).is_err());
}
