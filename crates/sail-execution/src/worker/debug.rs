use std::fmt::{Debug, Formatter};

use sail_common::debug::DebugBinary;

use crate::worker::gen::{TaskDefinition, TaskOutputHashDistribution};

impl Debug for TaskDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let TaskDefinition {
            plan,
            inputs,
            output,
        } = self;
        f.debug_struct("TaskDefinition")
            .field("plan", &DebugBinary::from(plan))
            .field("inputs", inputs)
            .field("output", output)
            .finish()
    }
}

impl Debug for TaskOutputHashDistribution {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let TaskOutputHashDistribution {
            keys,
            channels,
            replicas,
        } = self;
        f.debug_struct("TaskOutputHashDistribution")
            .field(
                "keys",
                &keys
                    .iter()
                    .map(|k| DebugBinary::from(k))
                    .collect::<Vec<_>>(),
            )
            .field("channels", channels)
            .field("replicas", replicas)
            .finish()
    }
}
