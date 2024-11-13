use std::fmt::{Debug, Formatter};

use sail_common::debug::DebugBinary;

use crate::worker::gen::RunTaskRequest;

impl Debug for RunTaskRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RunTaskRequest {
            task_id,
            attempt,
            plan,
            partition,
            channel,
        } = self;
        f.debug_struct("RunTaskRequest")
            .field("task_id", task_id)
            .field("attempt", attempt)
            .field("plan", &DebugBinary::from(plan))
            .field("partition", partition)
            .field("channel", channel)
            .finish()
    }
}
