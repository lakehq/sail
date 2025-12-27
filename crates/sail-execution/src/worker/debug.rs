use std::fmt::{Debug, Formatter};

use sail_common::debug::DebugBinary;

use crate::worker::gen::RunTaskRequest;

impl Debug for RunTaskRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RunTaskRequest {
            job_id,
            task_id,
            attempt,
            plan,
            partition,
            channel,
            peers,
        } = self;
        f.debug_struct("RunTaskRequest")
            .field("job_id", job_id)
            .field("task_id", task_id)
            .field("attempt", attempt)
            .field("plan", &DebugBinary::from(plan))
            .field("partition", partition)
            .field("channel", channel)
            .field("peers", peers)
            .finish()
    }
}
