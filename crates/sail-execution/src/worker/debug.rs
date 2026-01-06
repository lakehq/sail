use std::fmt::{Debug, Formatter};

use sail_common::debug::DebugBinary;

use crate::worker::gen::RunTaskRequest;

impl Debug for RunTaskRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RunTaskRequest {
            job_id,
            stage,
            partition,
            attempt,
            definition,
            peers,
        } = self;
        f.debug_struct("RunTaskRequest")
            .field("job_id", job_id)
            .field("stage", stage)
            .field("partition", partition)
            .field("attempt", attempt)
            .field("definition", &DebugBinary::from(definition))
            .field("peers", peers)
            .finish()
    }
}
