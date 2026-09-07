use std::fmt::{Debug, Formatter};

use crate::worker::r#gen::RunTaskRequest;

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
            .field("definition_bytes", &definition.len())
            .field("peers", peers)
            .finish()
    }
}
