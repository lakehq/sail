use std::fmt::{Debug, Formatter};

use sail_common::utils::debug::DebugBinary;

use crate::worker::r#gen::RunTaskBatchRequest;

impl Debug for RunTaskBatchRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RunTaskBatchRequest {
            job_id,
            stage,
            tasks,
            definition,
            peers,
        } = self;
        f.debug_struct("RunTaskBatchRequest")
            .field("job_id", job_id)
            .field("stage", stage)
            .field("tasks", tasks)
            .field("definition", &DebugBinary::from(definition))
            .field("peers", peers)
            .finish()
    }
}
