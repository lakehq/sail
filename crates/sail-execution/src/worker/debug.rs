// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Formatter};

use sail_common::utils::debug::DebugBinary;

use crate::worker::r#gen::{JobSession, RunTaskBatchRequest};

impl Debug for RunTaskBatchRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RunTaskBatchRequest {
            job_id,
            stage,
            tasks,
            definition,
            peers,
            session,
        } = self;
        f.debug_struct("RunTaskBatchRequest")
            .field("job_id", job_id)
            .field("stage", stage)
            .field("tasks", tasks)
            .field("definition", &DebugBinary::from(definition))
            .field("peers", peers)
            .field("session", session)
            .finish()
    }
}

impl Debug for JobSession {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let JobSession {
            options,
            parquet,
            spark_time_zone,
        } = self;
        f.debug_struct("JobSession")
            .field("options", options)
            .field("parquet", &DebugBinary::from(parquet))
            .field("spark_time_zone", spark_time_zone)
            .finish()
    }
}
