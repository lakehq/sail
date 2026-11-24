// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Formatter};

use sail_common::utils::debug::DebugBinary;

use crate::worker::r#gen::{RunTaskBatchRequest, StorageRoutes};

impl Debug for StorageRoutes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let StorageRoutes {
            version,
            token: _,
            routes,
        } = self;
        f.debug_struct("StorageRoutes")
            .field("version", version)
            .field("token", &"<redacted>")
            .field("routes", routes)
            .finish()
    }
}

impl Debug for RunTaskBatchRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RunTaskBatchRequest {
            job_id,
            stage,
            tasks,
            definition,
            peers,
            storage_routes,
        } = self;
        f.debug_struct("RunTaskBatchRequest")
            .field("job_id", job_id)
            .field("stage", stage)
            .field("tasks", tasks)
            .field("definition", &DebugBinary::from(definition))
            .field("peers", peers)
            .field("storage_routes", storage_routes)
            .finish()
    }
}
