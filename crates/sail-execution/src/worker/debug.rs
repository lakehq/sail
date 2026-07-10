use std::fmt::{Debug, Formatter};

use sail_common::debug::DebugBinary;

use crate::worker::r#gen::{RunTaskRequest, TaskLaunchContext};

struct DebugTaskLaunchContext<'a>(&'a Option<TaskLaunchContext>);

impl Debug for DebugTaskLaunchContext<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Some(context) = self.0.as_ref() else {
            return f.write_str("None");
        };
        let artifacts = context
            .resources
            .as_ref()
            .map(|resources| resources.python_artifacts.as_slice())
            .unwrap_or_default();
        let local_relations = context
            .resources
            .as_ref()
            .map(|resources| resources.local_relation_resources.as_slice())
            .unwrap_or_default();
        let artifact_inline_bytes: usize = artifacts
            .iter()
            .map(|artifact| artifact.data.as_ref().map(|x| x.len()).unwrap_or_default())
            .sum();
        let local_relation_inline_bytes: usize = local_relations
            .iter()
            .map(|resource| resource.data.as_ref().map(|x| x.len()).unwrap_or_default())
            .sum();
        f.debug_struct("TaskLaunchContext")
            .field("python_artifact_count", &artifacts.len())
            .field("python_artifact_inline_bytes", &artifact_inline_bytes)
            .field("local_relation_resource_count", &local_relations.len())
            .field(
                "local_relation_resource_inline_bytes",
                &local_relation_inline_bytes,
            )
            .finish()
    }
}

impl Debug for RunTaskRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let RunTaskRequest {
            job_id,
            stage,
            partition,
            attempt,
            definition,
            peers,
            launch_context,
        } = self;
        f.debug_struct("RunTaskRequest")
            .field("job_id", job_id)
            .field("stage", stage)
            .field("partition", partition)
            .field("attempt", attempt)
            .field("definition", &DebugBinary::from(definition))
            .field("peers", peers)
            .field("launch_context", &DebugTaskLaunchContext(launch_context))
            .finish()
    }
}
