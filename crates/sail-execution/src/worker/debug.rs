use std::fmt::{Debug, Formatter};

use crate::worker::r#gen::RunTaskRequest;

impl Debug for RunTaskRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let resources = self
            .launch_context
            .as_ref()
            .and_then(|context| context.resources.as_ref());
        let python_artifacts = resources
            .map(|resources| resources.python_artifacts.as_slice())
            .unwrap_or_default();
        let local_relations = resources
            .map(|resources| resources.local_relation_resources.as_slice())
            .unwrap_or_default();
        let inline_resource_bytes = python_artifacts
            .iter()
            .filter_map(|artifact| artifact.data.as_ref())
            .map(Vec::len)
            .chain(
                local_relations
                    .iter()
                    .filter_map(|resource| resource.data.as_ref())
                    .map(Vec::len),
            )
            .fold(0_usize, usize::saturating_add);
        let declared_resource_bytes = python_artifacts
            .iter()
            .map(|artifact| artifact.size)
            .chain(local_relations.iter().map(|resource| resource.size))
            .fold(0_u64, u64::saturating_add);
        let resource_uris = python_artifacts
            .iter()
            .filter(|artifact| artifact.uri.is_some())
            .count()
            .saturating_add(
                local_relations
                    .iter()
                    .filter(|resource| resource.uri.is_some())
                    .count(),
            );

        f.debug_struct("RunTaskRequest")
            .field("job_id", &self.job_id)
            .field("stage", &self.stage)
            .field("partition", &self.partition)
            .field("attempt", &self.attempt)
            .field("definition_bytes", &self.definition.len())
            .field("peer_count", &self.peers.len())
            .field("python_artifact_count", &python_artifacts.len())
            .field("local_relation_resource_count", &local_relations.len())
            .field("inline_resource_bytes", &inline_resource_bytes)
            .field("declared_resource_bytes", &declared_resource_bytes)
            .field("resource_uri_count", &resource_uris)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_task_debug_excludes_payload_content() {
        let request = RunTaskRequest {
            job_id: 1,
            stage: 2,
            partition: 3,
            attempt: 4,
            definition: b"secret-definition".to_vec(),
            peers: vec![crate::worker::r#gen::WorkerLocation {
                worker_id: 5,
                host: "secret-peer".to_string(),
                port: 6,
            }],
            launch_context: Some(crate::worker::r#gen::TaskLaunchContext {
                resources: Some(crate::worker::r#gen::TaskResources {
                    python_artifacts: vec![crate::worker::r#gen::PySparkPythonArtifact {
                        name: "secret-artifact-name".to_string(),
                        data: Some(b"secret-artifact".to_vec()),
                        uri: Some("s3://secret-bucket/secret-key".to_string()),
                        size: 15,
                        ..Default::default()
                    }],
                    local_relation_resources: vec![crate::worker::r#gen::LocalRelationResource {
                        key: "secret-relation-key".to_string(),
                        data: Some(b"secret-relation".to_vec()),
                        size: 15,
                        ..Default::default()
                    }],
                }),
            }),
        };

        let rendered = format!("{request:?}");
        assert!(rendered.contains("definition_bytes: 17"));
        assert!(rendered.contains("peer_count: 1"));
        assert!(rendered.contains("python_artifact_count: 1"));
        assert!(rendered.contains("local_relation_resource_count: 1"));
        assert!(rendered.contains("inline_resource_bytes: 30"));
        assert!(rendered.contains("declared_resource_bytes: 30"));
        assert!(rendered.contains("resource_uri_count: 1"));
        assert!(!rendered.contains("secret"));
        assert!(rendered.len() < 512);
    }
}
